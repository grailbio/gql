package gql

//go:generate ../../../../github.com/grailbio/base/gtl/generate.py --PREFIX=btsvTmp --package=gql --output=btsv_tmp_pool.go -DELEM=btsvStructTmp -DMAXSIZE=128 ../../../../github.com/grailbio/base/gtl/freepool.go.tpl

// This file implements functions for reading and writing *.btsv files.
//
// BTSV is similar to TSV in spirit. Differences are the following:
//
// - Files can be row-range-sharded, so writes are a lot faster.
//   Each row is still packed in one unit (i.e., no column sharding yet).
//
// - The file size is 1/3 to 1/4 of TSV.
//
// - It can represent all the gql-internal data types, including nested tables
//   and columns.
//
// - It can be written in a single pass, even when not all the rows are of the
//   same type. In contrast, TSV needs to read the input, first to collect the
//   columns that appear in the rows, second to output the rows.
//
// Data layout
//
// - One BTSV table is stored in a directory with multiple files. The below
//   example shows the layout of a 3-way range-sharded btsv table, foo/bar.btsv.
//
//   foo/bar.btsv/     # the directory is always named *.btsv.
//                000000-000003.grail-rio
//                000001-000003.grail-rio
//                000002-000003.grail-rio
//
//  - Each shard file is a compressed recordio file. A row in the table becomes
//    a recordio element. The index is stored in the trailer as
//    biopb.BinaryTSVIndex.
//
//  - Each Value is stored in a way similar to its GOB encoding. The difference
//    is that column names are translated in a dense integer space, and the int
//    -> column-name mappings are stored in BinaryTSVIndex. This allows for more
//    compact encoding of column values.

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/recordio"
	"github.com/grailbio/base/recordio/recordiozstd"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/gql/columnsorter"
	"github.com/grailbio/gql/gqlpb"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

const btsvShardFileExt = ".grail-rio"

// For small btsv recordio files, the scanner tries to shard the file accurately
// by sequentially scanning individual records, as opposed to seeking directly
// to nearest block boundaries. This constant defines the max size recordio file
// size for which the former policy is used.
const maxBTSVAccurateShardingSize = 16 << 20

// btsvRecordioRangeScanner is like a recordio ShardedScanner, but it seeks
// individual records for accurate sharding. It is very slow for it shall be
// used only for files whose size <= maxBTSVAccurateShardingSize.
type btsvRecordioRangeScanner struct {
	rio recordio.Scanner
	// The scanner reads the records in range [cur,limit).
	//
	// INVARIANT: cur <= limit, and limit <= total # of records in file.
	cur, limit int
}

func newBTSVRecordioRangeScanner(in io.ReadSeeker, start, limit int) *btsvRecordioRangeScanner {
	r := &btsvRecordioRangeScanner{rio: recordio.NewScanner(in, recordio.ScannerOpts{}), cur: start, limit: limit}
	for i := 0; i < start; i++ {
		if !r.rio.Scan() {
			panic(r)
		}
	}
	return r
}

func (r *btsvRecordioRangeScanner) Scan() bool {
	if r.cur >= r.limit {
		return false
	}
	if !r.rio.Scan() {
		panic(r)
	}
	r.cur++
	return true
}

func (r *btsvRecordioRangeScanner) Get() interface{} {
	return r.rio.Get()
}

func (r *btsvRecordioRangeScanner) Err() error {
	return r.rio.Err()
}

func (r *btsvRecordioRangeScanner) Header() recordio.ParsedHeader   { panic("Header not implemented") }
func (r *btsvRecordioRangeScanner) Trailer() []byte                 { panic("Trailer not implemented") }
func (r *btsvRecordioRangeScanner) Version() recordio.FormatVersion { panic("Version not implemented") }

func (r *btsvRecordioRangeScanner) Seek(loc recordio.ItemLocation) { panic("Seek not implemented") }

func (r *btsvRecordioRangeScanner) Finish() error {
	return r.rio.Finish()
}

// BTSVShardPath computes the path of a btsv shard file.
//
// REQUIRES: dir must end with ".btsv". 0 <= shard < nshards.
func BTSVShardPath(dir string, shard, nshards int) string {
	return file.Join(dir, fmt.Sprintf("%06d-%06d%s", shard, nshards, btsvShardFileExt))
}

// btsvStructTmp is a temp buffer used during table reading and writing. Stored
// in a freepool for performance.
type btsvStructTmp struct {
	colNames []symbol.ID
	cols     []StructField
}

type btsvTableColumnSpec struct {
	id          symbol.ID
	typ         ValueType
	description string
}

// btsvTable is a Table implementation for *.btsv files.
type btsvTable struct {
	ast ASTNode // source-code location
	dir string  // btsv dir name.

	once        sync.Once
	hash        hash.Hash // table hash computed from the path & file attributes.
	shards      []btsvTableShard
	cumShardLen []int // Cumulative #rows in the shards. Exact.
}

// btsvTableShard stores state for a btsv shard file.
type btsvTableShard struct {
	path  string
	index gqlpb.BinaryTSVIndex // read from the recordio trailer.
	locs  []*time.Location     // derived from the index

	// cols is the list of all columns that apperar in the table.  They are listed
	// in BinaryTSVIndex.Column.col order.
	cols []btsvTableColumnSpec

	// colIndexMap maps a column name to a cols[] index.
	colIndexMap map[symbol.ID]int
	attrs       TableAttrs // derived from the index

	// serializedSize stores the on-disk size of the shard.
	serializedSize int64
	// modTime is the last-modification time of the file.
	modTime time.Time
}

func (t *btsvTable) Attrs(ctx context.Context) TableAttrs {
	t.init(ctx)
	// All the shards have the same attrs, so pick any.
	return t.shards[0].attrs
}

func (t *btsvTable) Prefetch(ctx context.Context) { go Recover(func() { t.init(ctx) }) }

func (t *btsvTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	t.init(ctx)
	Debugf(t.ast, "btsv %s: start scan [%d,%d)/%d", t.dir, start, limit, total)
	scanStart, scanLimit := ScaleShardRange(start, limit, total, t.Len(ctx, Approx))
	return newBTSVTableScanner(ctx, t, scanStart, scanLimit)
}

func newBTSVTableScanner(ctx context.Context, t *btsvTable, scanStart, scanLimit int) *btsvTableScanner {
	sc := &btsvTableScanner{
		ctx:        ctx,
		parent:     t,
		start:      scanStart,
		limit:      scanLimit,
		curLimit:   scanStart,
		tmpDecoder: marshal.NewDecoder(nil),
		tmpPool:    btsvTmpPool{New: func() btsvStructTmp { return btsvStructTmp{} }},
	}
	runtime.SetFinalizer(sc, func(sc *btsvTableScanner) {
		if sc.rio != nil {
			if err := sc.rio.Finish(); err != nil {
				log.Panic(err)
			}
		}
		if sc.in != nil {
			if err := sc.in.Close(ctx); err != nil {
				log.Panic(err)
			}
		}
	})
	return sc
}

func (t *btsvTable) Hash() hash.Hash {
	t.init(BackgroundContext)
	return t.hash
}

func (t *btsvTable) Len(ctx context.Context, _ CountMode) int {
	t.init(ctx)
	return t.cumShardLen[len(t.shards)-1]
}

func (t *btsvTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	MarshalTablePath(enc, t.dir, singletonBTSVFileHandler, t.Hash())
}

func (t *btsvTableShard) LocationFromID(id int32) *time.Location {
	return t.locs[id]
}

func (t *btsvTable) init(ctx context.Context) {
	t.once.Do(func() {
		shardPaths := listBTSVShardPaths(ctx, t.dir, t.ast)
		if len(shardPaths) == 0 {
			Panicf(t.ast, "btsv %s: no file found", t.dir)
		}
		t.shards = make([]btsvTableShard, len(shardPaths))
		traverse.Parallel.Each(len(t.shards), func(i int) error { // nolint: errcheck
			t.shards[i] = t.initShard(ctx, shardPaths[i])
			return nil
		})
		var cum = 0
		t.cumShardLen = make([]int, len(shardPaths))
		h := hash.String(t.dir)
		for si := range shardPaths {
			cum += int(t.shards[si].index.Rows)
			t.cumShardLen[si] = cum
			h = h.Merge(hash.Time(t.shards[si].modTime))
		}
		if t.hash == hash.Zero {
			// Note: for other table types (.tsv, .bam, ..), we enforce that
			// (t.hash==hash.Zero || t.hash==h), but this invariant doesn't hold for
			// btsv. A btsv file can be used as a persistent cache of an arbitrary
			// table, in which case the hash of the btsv is the hash of the original
			// table.
			t.hash = h
		}
	})
}

// ListBTSVShardPaths returns the pathnames of recordio files found in dir.  The
// pathnames will be sorted lexicographically.
func listBTSVShardPaths(ctx context.Context, dir string, ast ASTNode) []string {
	l := file.List(ctx, dir, true)
	paths := []string{}
	for l.Scan() {
		if filepath.Ext(l.Path()) != btsvShardFileExt {
			Errorf(ast, "btsv %s: ignoring file %s", dir, l.Path())
			continue
		}
		paths = append(paths, l.Path())
	}
	if len(paths) == 0 {
		return nil
	}
	sort.Strings(paths)
	Debugf(ast, "btsv list %s: found %v", dir, paths)
	return paths
}

func (t *btsvTable) initShard(ctx context.Context, path string) (ts btsvTableShard) {
	recordiozstd.Init()
	ts.path = path
	in, err := file.Open(ctx, path)
	if err != nil {
		Panicf(t.ast, "btsv %v: open: %v", path, err)
	}
	defer in.Close(ctx) // nolint: errcheck
	info, err := in.Stat(ctx)
	if err != nil {
		Panicf(t.ast, "btsv %v: stat: %v", path, err)
	}
	ts.modTime = info.ModTime()
	ts.serializedSize = info.Size()
	rio := recordio.NewShardScanner(in.Reader(ctx), recordio.ScannerOpts{}, 0, 1, 1)
	defer func() {
		if err := rio.Finish(); err != nil {
			log.Panic(err)
		}
	}()
	trailer := rio.Trailer()
	if err := ts.index.Unmarshal(trailer); err != nil {
		Panicf(t.ast, "btsv %v: corrupt trailer: %v", path, err)
	}
	ts.locs = make([]*time.Location, len(ts.index.TimeLocation))
	for i, l := range ts.index.TimeLocation {
		var err error
		ts.locs[i], err = time.LoadLocation(l.Str)
		if err != nil {
			ts.locs[i] = time.FixedZone(l.Name, int(l.OffsetS))
		}
	}
	ts.cols = make([]btsvTableColumnSpec, len(ts.index.Column))
	ts.colIndexMap = map[symbol.ID]int{}
	for _, x := range ts.index.Column {
		colID := symbol.Intern(x.Name)
		ts.cols[x.Col].id = colID
		ts.cols[x.Col].typ = ValueType(x.Typ)
		ts.cols[x.Col].description = x.Description
		ts.colIndexMap[colID] = int(x.Col)
	}
	ts.attrs = TableAttrs{
		Name:        "btsv",
		Path:        path,
		Description: strings.Join(ts.index.Description, "\n"),
	}
	if ts.index.Name != "" {
		ts.attrs.Name = ts.index.Name
	}
	if ts.index.Path != "" {
		ts.attrs.Path = ts.index.Path
	}
	for _, col := range ts.index.Column {
		ts.attrs.Columns = append(ts.attrs.Columns,
			TSVColumn{
				Name:        col.Name,
				Type:        ValueType(col.Typ),
				Description: col.Description})
	}
	return
}

type btsvTableScanner struct {
	ctx          context.Context
	parent       *btsvTable
	start, limit int
	curLimit     int

	shard *btsvTableShard
	in    file.File
	rio   recordio.Scanner
	val   Value

	tmpDecoder   *marshal.Decoder
	tmpPool      btsvTmpPool
	unmarshalCtx UnmarshalContext
}

func (sc *btsvTableScanner) unmarshalValue(ctx UnmarshalContext, dec *marshal.Decoder) Value {
	v := Value{typ: ValueType(dec.Byte())}
	switch v.typ {
	case NullType:
		switch dec.Byte() {
		case 1:
			v.v = uint64(PosNull)
		case byte(0xff):
			tmp := NegNull
			v.v = uint64(tmp)
		default:
			log.Panicf("NA: %v", dec)
		}
	case BoolType:
		switch dec.Byte() {
		case 1:
			v.v = 1
		case 0:
			v.v = 0
		default:
			log.Panicf("Bool: %v", dec)
		}
	case IntType, CharType:
		v.v = uint64(dec.Varint())
	case FloatType:
		v.v = dec.Uint64()
	case StringType, FileNameType, EnumType:
		s := dec.String()
		sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
		v.p = unsafe.Pointer(sh.Data)
		v.v = uint64(sh.Len)
	case DateType, DateTimeType:
		v.v = uint64(dec.Varint())
		loc := sc.shard.LocationFromID(int32(dec.Varint()))
		v.p = unsafe.Pointer(loc)
	case TableType:
		v = NewTable(unmarshalTable(ctx, dec))
	case StructType:
		nFields := int(dec.Varint())
		tmp := sc.tmpPool.Get()
		for i := 0; i < nFields; i++ {
			colID := int(dec.Varint())
			col := &sc.shard.cols[colID]
			val := sc.unmarshalValue(ctx, dec)
			tmp.cols = append(tmp.cols, StructField{Name: col.id, Value: val})
		}
		v = NewStruct(NewSimpleStruct(tmp.cols...))
		tmp.cols = tmp.cols[:0]
		sc.tmpPool.Put(tmp)
	default:
		Panicf(sc.parent.ast, "invalid value %+v", v.typ)
		return Value{}
	}
	return v
}

func (sc *btsvTableScanner) Scan() bool {
	CheckCancellation(sc.ctx)
	for {
		if sc.rio == nil {
			nextOff := sc.curLimit
			subTableIndex, subTableStart, subTableLimit, scanLimit := nextSubTable(sc.start, sc.limit, nextOff, sc.parent.cumShardLen)
			if subTableIndex < 0 {
				return false
			}
			log.Debug.Printf("btsv scanner %s[%d,%d): start [%d,%d) of table %d[%d,%d) (%d tables, %v)",
				sc.parent.dir, sc.start, sc.limit, nextOff, scanLimit, subTableIndex, subTableStart, subTableLimit, len(sc.parent.shards), sc.parent.cumShardLen)
			sc.shard = &sc.parent.shards[subTableIndex]
			var err error
			if sc.in, err = file.Open(sc.ctx, sc.shard.path); err != nil {
				Panicf(sc.parent.ast, "btsv %v: open failed: %v", sc.shard.path, err)
			}
			Debugf(sc.parent.ast, "btsv %s: open shard [%d,%d)/%d", sc.shard.path,
				nextOff-subTableStart, scanLimit-subTableStart, subTableLimit-subTableStart)

			shardStart := nextOff - subTableStart
			shardLimit := scanLimit - subTableStart
			shardSize := subTableLimit - subTableStart
			if shardSize <= maxBTSVAccurateShardingSize {
				sc.rio = newBTSVRecordioRangeScanner(sc.in.Reader(sc.ctx), shardStart, shardLimit)
			} else {
				sc.rio = recordio.NewShardScanner(sc.in.Reader(sc.ctx), recordio.ScannerOpts{}, shardStart, shardLimit, shardSize)
			}
			sc.curLimit = scanLimit
			if len(sc.shard.index.MarshaledContext) > 0 {
				sc.unmarshalCtx = newUnmarshalContext(sc.shard.index.MarshaledContext)
			} else {
				// Old btsv format. They shouldn't use unmarshalctx at all, so leave it as nil.
			}
		}
		if !sc.rio.Scan() {
			if err := sc.rio.Err(); err != nil {
				log.Panic(err)
			}
			if err := sc.rio.Finish(); err != nil {
				log.Panic(err)
			}
			if err := sc.in.Close(sc.ctx); err != nil {
				log.Panic(err)
			}
			sc.rio = nil
			sc.in = nil
			continue
		}
		bytes := sc.rio.Get().([]byte)
		sc.tmpDecoder.Reset(bytes)
		sc.val = sc.unmarshalValue(sc.unmarshalCtx, sc.tmpDecoder)
		if sc.tmpDecoder.Len() > 0 {
			Panicf(sc.parent.ast, "btsv.Scan: %dB garbage at the end, value: %v", sc.tmpDecoder.Len(), sc.val)
		}
		return true
	}
}

func (sc *btsvTableScanner) Value() Value { return sc.val }

// NewBTSVTable creates a Table implementation for a btsv table stored in
// directory "path".  path must end with *.btsv. hash is an optional hash of the
// inputs that derives the btsv table.
func NewBTSVTable(path string, ast ASTNode, hash hash.Hash) *btsvTable {
	t := &btsvTable{
		ast:  ast,
		hash: hash,
		dir:  path,
	}
	return t
}

// BTSVShardWriter creates a BTSV shard. Use NewBTSVShardWriter to create this
// object..
type BTSVShardWriter struct {
	out   file.File
	rio   recordio.Writer
	attrs TableAttrs

	mu        sync.Mutex
	colSorter *columnsorter.T
	colIDMap  map[symbol.ID]int
	cols      []*gqlpb.BinaryTSVIndex_Column

	nrows      int
	locMap     map[*time.Location]int32
	locStrMap  map[string]int32
	locs       []gqlpb.BinaryTSVIndex_TimeLocation
	marshalCtx MarshalContext

	tmpPool    btsvTmpPool
	tmpEncoder *marshal.Encoder
}

// Close must be called exactly once at the end of writes.
// It finalizes the file contents.
func (b *BTSVShardWriter) Close(ctx context.Context) {
	b.rio.Flush()
	b.rio.Wait()
	b.colSorter.Sort()
	idx := gqlpb.BinaryTSVIndex{
		Description: []string{
			fmt.Sprintf("cmdline: %s", strings.Join(os.Args, "\t")),
		},
		Name:             b.attrs.Name,
		Path:             b.attrs.Path,
		MarshaledContext: b.marshalCtx.marshal(),
	}
	if b.attrs.Description != "" {
		idx.Description = append(idx.Description, b.attrs.Description)
	}
	for _, colName := range b.colSorter.Columns() {
		colID, ok := b.colIDMap[colName]
		if !ok {
			log.Panicf("col %v not registered", colName)
		}
		col := *b.cols[colID]

		// Copy the description from the attrs given by the caller.
		for _, c := range b.attrs.Columns {
			if c.Name == col.Name {
				col.Description = c.Description
				break
			}
		}
		idx.Column = append(idx.Column, col)
	}
	idx.Rows = int64(b.nrows)
	idx.TimeLocation = b.locs
	idxData, err := idx.Marshal()
	if err != nil {
		log.Panicf("btsv index marshal: %v", err)
	}
	b.rio.SetTrailer(idxData)
	if err := b.rio.Finish(); err != nil {
		log.Panicf("writebtsv %v: close: %v", b.out.Name(), err)
	}
	if err := b.out.Close(ctx); err != nil {
		log.Panicf("writebtsv %v: close: %v", b.out.Name(), err)
	}
	log.Debug.Printf("btsvwriter: close %s", b.out.Name())
}

func (b *BTSVShardWriter) internCol(name symbol.ID, typ ValueType) *gqlpb.BinaryTSVIndex_Column {
	colID, ok := b.colIDMap[name]
	if ok {
		return b.cols[colID]
	}
	colID = len(b.cols)
	b.colIDMap[name] = colID
	col := &gqlpb.BinaryTSVIndex_Column{Col: int32(colID), Typ: int32(typ), Name: name.Str()}
	b.cols = append(b.cols, col)
	return col
}

func (b *BTSVShardWriter) getLocationID(t time.Time) int32 {
	loc := t.Location()
	locID, ok := b.locMap[loc]
	if ok {
		return locID
	}
	locStr := loc.String()
	locID, ok = b.locStrMap[locStr]
	if ok {
		b.locMap[loc] = locID
		return locID
	}

	locID = int32(len(b.locs))
	name, offset := t.Zone()
	b.locs = append(b.locs, gqlpb.BinaryTSVIndex_TimeLocation{
		Str:     locStr,
		Name:    name,
		OffsetS: int32(offset),
	})
	b.locMap[loc] = locID
	b.locStrMap[locStr] = locID
	return locID
}

// MarshalValue encodes the value and appends to "enc".
func (b *BTSVShardWriter) marshalValue(ctx MarshalContext, enc *marshal.Encoder, v Value) {
	enc.PutByte(byte(v.typ))
	switch v.typ {
	case NullType:
		if v.Null() == PosNull {
			enc.PutByte(1)
		} else {
			enc.PutByte(byte(0xff))
		}
	case BoolType:
		if v.Bool(nil) {
			enc.PutByte(1)
		} else {
			enc.PutByte(0)
		}
	case IntType, CharType:
		enc.PutVarint(int64(v.v))
	case FloatType:
		enc.PutUint64(v.v) // v.v encodes the floating point in binary.
	case StringType, FileNameType, EnumType:
		s := v.Str(nil)
		enc.PutString(s)
	case DateType, DateTimeType:
		t := v.DateTime(nil)
		enc.PutVarint(t.UnixNano())
		enc.PutVarint(int64(b.getLocationID(t)))
	case TableType:
		v.Table(nil).Marshal(ctx, enc)
	case StructType:
		s := v.Struct(nil)
		nFields := s.Len()
		enc.PutVarint(int64(nFields))
		tmp := b.tmpPool.Get()
		tmp.colNames = tmp.colNames[:0]
		for i := 0; i < nFields; i++ {
			f := s.Field(i)
			tmp.colNames = append(tmp.colNames, f.Name)
			col := b.internCol(f.Name, f.Value.Type())
			enc.PutVarint(int64(col.Col))
			b.marshalValue(ctx, enc, f.Value)
		}
		b.colSorter.AddColumns(tmp.colNames)
		b.tmpPool.Put(tmp)
	default:
		log.Panicf("writebinarytsv: invalid value %v (%s)", v, DescribeValue(v))
	}
}

// Append adds a value to the btsv table.
func (b *BTSVShardWriter) Append(val Value) {
	b.nrows++
	b.rio.Append(val)
}

// NewBTSVShardWriter creates a BTSVShardWriter object.  The attrs is used only
// as table description in the index.
//
// REQUIRES: path ends with ".btsv"
//
// Example:
//  w := NewBTSVShardWriter("/tmp/foo.btsv", 0, 1, attrs)
//  t := some Table
//  s := t.Scanner(...)
//  for s.Scan() {
//    w.Append(s.Value())
//  }
//  w.Close()
func NewBTSVShardWriter(ctx context.Context, dir string, shard, nshards int, attrs TableAttrs) *BTSVShardWriter {
	path := BTSVShardPath(dir, shard, nshards)
	out, err := file.Create(ctx, path)
	if err != nil {
		log.Panicf("writebtsv %v: create: %v", path, err)
	}
	log.Debug.Printf("btsvwriter: create %s", path)
	recordiozstd.Init()
	w := &BTSVShardWriter{
		out:        out,
		attrs:      attrs,
		colSorter:  columnsorter.New(),
		colIDMap:   map[symbol.ID]int{},
		locMap:     map[*time.Location]int32{},
		locStrMap:  map[string]int32{},
		marshalCtx: newMarshalContext(ctx),
		tmpPool:    btsvTmpPool{New: func() btsvStructTmp { return btsvStructTmp{} }},
		tmpEncoder: marshal.NewEncoder(nil),
	}
	w.rio = recordio.NewWriter(out.Writer(ctx), recordio.WriterOpts{
		Transformers: []string{recordiozstd.Name},
		Marshal: func(buf []byte, v interface{}) ([]byte, error) {
			val := v.(Value)
			w.mu.Lock() // This function may be called concurrently.
			enc := w.tmpEncoder
			enc.Reset(buf[:0])
			w.marshalValue(w.marshalCtx, enc, val)
			data := enc.Bytes()
			w.mu.Unlock()
			return data, nil
		}})
	w.rio.AddHeader(recordio.KeyTrailer, true)
	return w
}

// BTSVFileHandler is a FileHandler implementation for btsv files.
type btsvFileHandler struct{}

var singletonBTSVFileHandler = &btsvFileHandler{}

// Name implements FileHandler.
func (*btsvFileHandler) Name() string { return "btsv" }

// Open implements FileHandler.
func (*btsvFileHandler) Open(ctx context.Context, path string, ast ASTNode, hash hash.Hash) Table {
	return NewBTSVTable(path, ast, hash)
}

// Write implements FileHandler.
func (*btsvFileHandler) Write(ctx context.Context, path string, ast ASTNode, table Table, nShard int, overwrite bool) {
	paths := listBTSVShardPaths(ctx, path, ast)
	if len(paths) > 0 {
		if !overwrite {
			log.Printf("write %v: file already exists and --overwrite-files=false.", path)
			return
		}
		err := traverse.Parallel.Each(len(paths), func(i int) error {
			return file.Remove(ctx, path)
		})
		if err != nil {
			Errorf(ast, "remove %s: %v", path, err)
		}
	}
	traverse.Parallel.Each(nShard, func(shard int) error { // nolint: errcheck
		w := NewBTSVShardWriter(ctx, path, shard, nShard, table.Attrs(ctx))
		sc := table.Scanner(ctx, shard, shard+1, nShard)
		for sc.Scan() {
			w.Append(sc.Value())
		}
		w.Close(ctx)
		return nil
	})
	return
}

func init() {
	RegisterFileHandler(singletonBTSVFileHandler, `\.btsv$`)
}

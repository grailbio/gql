package gql

import (
	"context"
	"io"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/gql/termutil"
)

// TableAttrs represents a metadata about a table. It is used only for debugging
// and display purposes, so all the fields are optional.
type TableAttrs struct {
	// Name is a short description of the the table, "tsv", "mapfilter", etc.
	Name string
	// Path is the file from which the contents were read. It is "" for a computed
	// table.
	Path string
	// List of columns in the table.
	Columns []TSVColumn
	// Description can be any string.
	Description string
}

// CountMode controls the behavior of Table.Len().
type CountMode int

const (
	// Exact causes Table.Len() implementations to return the exact count.
	Exact CountMode = iota
	// Approx causes Table.Len() implementations to quickly return an approximate
	// count.
	Approx
)

// Table is a collection of Structs. It can be stored in a Value. It is
// analogous to a table in a relational database. Thread safe.
type Table interface {
	// Attrs obtains the table metadata. Any of the attr fields may be empty.
	Attrs(ctx context.Context) TableAttrs
	// Scanner creates a table scanner. The parameters are used to scan the part
	// [start,limit) of the table, where range [0,total) represents the whole
	// table.
	//
	// For example, Scanner(0,2,3) reads the 2/3rds of the table, and
	// Scanner(2,3,3) reads the remaining 1/3rd of the table. Note that the
	// scanner implementation need not guarantee that the shard size is uniform
	// (but they strive to). Also, it need not always range-shard the table -- it
	// can hash-shard the table, it that's more convenient.
	//
	// REQUIRES: 0 <= start <= limit <= total.
	Scanner(ctx context.Context, start, limit, total int) TableScanner

	// Len returns the number of rows in the table. When mode==Approx, this method
	// should return the approx number of rows quickly. The Approx mode is used to
	// guide sharding and parallelizing policies. When mode==Exact, this method
	// must compute the exact # of rows in the table.
	Len(ctx context.Context, mode CountMode) int

	// Hash computes a hash value for this table.
	Hash() hash.Hash
	// Marshal produces a string that can be fed to parser to reconstruct this table.
	Marshal(ctx MarshalContext, enc *marshal.Encoder)
	// Prefetch is called to fetch the contents in the background, if possible.
	// This method shall never block.
	Prefetch(ctx context.Context)
}

// TableScanner is an interface for reading a table. Thread compatible.
type TableScanner interface {
	// Scan fetches the next value. It returns false on EOF.
	Scan() bool
	// Value returns the current value.
	//
	// REQUIRES: the last Scan() call returned true.
	Value() Value
}

// nullTable implements the Table interfaces for NA.
var nullTableMarshalMagic = UnmarshalMagic{0xad, 0x55}

// nullTable implements an empty Table.
type nullTable struct{}

func (n nullTable) Prefetch(ctx context.Context)                {}
func (n nullTable) Len(ctx context.Context, mode CountMode) int { return 0 }
func (n nullTable) Attrs(ctx context.Context) TableAttrs        { return TableAttrs{Name: "NA"} }
func (n nullTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	return &NullTableScanner{}
}

// Marshal implements the Table interface
func (n nullTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	enc.PutRawBytes(nullTableMarshalMagic[:])
	h := n.Hash()
	enc.PutHash(h)
}

// Hash implements the Table interface.
func (n nullTable) Hash() hash.Hash {
	return hash.Hash{0x0d, 0xdb, 0xb1, 0x87, 0xe6, 0x90, 0xa2, 0x46,
		0x23, 0x54, 0x0a, 0x74, 0xc2, 0x8b, 0xe9, 0x26,
		0xfb, 0x57, 0x5b, 0x05, 0xad, 0x0a, 0xcc, 0x89,
		0xef, 0x4c, 0xd7, 0x03, 0xd1, 0xac, 0xac, 0x95}
}

func NewNullTable() Table {
	return &nullTable{}
}

// NullScanner implements TableScanner for NA. Its Scan() always returns false.
type NullTableScanner struct{}

func (n *NullTableScanner) Scan() bool   { return false }
func (n *NullTableScanner) Value() Value { panic("null") }

// simpleTableImpl is a trivial table that stores rows in memory.
type simpleTable struct {
	rows  []Value
	hash  hash.Hash
	attrs TableAttrs
}

func appendSimpleTable(t *simpleTable, rows ...Value) *simpleTable {
	return &simpleTable{
		rows:  append(t.rows, rows...),
		hash:  t.hash.Merge(hashValues(rows)),
		attrs: t.attrs,
	}
}

type simpleTableScanner struct {
	parent                 *simpleTable
	startIndex, limitIndex int
	index                  int
}

func (t *simpleTableScanner) Scan() bool {
	t.index++
	return t.index < t.limitIndex
}

func (t *simpleTableScanner) Value() Value {
	return t.parent.rows[t.index]
}

func (t *simpleTable) Prefetch(ctx context.Context) {}

func (t *simpleTable) Len(ctx context.Context, mode CountMode) int { return len(t.rows) }

func (t *simpleTable) MarshalBinary() ([]byte, error) {
	panic("use Marshal instead")
}

func (t *simpleTable) UnmarshalBinary([]byte) error {
	panic("use Unmarshal instead")
}

func (t *simpleTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	if len(t.rows) < 65536 {
		MarshalTableInline(ctx, enc, t)
	} else {
		MarshalTableOutline(ctx, enc, t)
	}
}

func (t *simpleTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	sc := &simpleTableScanner{parent: t}
	sc.startIndex, sc.limitIndex = ScaleShardRange(start, limit, total, len(t.rows))
	sc.index = sc.startIndex - 1 // First Scan() will increment it.
	return sc
}

func (t *simpleTable) Hash() hash.Hash                      { return t.hash }
func (t *simpleTable) Attrs(ctx context.Context) TableAttrs { return t.attrs }

// NewSimpleTable creates a trivial table that stores all the rows in memory.
func NewSimpleTable(recs []Value, h hash.Hash, attrs TableAttrs) Table {
	if h == hash.Zero {
		log.Panicf("empty hash")
	}
	v := &simpleTable{
		rows:  make([]Value, len(recs)),
		hash:  h,
		attrs: attrs,
	}
	copy(v.rows, recs)
	return v
}

var (
	defaultMarshalTableMagic       = UnmarshalMagic{0x93, 0x86}
	defaultMarshalTableInlineMagic = UnmarshalMagic{0xf3, 0xb8}
)

// DefaultTableLen computes the number of rows in the table accurately by
// scanning it.
func DefaultTableLen(ctx context.Context, t Table) int {
	// TODO: scan in parallel
	scanner := t.Scanner(ctx, 0, 1, 1)
	n := 0
	for scanner.Scan() {
		n++
	}
	return n
}

// materializeTable saves the table in a btsv file. The path name is of form
// "cachedir/hash.btsv", where hash is the hash of the table. So calling this
// function multiple times for the same table will be cheap.
//
// Writer should be a function that produces contents of the table in the given
// writer. if writer==nil, then  the following fuction is used:
//
//   func(w *BTSVShardWriter) {
// 			sc := t.Scanner(0, 1, 1)
// 			for sc.Scan() {
// 				w.Append(sc.Value())
// 			}
// 	 }
func materializeTable(ctx context.Context, t Table, writer func(w *BTSVShardWriter)) *btsvTable {
	if btsv, ok := t.(*btsvTable); ok {
		return btsv
	}
	if writer == nil {
		writer = func(w *BTSVShardWriter) {
			sc := t.Scanner(ctx, 0, 1, 1)
			for sc.Scan() {
				w.Append(sc.Value())
			}
		}
	}
	cacheName := t.Hash().String() + ".btsv"
	btsvPath, found := LookupCache(ctx, cacheName)
	if !found {
		w := NewBTSVShardWriter(ctx, btsvPath, 0, 1, t.Attrs(ctx))
		writer(w)
		w.Close(ctx)
		ActivateCache(ctx, cacheName, btsvPath)
	}
	bt := NewBTSVTable(btsvPath, astUnknown /*TODO:fix*/, t.Hash())
	return bt
}

// MarshalTableOutline marshals the given table by first writing its contents in
// btsv format in the cachedir, then marshaling the pathname of the generated
// btsv file.
func MarshalTableOutline(ctx MarshalContext, enc *marshal.Encoder, t Table) {
	h := t.Hash()
	btsv := materializeTable(ctx.ctx, t, nil)
	MarshalTablePath(enc, btsv.dir, singletonBTSVFileHandler, h)
}

// MarshalTablePath encodes a table as a tuple of <pathname, filehandler, hash>.
// It does not encode the table contents, so this function is suitable only for
// files that can be accessed from any machine (e.g., S3) and are immutable.
func MarshalTablePath(enc *marshal.Encoder, path string, fh FileHandler, hash hash.Hash) {
	enc.PutRawBytes(defaultMarshalTableMagic[:])
	enc.PutHash(hash)
	enc.PutString(fh.Name())
	enc.PutString(path)
}

func defaultUnmarshalTable(ctx UnmarshalContext, hash hash.Hash, dec *marshal.Decoder) Table {
	fh := GetFileHandlerByName(dec.String())
	path := dec.String()
	return newTableFromFileWithHash(ctx.ctx, path, astUnknown /*TODO:fix*/, fh, hash)
}

// MarshalTableInline marshals the given table contents directly. It shall be
// used only for small tables.
func MarshalTableInline(ctx MarshalContext, enc *marshal.Encoder, t Table) {
	enc.PutRawBytes(defaultMarshalTableInlineMagic[:])
	enc.PutHash(t.Hash())
	enc.PutVarint(int64(t.Len(ctx.ctx, Exact)))
	sc := t.Scanner(ctx.ctx, 0, 1, 1)
	for sc.Scan() {
		sc.Value().Marshal(ctx, enc)
	}
}

func defaultUnmarshalTableInline(ctx UnmarshalContext, hash hash.Hash, dec *marshal.Decoder) Table {
	nrows := int(dec.Varint())
	rows := make([]Value, nrows)
	for i := 0; i < nrows; i++ {
		rows[i].Unmarshal(ctx, dec)
	}
	return NewSimpleTable(rows, hash, TableAttrs{Name: "unmarshalinline"} /*TODO: save&restore attrs*/)
}

// If verifyFileHash = true, some table implementation will verify that the
// fileHash (see below) hasn't changed since it was first computed. The check is
// slow especially on S3, so it's disabled by default.
var VerifyFileHash = false

// IsFileImmutable checks if the pathname matches gql.Opts.ImmutableFilesRE.
func IsFileImmutable(path string) bool {
	for _, re := range immutableFilesRE {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}

// FileHash computes a hash from a pathname and the file's modtime. The
// resulting value can be used as the hash for a file-based Table.
func FileHash(ctx context.Context, path string, ast ASTNode) hash.Hash {
	if IsFileImmutable(path) {
		return hash.String(path)
	}
	in, err := file.Open(ctx, path)
	if err != nil {
		log.Panicf("hashFile open %s: %v", path, err)
	}
	stat, err := in.Stat(ctx)
	if err != nil {
		log.Panicf("hashFile stat %s: %v", path, err)
	}
	if err := in.Close(ctx); err != nil {
		log.Panicf("hashFile close %s: %v", path, err)
	}
	return hash.String(path).Merge(hash.Time(stat.ModTime())).Merge(hash.Int(stat.Size()))
}

// tryPrintTableInline tries to print the table in form "[row0,row1,...]".  If
// the resulting string is <= maxLength bytes long, it returns that string.
// Else it returns "".
func tryPrintTableInline(ctx context.Context, t Table, maxLen int) string {
	if maxLen <= 0 {
		maxLen = defaultMaxInlineTablePrintLen
	}
	if t.Len(ctx, Approx) > maxLen {
		return ""
	}
	out := termutil.NewBufferPrinter()
	out.WriteString("[")
	args := PrintArgs{
		Out:                out,
		Mode:               PrintCompact,
		MaxInlinedTableLen: maxLen,
	}
	sc := t.Scanner(ctx, 0, 1, 1)
	n := 0
	for sc.Scan() {
		if out.Len() >= maxLen {
			return ""
		}
		if n > 0 {
			out.WriteString(",")
		}
		sc.Value().Print(ctx, args)
		n++
	}
	out.WriteString("]")
	return out.String()
}

// PrintTable prints the contents of a table.
func printTable(ctx context.Context, args PrintArgs, t Table, depth int) {
	// TODO(saito) Remove the depth param. Mode suffices.
	if args.Mode == PrintCompact || depth > 0 {
		data := tryPrintTableInline(ctx, t, args.MaxInlinedTableLen)
		if data != "" {
			args.Out.WriteString(data)
			return
		}
		tmpVar := "[omitted]"
		if args.TmpVars != nil {
			tmpVar = args.TmpVars.Register(NewTable(t))
		}
		args.Out.WriteString(tmpVar)
		return
	}

	sc := t.Scanner(ctx, 0, 1, 1)

	// Read one row from the table.
	readRow := func() (values []termutil.Column, err error) {
		if !sc.Scan() {
			return nil, io.EOF
		}
		val := sc.Value()
		switch val.Type() {
		case StructType:
			st := val.Struct(astUnknown)
			nCols := st.Len()
			values = make([]termutil.Column, nCols)
			for ci := 0; ci < nCols; ci++ {
				col := st.Field(ci)
				out := termutil.NewBufferPrinter()
				col.Value.printRec(ctx, PrintArgs{
					Out:                out,
					Mode:               PrintCompact,
					TmpVars:            args.TmpVars,
					MaxInlinedTableLen: args.MaxInlinedTableLen,
				}, depth+1)
				values[ci].Name = col.Name
				values[ci].Value = out.String()
			}
		default:
			out := termutil.NewBufferPrinter()
			val.printRec(ctx, PrintArgs{
				Out:                out,
				Mode:               PrintCompact,
				TmpVars:            args.TmpVars,
				MaxInlinedTableLen: args.MaxInlinedTableLen,
			}, depth+1)
			values = []termutil.Column{{
				Name:  symbol.AnonRow,
				Value: out.String(),
			}}
		}
		return
	}

	args.Out.WriteTable(readRow)
}

type (
	// UnmarshalMagic identifies a magic number prefixed before a serialized Table
	// or Struct. It is used to look up the unmarshal callback.
	UnmarshalMagic [2]byte

	// UnmarshalTableCallback reconstruct a table object from a binary stream
	// produced by Table.MarshalGOB
	UnmarshalTableCallback func(ctx UnmarshalContext, hash hash.Hash, dec *marshal.Decoder) Table
)

var tableUnmarshalers = map[UnmarshalMagic]UnmarshalTableCallback{}

func unmarshalTable(ctx UnmarshalContext, dec *marshal.Decoder) Table {
	magic := UnmarshalMagic{}
	dec.RawBytes(magic[:])
	cb := tableUnmarshalers[magic]
	if cb == nil {
		log.Panicf("unmarshaltable: unknown magic %v", magic)
	}
	h := dec.Hash()
	return cb(ctx, h, dec)
}

// RegisterTableUnmarshaler registers a magic number for table marshaler and
// unmarshaler. "cb" will be called to unmarshal a table with the given
// magic. The MarshalGOB method of the table type should use the same magic.
//
// This function must be called in an init() function.
func RegisterTableUnmarshaler(magic UnmarshalMagic, cb UnmarshalTableCallback) {
	if _, ok := tableUnmarshalers[magic]; ok {
		log.Panicf("Table marshaler for %x already registered", magic)
	}
	tableUnmarshalers[magic] = cb
}

func init() {
	RegisterTableUnmarshaler(defaultMarshalTableMagic, defaultUnmarshalTable)
	RegisterTableUnmarshaler(defaultMarshalTableInlineMagic, defaultUnmarshalTableInline)
	RegisterTableUnmarshaler(nullTableMarshalMagic, func(ctx UnmarshalContext, h hash.Hash, dec *marshal.Decoder) Table {
		if h != (nullTable{}).Hash() {
			panic(h)
		}
		return nullTable{}
	})
}

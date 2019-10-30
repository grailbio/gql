package gql

// This file implements a basic TSV reader.

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"io"
	"math"
	"runtime"
	"strconv"
	"sync"
	"unicode/utf8"

	"github.com/grailbio/base/compress"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/gql/columnsorter"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/gql/termutil"
)

// the size of the buffer used for writing column files.
const colWriterBufferSize = 16 * 1024 * 1024

var (
	// MaxTSVRowsInMemory defines the maximum number of TSV rows to read and cache
	// in memory.  If a TSV file contains more rows than this limit, the file
	// contents are read incrementally in each scanner.
	//
	// This flag is exposed only for unittesting.
	MaxTSVRowsInMemory = 65536
)

// TSVTable implements Table for a TSV file.
type TSVTable struct {
	hashOnce sync.Once
	hash     hash.Hash // table hash computed from the path & file attributes.
	ast      ASTNode   // source-code location.
	path     string    // tsv file name

	// fileHandler is usually TSVFileHandler, but it may be BEDFileHandler or some
	// such.
	fileHandler                FileHandler
	pretendFileNamesAreStrings bool

	lenOnce sync.Once
	len     int

	mu          sync.Mutex
	initialized bool

	format *TSVFormat

	nRows int // # of rows. Set in init.
	table Table

	// Underlying open tsv file.
	in file.File
}

type tsvTableScanner struct {
	ctx    context.Context
	parent *TSVTable
	in     file.File
	// For closing & checksum the compression reader.  it is a noop closer if the
	// file is not compressed.
	compressr io.Closer
	r         *csv.Reader

	tmpCols []StructField
	row     Value
}

func (s *tsvTableScanner) Value() Value { return s.row }
func (s *tsvTableScanner) Scan() bool {
	rawRow, err := s.r.Read()
	if err != nil {
		if err == io.EOF {
			return false
		}
		Panicf(s.parent.ast, "read %v: %v", s.parent.path, err)
	}
	CheckCancellation(s.ctx)
	for fi, field := range s.parent.format.Columns {
		if len(rawRow) < fi {
			s.tmpCols[fi] = StructField{symbol.Intern(field.Name), Null}
			continue
		}
		s.tmpCols[fi].Value = s.parent.parseRowString(rawRow[fi], field.Type)
	}
	s.row = NewStruct(NewSimpleStruct(s.tmpCols...))
	return true
}

func isNull(v string) bool {
	if v == "" {
		return true
	}
	ch := v[0]
	if ch != 'n' && ch != 'N' && ch != '#' && ch != '-' { // quick check
		return false
	}
	if v == "#N/A" || v == "#N/A N/A" || v == "#NA" || v == "-NaN" ||
		v == "-nan" || v == "N/A" || v == "NA" || v == "NULL" || v == "NaN" || v == "nan" {
		return true
	}
	if len(v) >= 7 && ch == '-' && v[1] == '1' && v[3] == '#' &&
		(v[4:] == "IND" || v[4:] == "QNAN") {
		return true
	}
	return false
}

func (t *TSVTable) parseRowString(rowStr string, typ ValueType) Value {
	if isNull(rowStr) {
		return Null
	}
	switch typ {
	case IntType:
		v, err := strconv.ParseInt(rowStr, 0, 64)
		if err == nil {
			return NewInt(v)
		}
		f, err := strconv.ParseFloat(rowStr, 64)
		if err == nil && f <= math.MaxInt64 && f >= math.MinInt64 {
			return NewInt(int64(math.Trunc(f)))
		}
		Panicf(t.ast, "parserow: %v cannot be parsed as integer: %v", rowStr, err)
	case FloatType:
		v, err := strconv.ParseFloat(rowStr, 64)
		if err == nil {
			return NewFloat(v)
		}
		Panicf(t.ast, "parserow: %v cannot be parsed as float: %v", rowStr, err)
	case BoolType:
		switch rowStr {
		case "Y", "yes":
			return True
		case "N", "no":
			return False
		}
		v, err := strconv.ParseBool(rowStr)
		if err == nil {
			return NewBool(v)
		}
		Panicf(t.ast, "parserow: %v cannot be parsed as bool: %v", rowStr, err)
	case StringType:
		return NewString(rowStr)
	case FileNameType:
		return NewFileName(rowStr)
	case EnumType:
		return NewEnum(rowStr)
	case CharType:
		ch, n := utf8.DecodeRuneInString(rowStr)
		if ch != utf8.RuneError && n == len(rowStr) {
			return NewChar(ch)
		}
		Panicf(t.ast, "parserow: %v cannot be parsed as char", rowStr)
	case DateTimeType, DateType:
		v := ParseDateTime(rowStr)
		if v.Type() != typ {
			Panicf(t.ast, "parserow: %v cannot be parsed as datetime or date (%v)", rowStr, typ)
		}

		return v
	}
	Panicf(t.ast, "parserow: unknown data type %v", typ)
	return Value{}
}

func newCSVReader(ctx context.Context, in io.Reader) *csv.Reader {
	csvr := csv.NewReader(in)
	csvr.Comma = '\t'
	csvr.Comment = '#'
	csvr.TrimLeadingSpace = false
	csvr.LazyQuotes = true
	return csvr
}

func (t *TSVTable) init(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.initialized {
		return
	}
	in, err := file.Open(ctx, t.path)
	if err != nil {
		Panicf(t.ast, "init %s: open: %v", t.path, err)
	}
	compressr, _ := compress.NewReader(in.Reader(ctx))
	csvr := newCSVReader(ctx, compressr)
	rawRows := make([][]string, 0, MaxTSVRowsInMemory)
	readAll := false
	for i := 0; i < MaxTSVRowsInMemory; i++ {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				readAll = true
				break
			}
			Panicf(t.ast, "read %s: csv.ReadAll: %v", in.Name(), err)
		}
		rawRows = append(rawRows, row)
	}

	if t.format == nil {
		format := guessTSVFormat(t.path, rawRows)
		t.format = &format
	}

	Logf(t.ast, "read %v (%d rows, readall: %v), %d #header, %d cols",
		in.Name(), len(rawRows), readAll, t.format.HeaderLines, len(t.format.Columns))
	rows := []Value{}
	tmpCols := make([]StructField, len(t.format.Columns))
	for fi, field := range t.format.Columns {
		tmpCols[fi].Name = symbol.Intern(field.Name)
	}
	for li := t.format.HeaderLines; li < len(rawRows); li++ {
		rawRow := rawRows[li]
		for fi, field := range t.format.Columns {
			if len(rawRow) <= fi {
				tmpCols[fi].Value = Null
				continue
			}
			tmpCols[fi].Value = t.parseRowString(rawRow[fi], field.Type)
		}
		rows = append(rows, NewStruct(NewSimpleStruct(tmpCols...)))
	}
	if err := compressr.Close(); err != nil {
		Panicf(t.ast, "close(compression) %s: %v", in.Name(), err)
	}
	if readAll {
		if err := in.Close(ctx); err != nil {
			Errorf(t.ast, "close %s: %v", in.Name(), err)
		}
		t.nRows = len(rows)
		t.table = NewSimpleTable(rows, t.Hash(), TableAttrs{Name: "tsv", Path: t.path, Columns: t.format.Columns})
	} else {
		t.in = in
	}
	t.initialized = true
}

// Prefetch implements the Table interface.
func (t *TSVTable) Prefetch(ctx context.Context) { go Recover(func() { t.init(ctx) }) }

// Len implements the Table interface
func (t *TSVTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		// This codepath is taken when printing the toplevel CCGA tables when it
		// checks whether to print child TSV table contents inline. Returning a
		// large value here effectively disables this optimization.
		//
		// TODO(saito) Perhaps return an accurate value if the file is on the local
		// file system?
		return 100000
	}
	t.lenOnce.Do(func() { t.len = DefaultTableLen(ctx, t) })
	return t.len
}

// Hash implements the Table interface.
func (t *TSVTable) Hash() hash.Hash {
	t.hashOnce.Do(func() {
		if t.hash == hash.Zero || VerifyFileHash {
			h := FileHash(BackgroundContext, t.path, t.ast)
			if t.hash != hash.Zero && t.hash != h {
				Panicf(t.ast, "mismatched hash for '%s' (file changed in the background?)", t.path)
			}
			t.hash = h
		}
	})
	return t.hash
}

// Marshal implements the Table interface.
func (t *TSVTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	MarshalTablePath(enc, t.path, t.fileHandler, t.Hash())
}

// Attrs implements the Table interface
func (t *TSVTable) Attrs(ctx context.Context) TableAttrs {
	t.init(ctx)
	return TableAttrs{Name: "tsv", Path: t.path, Columns: t.format.Columns}
}

// Scanner implements the Table interface.
func (t *TSVTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	t.init(ctx)
	t.mu.Lock()
	if t.table != nil {
		t.mu.Unlock()
		return t.table.Scanner(ctx, start, limit, total)
	}
	// The table was too large
	if start > 0 {
		t.mu.Unlock()
		return &NullTableScanner{}
	}
	var in file.File
	if t.in != nil {
		in, t.in = t.in, nil
	}
	t.mu.Unlock()
	if in == nil {
		var err error
		in, err = file.Open(ctx, t.path)
		if err != nil {
			Panicf(t.ast, "tsv open %v: %v", t.path, err)
		}
	}
	if n, err := in.Reader(ctx).Seek(0, io.SeekStart); err != nil || n != 0 {
		Panicf(t.ast, "seek: %v", err)
	}
	compressr, _ := compress.NewReader(in.Reader(ctx))
	sc := &tsvTableScanner{
		ctx:       ctx,
		parent:    t,
		in:        in,        //takes ownership
		compressr: compressr, // takes ownership
		r:         newCSVReader(ctx, compressr),
		tmpCols:   make([]StructField, len(t.format.Columns))}
	for fi, field := range t.format.Columns {
		sc.tmpCols[fi].Name = symbol.Intern(field.Name)
	}
	for i := 0; i < t.format.HeaderLines; i++ {
		_, err := sc.r.Read()
		if err != nil {
			Panicf(t.ast, "readheader %v: %v", t.path, err)
		}
	}
	runtime.SetFinalizer(sc, func(sc *tsvTableScanner) {
		if err := sc.compressr.Close(); err != nil {
			// At this point there's not much we can do to report the error upstream
			Errorf(t.ast, "close(compress) %s: %v", t.path, err)
		}
		if err := sc.in.Close(ctx); err != nil {
			Errorf(t.ast, "close %s: %v", t.path, err)
		}
	})
	return sc
}

// NewTSVTable creates a Table for reading the given TSV file.  tableName is
// just for logging. "h" is the hash of the inputs that generates this table.
func NewTSVTable(path string, ast ASTNode, h hash.Hash, fh FileHandler, format *TSVFormat) Table {
	Debugf(ast, "NewTSVTable %s: missed cache (hash %v) ", path, h)
	if fh == nil {
		fh = singletonTSVFileHandler
	}
	t := &TSVTable{
		hash:        h,
		ast:         ast,
		path:        path,
		fileHandler: fh,
		format:      format,
		nRows:       -1,
	}
	runtime.SetFinalizer(t, func(t *TSVTable) {
		if t.in != nil {
			t.in.Close(BackgroundContext) // nolint: errcheck
		}
	})
	return t
}

// TSV writer

func (w *defaultTSVWriter) writeRow(cols []string) {
	w.buf.Reset()
	for i, col := range cols {
		if i > 0 {
			w.buf.WriteByte('\t')
		}
		for j := 0; j < len(col); j++ {
			ch := col[j]
			if ch == '\t' {
				ch = ' '
			}
			w.buf.WriteByte(ch)
		}
	}
	w.buf.WriteByte('\n')
	if _, err := w.w.Write(w.buf.Bytes()); err != nil {
		log.Panic(err)
	}
}

// A dummy column name used to produce an empty table. We can't produce an empty
// file or tidy dictionary validator will complain.
var dummyTSVCol = symbol.Intern("dummycol")

// Create a tidy data dictionary file describing columns in the given table.
func writeTSVDict(ctx context.Context, dictPath string, colIDs []symbol.ID, colTypes []ValueType, colDescs []string, gzipFile bool) {
	w := newDefaultTSVWriter(ctx, dictPath, []symbol.ID{symbol.Intern("column_name"), symbol.Intern("type"), symbol.Intern("description")}, true, gzipFile)
	for ci, colID := range colIDs {
		colName := colID.Str()
		typeName := ""
		switch colTypes[ci] {
		case NullType:
			// This happens only when the there was no valid value in the column
			// (either now row in the table, or all cells are NA).  So we emit an
			// arbitrary type here.
			typeName = "int"
		case BoolType:
			typeName = "bool"
		case IntType:
			typeName = "int"
		case FloatType:
			typeName = "float"
		case StringType:
			typeName = "string"
		case FileNameType:
			typeName = "filename"
		case TableType:
			typeName = "filename"
		case EnumType:
			// TODO(saito) show enum values.
			typeName = "enum:"
		case CharType:
			typeName = "char"
		case DateTimeType:
			typeName = "datetime"
		case DateType:
			typeName = "date"
		case DurationType:
			typeName = "duration"
		default:
			log.Panicf("tidytsv %s: Unknown type %+v for column '%s'", dictPath, colTypes[ci], colName)
		}
		desc := "Unknown"
		if colDescs != nil && colDescs[ci] != "" {
			desc = colDescs[ci]
		}
		w.writeRow([]string{colName, typeName, desc})
	}
	w.Close()
}

type tsvWriter interface {
	Append(v Value)
	Close()
	Discard()
}

// tsvColumnMap maps column index -> column name mappings and its inverse.
type tsvColumnMap struct {
	ids    []symbol.ID
	idxMap map[symbol.ID]int
}

func newTSVColumnMap(ids []symbol.ID) tsvColumnMap {
	m := tsvColumnMap{ids: ids, idxMap: map[symbol.ID]int{}}
	for i, id := range ids {
		m.idxMap[id] = i
	}
	return m
}

func (m tsvColumnMap) len() int { return len(m.ids) }

func (m tsvColumnMap) lookupByID(col symbol.ID) int {
	if i, ok := m.idxMap[col]; ok {
		return i
	}
	return -1
}

type defaultTSVWriter struct {
	ctx            context.Context
	path           string
	out            file.File
	w              io.Writer
	closeCallbacks []func()

	buf     bytes.Buffer
	colMap  tsvColumnMap
	tmpBuf  *termutil.BufferPrinter
	tmpVars TmpVars
	colVals []string
}

func newDefaultTSVWriter(ctx context.Context, path string, colIDs []symbol.ID, headerLine, gzipFile bool) *defaultTSVWriter {
	if len(colIDs) == 0 {
		panic(path)
	}
	w := &defaultTSVWriter{
		ctx:    ctx,
		path:   path,
		colMap: newTSVColumnMap(colIDs),
		tmpBuf: termutil.NewBufferPrinter(),
	}
	var err error
	if w.out, err = file.Create(ctx, w.path); err != nil {
		log.Panicf("writetsvdata %s: %v", path, err)
	}
	w.w = w.out.Writer(ctx)

	if gzipFile {
		gwr := gzip.NewWriter(w.w)
		w.w = gwr
		w.closeCallbacks = append(w.closeCallbacks, func() {
			if err := gwr.Flush(); err != nil {
				log.Panicf("writeTSVDict %v: %v", path, err)
			}
			if err := gwr.Close(); err != nil {
				log.Panicf("writeTSVDict %v: %v", path, err)
			}
		})
	}
	buffered := bufio.NewWriterSize(w.w, colWriterBufferSize)
	w.closeCallbacks = append(w.closeCallbacks, func() {
		if err := buffered.Flush(); err != nil {
			log.Panicf("failed to flush buffered stream for %v: %v", path, err)
		}
	})
	w.w = buffered
	if headerLine {
		colNames := make([]string, len(w.colMap.ids))
		for ci, colID := range w.colMap.ids {
			colNames[ci] = colID.Str()
		}
		w.writeRow(colNames)
	}
	w.colVals = make([]string, len(w.colMap.ids))
	return w
}

func (w *defaultTSVWriter) valueToString(v Value) string {
	if v.Type() == InvalidType {
		panic(v)
	}
	w.tmpBuf.Reset()
	v.Print(w.ctx, PrintArgs{
		Out:     w.tmpBuf,
		Mode:    PrintValues,
		TmpVars: &w.tmpVars,
	})
	return w.tmpBuf.String()
}

// Append implements tsvWriter
func (w *defaultTSVWriter) Append(v Value) {
	if v.Type() != StructType {
		w.colVals[0] = w.valueToString(v)
	} else {
		row := v.Struct(nil)
		for i := range w.colVals {
			w.colVals[i] = "NA"
		}
		nFields := row.Len()
		for fi := 0; fi < nFields; fi++ {
			f := row.Field(fi)
			colIdx := w.colMap.lookupByID(f.Name)
			if colIdx < 0 {
				log.Panicf("writetsv %s: column %s not found", w.path, f.Name.Str())
			}
			w.colVals[colIdx] = w.valueToString(f.Value)
		}
	}
	w.writeRow(w.colVals)
}

// Close implements tsvWriter
func (w *defaultTSVWriter) Close() {
	for i := len(w.closeCallbacks) - 1; i >= 0; i-- {
		w.closeCallbacks[i]()
	}
	if err := w.out.Close(w.ctx); err != nil {
		log.Panicf("writetsvdata %v: close: %v", w.path, err)
	}
}

// Discard implements tsvWriter
func (w *defaultTSVWriter) Discard() { w.out.Discard(w.ctx) }

func maybeAddDummyColumn(colIDs []symbol.ID, colTypes []ValueType, colDescs []string) ([]symbol.ID, []ValueType, []string) {
	if len(colIDs) == 0 {
		colIDs, colTypes, colDescs = []symbol.ID{dummyTSVCol}, []ValueType{IntType}, []string{"Unknown"}
	}
	return colIDs, colTypes, colDescs
}

// writeTSVHelper is used internally by WriteTSV and WriteColumnarTSV.
//
// 1. It first tries to produce the BTSV file and the (columnar) tsv file,
// assuming that all the rows have the same set of columns in the same order. If
// that assumption is met, then this function is done at that point.
//
// 2. If rows have different set of columns or columns in in different order,
// then this function takes the BTSV file created in the previous step and
// creates the final (columnar) tsv file with union of all the columns found in
// the BTSV.
//
// In the end, this function creates two files: (a) a btsv file under the
// cacheRoot, and (b) the (columnar) TSV file.
//
// The arg writerFactory is a function that creates a tsvWriter for the given
// set of columns. It may be called multiple times, but never concurrently.  Arg
// table is the source table. gzipFiles controls whether the contents should be
// gzip compressed.
func writeTSVHelper(ctx context.Context,
	writerFactory func(colIDs []symbol.ID) tsvWriter,
	dictPath string, table Table, gzipFiles bool) {

	// If the table is a btsvTable, or it already has a dump in the cache dir,
	// skip the first step.
	btsv, ok := table.(*btsvTable)
	if !ok {
		cacheName := table.Hash().String() + ".btsv"
		btsvPath, found := LookupCache(ctx, cacheName)
		if !found {
			// Step 1.
			done := tryWriteToTSVAndBTSV(ctx, writerFactory, dictPath, btsvPath, table, gzipFiles)
			ActivateCache(ctx, cacheName, btsvPath)
			if done {
				return
			}
		}
		btsv = NewBTSVTable(btsvPath, astUnknown /*TODO:fix*/, table.Hash())
	}

	// Step 2.
	btsv.init(ctx) // Load the shard indexes.
	colSorter := columnsorter.New()
	for _, shard := range btsv.shards {
		cols := []symbol.ID{}
		for _, c := range shard.cols {
			cols = append(cols, c.id)
		}
		colSorter.AddColumns(cols)
	}
	colSorter.Sort()
	colMap := map[symbol.ID]btsvTableColumnSpec{}
	for _, shard := range btsv.shards {
		for _, c := range shard.cols {
			colMap[c.id] = c
		}
	}
	colIDs := colSorter.Columns()
	colTypes := make([]ValueType, len(colIDs))
	colDescs := make([]string, len(colIDs))
	for ci, colID := range colIDs {
		colTypes[ci] = colMap[colID].typ
		colDescs[ci] = colMap[colID].description
	}
	colIDs, colTypes, colDescs = maybeAddDummyColumn(colIDs, colTypes, colDescs)
	w := writerFactory(colIDs)
	sc := table.Scanner(ctx, 0, 1, 1)
	for sc.Scan() {
		w.Append(sc.Value())
	}
	w.Close()
	if dictPath != "" {
		writeTSVDict(ctx, dictPath, colIDs, colTypes, colDescs, gzipFiles)
	}
}

// tryWriteToTSVAndBTSV does the first step of writeTSVHelper. It writes
// contents of "table" to two tables, a btsv cache and the final destination
// file, assuming that all the columns have the same set of columns in the same
// order.  If the assumption is met, it returns true. Otherwise, the caller
// should copy rows from the generated btsv file to the final tsv.
func tryWriteToTSVAndBTSV(
	ctx context.Context,
	writerFactory func(colIDs []symbol.ID) tsvWriter,
	dictPath, btsvPath string, table Table, gzipFiles bool) bool {
	var (
		wg       sync.WaitGroup
		tsvOK    = true // do all the rows we've seen so far have the same layout?
		firstRow = true
		tsvW     tsvWriter
		colIDs   []symbol.ID
		colMap   tsvColumnMap
		colTypes []ValueType
		colFound []bool
		tsvReqCh chan []Value

		btsvReqCh = make(chan []Value, 100)
		btsvW     = NewBTSVShardWriter(ctx, btsvPath, 0, 1, table.Attrs(ctx))
	)

	// Start the BTSV writer.  The TSV writer will be created after seeing the
	// first row, because the writer needs to know the set of columns in the
	// table.
	wg.Add(1)
	go func() {
		for rows := range btsvReqCh {
			for _, row := range rows {
				btsvW.Append(row)
			}
		}
		wg.Done()
	}()

	const batchSize = 8192
	sc := table.Scanner(ctx, 0, 1, 1)
	reqBuf := make([]Value, 0, batchSize)

	for sc.Scan() {
		val := sc.Value()
		row := val.Struct(astUnknown) // TODO(saito) pass a valid source code location.
		reqBuf = append(reqBuf, val)

		if firstRow {
			// Start the TSV writer, optimistically assuming that all the subsequent
			// rows have the same layout as this one.
			firstRow = false
			for i := 0; i < row.Len(); i++ {
				colIDs = append(colIDs, row.Field(i).Name)
				colTypes = append(colTypes, row.Field(i).Value.Type())
			}
			colIDs, colTypes, _ = maybeAddDummyColumn(colIDs, colTypes, nil)
			colMap = newTSVColumnMap(colIDs)
			colFound = make([]bool, len(colIDs))

			tsvW = writerFactory(colIDs)
			tsvReqCh = make(chan []Value, 100)
			wg.Add(1)
			go func() {
				for rows := range tsvReqCh {
					for _, row := range rows {
						tsvW.Append(row)
					}
				}
				wg.Done()
			}()
		} else {
			// Verify that this row has the same layout as the first row's.
			if tsvOK {
				if colMap.len() != row.Len() {
					tsvOK = false
				}
				for i := 0; i < colMap.len(); i++ {
					colFound[i] = false
				}
				for i := 0; i < colMap.len(); i++ {
					ci := colMap.lookupByID(row.Field(i).Name)
					if ci < 0 || colFound[ci] {
						tsvOK = false
					} else {
						colFound[ci] = true
					}
				}
			}
		}
		if len(reqBuf) >= batchSize {
			btsvReqCh <- reqBuf
			if tsvOK {
				tsvReqCh <- reqBuf
			}
			reqBuf = make([]Value, 0, batchSize)
		}
	}

	if len(reqBuf) > 0 {
		btsvReqCh <- reqBuf
		if tsvOK {
			tsvReqCh <- reqBuf
		}
	}

	close(btsvReqCh)
	if tsvW != nil {
		close(tsvReqCh)
	}
	wg.Wait()

	// Close the btsv and tsv files in parallel.
	traverse.Each(2, func(n int) error { // nolint: errcheck
		if n == 0 {
			btsvW.Close(ctx)
			return nil
		}
		if tsvOK {
			if tsvW == nil {
				// no data found.
				colIDs, colTypes, _ = maybeAddDummyColumn(nil, nil, nil)
				tsvW = writerFactory(colIDs)
			}
			tsvW.Close()
			if dictPath != "" {
				writeTSVDict(ctx, dictPath, colIDs, colTypes, nil, gzipFiles)
			}
		} else {
			tsvW.Discard()
		}
		return nil
	})
	return tsvOK
}

// WriteTSV writes the table contents to a TSV file.  If headerLine=true, it
// emits the column names in the first line. If gzip is true, the file is
// compressed using gzip.
func WriteTSV(ctx context.Context, path string, table Table, headerLine, gzip bool) {
	writeTSVHelper(ctx, func(colIDs []symbol.ID) tsvWriter {
		return newDefaultTSVWriter(ctx, path, colIDs, headerLine, gzip)
	}, "", table, gzip)
}

// TSVFileHandler is a FileHandler implementation for TSV files.
type tsvFileHandler struct{}

var singletonTSVFileHandler = &tsvFileHandler{}

func TSVFileHandler() FileHandler {
	return singletonTSVFileHandler
}

// Name implements FileHandler.
func (fh *tsvFileHandler) Name() string { return "tsv" }

// Open implements FileHandler.
func (fh *tsvFileHandler) Open(ctx context.Context, path string, ast ASTNode, hash hash.Hash) Table {
	return NewTSVTable(path, ast, hash, fh, nil)
}

// Write implements FileHandler.
func (*tsvFileHandler) Write(ctx context.Context, path string, ast ASTNode, table Table, nShard int, overwrite bool) {
	if _, err := file.Stat(ctx, path); err == nil {
		if !overwrite {
			log.Printf("write %v: file already exists and --overwrite-files=false.", path)
			return
		}
	}
	WriteTSV(ctx, path, table, true, false)
}

func init() {
	RegisterFileHandler(singletonTSVFileHandler, `\.tsv`+OptionalCompression)
}

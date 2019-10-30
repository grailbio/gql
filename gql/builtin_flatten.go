package gql

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"golang.org/x/sync/semaphore"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

// largeFlatTable is a Table implementation for a flattened tables when
// subshard=false.
type largeFlatTable struct {
	hash      hash.Hash
	ast       ASTNode // location in the source code. Only for error reporting.
	srcTables []Table // input tables.
	attrs     TableAttrs

	once sync.Once
	// cumApproxSrcTableLen[i] is the sum of Len(Approx) of srcTables[0..i].  Used
	// to guide sharded scanning.
	cumApproxSrcTableLen []int

	onceExactLen sync.Once
	exactLen     int64 // Len(Exact) value.
}

// largeFlatTableScanner implements TableScanner for largeFlatTable.
type largeFlatTableScanner struct {
	ctx          context.Context
	parent       *largeFlatTable
	start, limit int          // the scanner range, relative to [0,parent.Len(approx)].
	curLimit     int          // the limit of srcSc. start < curLimit <= limit.
	srcSc        TableScanner // iterates parent.srcTables
	cur          TableScanner // iterates table read from srcSc
}

// limitedWorkerGroup is similar to errgroup.Group, but with limited concurrency of NumCPU*2.
type limitedWorkerGroup struct {
	sem *semaphore.Weighted
	wg  sync.WaitGroup
	err errors.Once
}

func newLimitedWorkerGroup() *limitedWorkerGroup {
	return &limitedWorkerGroup{
		sem: semaphore.NewWeighted(int64(runtime.NumCPU() * 2)),
	}
}

func (wg *limitedWorkerGroup) Go(ctx context.Context, callback func()) {
	wg.wg.Add(1)
	if err := wg.sem.Acquire(ctx, 1); err != nil {
		log.Panic(err)
	}
	go func() {
		wg.err.Set(Recover(callback))
		wg.sem.Release(1)
		wg.wg.Done()
	}()
}

func (wg *limitedWorkerGroup) Wait() {
	wg.wg.Wait()
	if err := wg.err.Err(); err != nil {
		panic(err)
	}
}

// Scanner implements the Table interface.
func (t *largeFlatTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	t.init(ctx)
	scanStart, scanLimit := ScaleShardRange(start, limit, total, t.Len(ctx, Approx))
	Logf(t.ast, "flattable(L) scanner %+v (%d tables): range [%d,%d)/%d, scanrange=[%d,%d)/%d",
		t.attrs, len(t.srcTables), start, limit, total, scanStart, scanLimit, t.Len(ctx, Approx))
	sc := &largeFlatTableScanner{
		ctx:      ctx,
		parent:   t,
		start:    scanStart,
		limit:    scanLimit,
		curLimit: scanStart,
	}
	return sc
}

// Len implements the Table interface.
func (t *largeFlatTable) Len(ctx context.Context, mode CountMode) int {
	t.init(ctx)
	if mode == Approx {
		n := len(t.srcTables)
		if n == 0 {
			return 0
		}
		return t.cumApproxSrcTableLen[n-1]
	}
	t.onceExactLen.Do(func() {
		wg0 := newLimitedWorkerGroup()
		wg1 := newLimitedWorkerGroup() // prevent deadlocks
		for _, table := range t.srcTables {
			wg0.Go(ctx, func() {
				sc := table.Scanner(ctx, 0, 1, 1)
				for sc.Scan() {
					subTable := sc.Value().Table(t.ast)
					wg1.Go(ctx, func() { atomic.AddInt64(&t.exactLen, int64(subTable.Len(ctx, Exact))) })
				}
			})
		}
		wg0.Wait()
		wg1.Wait()
	})
	return int(t.exactLen)
}

func (t *largeFlatTable) init(ctx context.Context) {
	t.once.Do(func() {
		t.cumApproxSrcTableLen = make([]int, len(t.srcTables))
		cum := 0
		for i, srcTable := range t.srcTables {
			cum += srcTable.Len(ctx, Approx)
			t.cumApproxSrcTableLen[i] = cum
		}
	})
}

// Prefetch implements the Table interface.
func (t *largeFlatTable) Prefetch(ctx context.Context) {
	for _, s := range t.srcTables {
		s.Prefetch(ctx)
	}
}

// Hash implements the Table interface.
func (t *largeFlatTable) Hash() hash.Hash { return t.hash }

var largeFlatMagic = UnmarshalMagic{0x8d, 0x01}

// Marshal implements the Table interface.
func (t *largeFlatTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	t.init(ctx.ctx)
	enc.PutRawBytes(largeFlatMagic[:])
	enc.PutHash(t.hash)
	enc.PutGOB(&t.ast)
	enc.PutVarint(int64(len(t.srcTables)))
	for i := range t.srcTables {
		t.srcTables[i].Marshal(ctx, enc)
	}
}

func unmarshalLargeFlatTable(ctx UnmarshalContext, h hash.Hash, dec *marshal.Decoder) Table {
	var ast ASTNode
	dec.GOB(&ast)
	t := &largeFlatTable{
		ast:   ast,
		hash:  h,
		attrs: TableAttrs{Name: "flat(L)"},
	}
	nSrcTables := int(dec.Varint())
	for i := 0; i < nSrcTables; i++ {
		t.srcTables = append(t.srcTables, unmarshalTable(ctx, dec))
	}
	return t
}

// Attrs implements the Table interface.
func (t *largeFlatTable) Attrs(ctx context.Context) TableAttrs { return t.attrs }

// Scan implements the TableScanner interface.
func (sc *largeFlatTableScanner) Scan() bool {
	for {
		if sc.cur == nil {
			if sc.srcSc == nil {
				nextOff := sc.curLimit
				srcTableIndex, srcTableStart, srcTableLimit, scanLimit := nextSubTable(sc.start, sc.limit, nextOff, sc.parent.cumApproxSrcTableLen)
				if srcTableIndex < 0 {
					return false
				}
				sc.srcSc = sc.parent.srcTables[srcTableIndex].Scanner(
					sc.ctx,
					nextOff-srcTableStart, scanLimit-srcTableStart,
					srcTableLimit-srcTableStart)
				sc.curLimit = scanLimit
			}
			if !sc.srcSc.Scan() {
				sc.srcSc = nil
				continue
			}
			val := sc.srcSc.Value()
			if val.Type() == StructType {
				st := val.Struct(sc.parent.ast)
				if st.Len() != 1 {
					Panicf(sc.parent.ast, "flatten: subtable must contain exactly one column, but found %v", val)
				}
				val = st.Field(0).Value
			}
			sc.cur = val.Table(sc.parent.ast).Scanner(sc.ctx, 0, 1, 1)
		}
		if sc.cur.Scan() {
			return true
		}
		sc.cur = nil
	}
}

// Value implements the TableScanner interface.
func (sc *largeFlatTableScanner) Value() Value {
	return sc.cur.Value()
}

// smallFlatTable implements a flattened tables when subshard=true
type smallFlatTable struct {
	hash      hash.Hash
	ast       ASTNode // location in the source code. Only for error reporting.
	srcTables []Table // the tables mentioned in the constructor
	attrs     TableAttrs

	lenOnce sync.Once
	// subTableCumLen[i] is the cumulative length of subTables in range [0,i].
	cumSubTableLen []int

	subTablesOnce sync.Once
	subTables     []Table // list of subtables read from srcTables[].

	exactLenOnce sync.Once
	exactLen     int64
}

// smallFlatTableScanner implements TableScanner for smallFlatTable.
type smallFlatTableScanner struct {
	ctx          context.Context
	parent       *smallFlatTable
	start, limit int // the scanner range, relative to [0,parent.Len(approx)].
	curLimit     int // the limit of "cur". start < curLimit <= limit.
	cur          TableScanner
}

// Scanner implements the Table interface.
func (t *smallFlatTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	t.initLen(ctx)
	scanStart, scanLimit := ScaleShardRange(start, limit, total, t.Len(ctx, Approx))
	Logf(t.ast, "flattable(S)  %+v (%d subtables): range [%d,%d)/%d, scanrange=[%d,%d)/%d",
		t.attrs, len(t.subTables), start, limit, total, scanStart, scanLimit, t.Len(ctx, Approx))
	sc := &smallFlatTableScanner{
		ctx:      ctx,
		parent:   t,
		start:    scanStart,
		limit:    scanLimit,
		curLimit: scanStart,
	}
	return sc
}

// Len implements the Table interface.
func (t *smallFlatTable) Len(ctx context.Context, mode CountMode) int {
	t.initLen(ctx)
	if mode == Approx {
		n := len(t.subTables)
		if n == 0 {
			return 0
		}
		return t.cumSubTableLen[n-1]
	}
	t.exactLenOnce.Do(func() {
		wg := newLimitedWorkerGroup()
		for i := range t.subTables {
			table := t.subTables[i]
			wg.Go(ctx, func() { atomic.AddInt64(&t.exactLen, int64(table.Len(ctx, Exact))) })
		}
		wg.Wait()
	})
	return int(t.exactLen)
}

// Scan implements the TableScanner interface.
func (sc *smallFlatTableScanner) Scan() bool {
	for {
		if sc.cur == nil {
			nextOff := sc.curLimit
			subTableIndex, subTableStart, subTableLimit, scanLimit := nextSubTable(sc.start, sc.limit, nextOff, sc.parent.cumSubTableLen)
			if subTableIndex < 0 {
				return false
			}
			sc.cur = sc.parent.subTables[subTableIndex].Scanner(
				sc.ctx,
				nextOff-subTableStart, scanLimit-subTableStart,
				subTableLimit-subTableStart)
			sc.curLimit = scanLimit
		}
		if sc.cur.Scan() {
			return true
		}
		sc.cur = nil
	}
}

// Value implements the TableScanner interface.
func (sc *smallFlatTableScanner) Value() Value {
	return sc.cur.Value()
}

// initLen computes the length of each subtable.
func (t *smallFlatTable) initLen(ctx context.Context) {
	t.lenOnce.Do(func() {
		t.initSubTables(ctx)
		n := len(t.subTables)
		Logf(t.ast, "flattable(S): reading %d subtables", n)
		subTableLen := make([]int, n)
		traverse.Parallel.Each(len(t.subTables), func(i int) error { // nolint: errcheck
			subTableLen[i] = t.subTables[i].Len(ctx, Approx)
			return nil
		})
		t.cumSubTableLen = make([]int, n)
		cum := 0
		for i := range subTableLen {
			cum += subTableLen[i]
			t.cumSubTableLen[i] = cum
		}
		Logf(t.ast, "flattable(S): finished computing length of %d subtables", n)
	})
}

func (t *smallFlatTable) initSubTables(ctx context.Context) {
	t.subTablesOnce.Do(func() {
		if len(t.srcTables) == 0 {
			// the table was created via unmarshal.
			return
		}
		for _, srcTable := range t.srcTables {
			Logf(t.ast, "flattable(S): reading srctable %v", srcTable.Attrs(ctx))
			sc := srcTable.Scanner(ctx, 0, 1, 1)
			for sc.Scan() {
				val := sc.Value()
				if val.Type() == StructType {
					st := val.Struct(t.ast)
					if st.Len() != 1 {
						Panicf(t.ast, "flattable(S): subtable must contain exactly one column, but found %v", val)
					}
					val = st.Field(0).Value
				}
				t.subTables = append(t.subTables, val.Table(t.ast))
			}
		}
		t.srcTables = nil // srcTables isn't needed any more.
		Logf(t.ast, "flattable(S): finished reading %d srctables", len(t.srcTables))
	})
}

// Prefetch implements the Table interface.
func (t *smallFlatTable) Prefetch(ctx context.Context) {}

// Hash implements the Table interface.
func (t *smallFlatTable) Hash() hash.Hash { return t.hash }

var smallFlatMagic = UnmarshalMagic{0xe2, 0xb1}

// Marshal implements the Table interface.
func (t *smallFlatTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	t.initSubTables(ctx.ctx)
	enc.PutRawBytes(smallFlatMagic[:])
	enc.PutHash(t.hash)
	enc.PutGOB(&t.ast)
	enc.PutVarint(int64(len(t.subTables)))
	subBufs := make([]marshal.Encoder, len(t.subTables))
	for i := range t.subTables {
		t.subTables[i].Marshal(ctx, &subBufs[i])
	}
	for i := range t.subTables {
		enc.PutRawBytes(subBufs[i].Bytes())
	}
}

func unmarshalSmallFlatTable(ctx UnmarshalContext, h hash.Hash, dec *marshal.Decoder) Table {
	t := &smallFlatTable{
		hash:  h,
		attrs: TableAttrs{Name: "flat"},
	}
	dec.GOB(&t.ast)
	nSubTables := int(dec.Varint())
	Logf(t.ast, "flattable(S): unmarshal %d subtables", nSubTables)
	for i := 0; i < nSubTables; i++ {
		t.subTables = append(t.subTables, unmarshalTable(ctx, dec))
	}
	return t
}

func (t *smallFlatTable) Attrs(ctx context.Context) TableAttrs { return t.attrs }

// NewFlatTable creates a flatten() table.
func NewFlatTable(ast ASTNode, tables []Table, subshard bool) Table {
	h := hash.Hash{
		0xad, 0xdf, 0x2e, 0x61, 0x48, 0xda, 0xc3, 0x44,
		0x7a, 0x40, 0x19, 0xab, 0x2f, 0x12, 0x50, 0xa3,
		0x37, 0xb7, 0x12, 0x91, 0x8d, 0xf5, 0x71, 0x8b,
		0x97, 0x9c, 0x34, 0xe1, 0xa3, 0xb6, 0xa3, 0x94}
	for _, t := range tables {
		h = h.Merge(t.Hash())
	}
	var t Table
	if subshard {
		t = &smallFlatTable{
			hash:      h,
			ast:       ast,
			srcTables: tables,
			attrs:     TableAttrs{Name: "flatten"},
		}
	} else {
		t = &largeFlatTable{
			hash:      h,
			ast:       ast,
			srcTables: tables,
			attrs:     TableAttrs{Name: "flatten"},
		}
	}
	return t
}

func init() {
	RegisterBuiltinFunc("flatten",
		`
    flatten(tbl0, tbl1, ..., tblN [,subshard:=subshardarg])

Arg types:

- _tbl0_, _tbl1_, ... : table
- _subshardarg_: boolean (default: false)

::tbl | flatten():: (or ::flatten(tbl)::) creates a new table that concatenates the rows of the subtables.
Each table _tbl0_, _tbl1_, .. must be a single-column table where each row is a
another table. Imagine two tables ::table0:: and ::table1:::

table0:

        ║col0 ║ col1║
        ├─────┼─────┤
        │Cat  │ 10  │
        │Dog  │ 20  │

table1:

        ║col0 ║ col1║
        ├─────┼─────┤
        │Bat  │ 3   │
        │Pig  │ 8   │

Then ::flatten(table(table0, table1)):: produces the following table

        ║col0 ║ col1║
        ├─────┼─────┤
        │Cat  │ 10  │
        │Dog  │ 20  │
        │Bat  │ 3   │
        │Pig  │ 8   │

::flatten(tbl0, ..., tblN):: is equivalent to
::flatten(table(flatten(tbl0), ..., flatten(tblN)))::.
That is, it flattens each of _tbl0_, ..., _tblN_, then
concatenates their rows into one table.

Parameter _subshard_ specifies how the flattened table is sharded, when it is used
as an input to distributed ::map:: or ::reduce::. When _subshard_ is false (default),
then ::flatten:: simply shards rows in the input tables (_tbl0_, _tbl1_ ,..., _tblN_). This works
fine if the number of rows in the input tables are much larger than the shard count.

When ::subshard=true::, then flatten will to shard the individual subtables
contained in the input tables (_tbl0_, _tbl1_,...,_tblN_). This mode will work better
when the input tables contain a small number (~1000s) of rows, but each
subtable can be very large. The downside of subsharding is that the flatten
implementation must read all the rows in the input tables beforehand to figure
out their size distribution. So it can be very expensive when input tables
contains many rows.
`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			n := len(args)
			subshard, args := args[n-1].Bool(), args[:n-1]
			tables := make([]Table, len(args)) // a list of list of tables.
			for i, arg := range args {
				tables[i] = arg.Table()
			}
			return NewTable(NewFlatTable(ast, tables, subshard))
		},
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Variadic: true, Types: []ValueType{TableType}},
		FormalArg{Name: symbol.Subshard, Types: []ValueType{BoolType}, DefaultValue: NewBool(false)},
	)

	RegisterTableUnmarshaler(smallFlatMagic, unmarshalSmallFlatTable)
	RegisterTableUnmarshaler(largeFlatMagic, unmarshalLargeFlatTable)
}

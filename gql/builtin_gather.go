package gql

import (
	"context"
	"sync"

	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/gql/termutil"
)

// spreadGatherTable is used to implement spread and gather.
type spreadGatherTable struct {
	hash         hash.Hash
	ast          ASTNode // source-code location
	spread       bool    // true for spread, false for gather
	key, val     symbol.ID
	cols         []symbol.ID // nil for spread
	src          Table
	exactLen     int
	exactLenOnce sync.Once
}

func (t *spreadGatherTable) Len(ctx context.Context, mode CountMode) int {
	if t.spread {
		// Spread does not change the # of rows.
		return t.src.Len(ctx, mode)
	}
	if mode == Exact {
		// Best to scan the entire table.
		t.exactLenOnce.Do(func() {
			t.exactLen = DefaultTableLen(ctx, t)
		})
		return t.exactLen
	}
	// Gather creates new rows in propoportion to the number of columns
	// being gathered. This should also be the exact count, but it's not
	// clear how to handle corner cases such as missing fields for values etc.
	return len(t.cols) * t.src.Len(ctx, mode)
}

func (t *spreadGatherTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	MarshalTableOutline(ctx, enc, t)
}
func (t *spreadGatherTable) Prefetch(ctx context.Context) { /*TODO(saito):implement*/ }
func (t *spreadGatherTable) Hash() hash.Hash              { return t.hash }
func (t *spreadGatherTable) Attrs(ctx context.Context) TableAttrs {
	return TableAttrs{Name: "spreadgather", Path: t.src.Attrs(ctx).Path}
}

func (t *spreadGatherTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	if t.spread {
		buf := termutil.NewBufferPrinter()
		return &spreadTableScanner{
			ctx:    ctx,
			parent: t,
			key:    t.key,
			val:    t.val,
			src:    NewPrefetchingTableScanner(ctx, t.src.Scanner(ctx, start, limit, total), -1),
			// Keep a pointer to the buf used for PrintArgs.Out so as to
			// be able to call .Reset on it.
			buf: buf,
			// Allocate printArgs once here rather than in each call to
			// scan.
			printArgs: PrintArgs{
				Out:  buf,
				Mode: PrintValues,
			},
			symbols: make(map[string]symbol.ID, 32),
		}
	}
	gm := make(map[symbol.ID]bool, len(t.cols))
	for _, k := range t.cols {
		gm[k] = true
	}
	return &gatherTableScanner{
		parent: t,
		key:    t.key,
		val:    t.val,
		gather: gm,
		cols:   t.cols,
		src:    NewPrefetchingTableScanner(ctx, t.src.Scanner(ctx, start, limit, total), -1),
		rows:   make([]Value, len(t.cols)),
		next:   -1,
	}
}

type gatherTableScanner struct {
	parent   *spreadGatherTable
	src      TableScanner
	key, val symbol.ID
	cols     []symbol.ID
	gather   map[symbol.ID]bool
	fields   []StructField
	rows     []Value
	next     int
}

func (sc *gatherTableScanner) Value() Value {
	return sc.rows[sc.next]
}

func (sc *gatherTableScanner) Scan() bool {
	if sc.next >= 0 {
		sc.next++
		if sc.next < len(sc.rows) {
			return true
		}
	}
	if !sc.src.Scan() {
		return false
	}
	row := sc.src.Value().Struct(sc.parent.ast)
	nf := row.Len()
	cnf := nf + 2 - len(sc.gather)
	if cap(sc.fields) < nf {
		sc.fields = make([]StructField, cnf)
	}
	common := sc.fields[0:cnf]
	next := 0
	for i := 0; i < nf; i++ {
		common[next] = row.Field(i)
		if !sc.gather[common[next].Name] {
			next++
		}
	}
	if next != len(common)-2 {
		Panicf(sc.parent.ast, "failed to find the specified key and/or value column in %v", sc.src.Value())
	}
	ki, vi := next, next+1
	for i, g := range sc.cols {
		common[ki] = StructField{
			Name:  sc.key,
			Value: NewString(g.Str()),
		}
		v, ok := row.Value(g)
		if !ok {
			Panicf(sc.parent.ast, "struct does not have field %v: %v", g.Str(), sc.src.Value())
		}
		common[vi] = StructField{
			Name:  sc.val,
			Value: v,
		}
		sc.rows[i] = NewStruct(NewSimpleStruct(common...))
	}
	sc.next = 0
	return true
}

type spreadTableScanner struct {
	ctx       context.Context
	parent    *spreadGatherTable
	src       TableScanner
	key, val  symbol.ID
	row       Value
	buf       *termutil.BufferPrinter
	fields    []StructField
	symbols   map[string]symbol.ID
	printArgs PrintArgs
}

func (sc *spreadTableScanner) Value() Value {
	return sc.row
}

func (sc *spreadTableScanner) Scan() bool {
	if !sc.src.Scan() {
		return false
	}
	row := sc.src.Value().Struct(sc.parent.ast)
	nf := row.Len()
	if cap(sc.fields) < nf {
		sc.fields = make([]StructField, nf-1)
	}
	common := sc.fields[0 : nf-1]
	next := 0
	ki, vi := -1, -1
	for i := 0; i < nf; i++ {
		fl := row.Field(i)
		switch {
		case fl.Name == sc.key:
			ki = i
		case fl.Name == sc.val:
			vi = i
		default:
			// Avoid indexing past the end of the slice if the key or
			// value are not found.
			if next < len(common) {
				common[next] = fl
				next++
			}
		}
	}

	if ki == -1 || vi == -1 {
		Panicf(sc.parent.ast, "one or both of the key/value columns are missing from: %v", sc.src.Value())
	}

	if next != nf-2 {
		Panicf(sc.parent.ast, "too many non key/value columns")
	}

	kv := row.Field(ki)
	vv := row.Field(vi)
	// Print the value of kv to buffer stored in sc.printArgs so that
	// it can be used as a field name.
	sc.buf.Reset()
	kv.Value.printRec(sc.ctx, sc.printArgs, 0)
	colname := sc.buf.String()
	colid, ok := sc.symbols[colname]
	if !ok {
		colid = symbol.Intern(colname)
		sc.symbols[colname] = colid

	}
	common[nf-2] = StructField{
		Name:  colid,
		Value: vv.Value,
	}
	sc.row = NewStruct(NewSimpleStruct(common...))
	return true
}

func builtinSpreadGather(table Table, spread bool, ast ASTNode, key, val symbol.ID, cols []symbol.ID) Value {
	h := sgHashOp(table, key, val, cols)
	sgt := &spreadGatherTable{
		hash:   h,
		ast:    ast,
		src:    table,
		spread: spread,
		key:    key,
		val:    val,
		cols:   cols,
	}
	nt := NewTable(sgt)
	return nt
}

func sgHashOp(table Table, key, val symbol.ID, cols []symbol.ID) hash.Hash {
	h := hash.Hash{
		0x56, 0x17, 0xf7, 0x05, 0x4f, 0xd8, 0x1c, 0x40,
		0xd3, 0x38, 0x4f, 0xe3, 0x0c, 0x3c, 0xb9, 0x46,
		0x4c, 0xa3, 0x11, 0x0d, 0x09, 0x43, 0x80, 0xd2,
		0x67, 0x77, 0xdb, 0x5c, 0x21, 0xad, 0xa8, 0x8c,
	}
	h = h.Merge(table.Hash())
	h = h.Merge(key.Hash())
	h = h.Merge(val.Hash())
	for _, s := range cols {
		h = h.Merge(s.Hash())
	}
	return h
}

func init() {
	RegisterBuiltinFunc("gather",
		`
    tbl | gather(colname..., key:=keycol, value:=valuecol)

Arg types:

- _colname_: string
- _keycol_: string
- _valuecol_: string

Gather collapses multiple columns into key-value pairs, duplicating all other columns as needed. gather is based on the R tidyr::gather() function.

Example: Imagine table t0 with following contents:

        ║col0 ║ col1║ col2║
        ├─────┼─────┤─────┤
        │Cat  │ 30  │ 31  │
        │Dog  │ 40  │ 41  │

::t0 | gather("col1", "col2, key:="name", value:="value"):: will produce the following table:

║ col0║ name║ value║
├─────┼─────┼──────┤
│  Cat│ col1│    30│
│  Cat│ col2│    31│
│  Dog│ col1│    40│
│  Dog│ col2│    41│
		`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			n := len(args)
			key, val := args[n-2], args[n-1]
			gather := make([]symbol.ID, 0, n-3)
			for _, arg := range args[1 : n-2] {
				gather = append(gather, symbol.Intern(arg.Str()))
			}
			return builtinSpreadGather(
				args[0].Table(),
				false,
				ast,
				symbol.Intern(key.Str()),
				symbol.Intern(val.Str()),
				gather)
		},
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},                  // table
		FormalArg{Positional: true, Required: true, Variadic: true, Types: []ValueType{StringType}}, // column names
		FormalArg{Name: symbol.Key, Required: true, Types: []ValueType{StringType}},                 // key colname
		FormalArg{Name: symbol.Value, Required: true, Types: []ValueType{StringType}},               // value colname
	)
}

func init() {
	RegisterBuiltinFunc("spread",
		`
    tbl | spread(keycol, valuecol)

Arg types:

- _keycol_: string
- _valuecol_: string

Spread expands rows across two columns as key-value pairs, duplicating all other columns as needed. spread is based on the R tidyr::spread() function.

Example: Imagine table t0 with following contents:

        ║col0 ║ col1║ col2║
        ├─────┼─────┤─────┤
        │Cat  │ 30  │ 31  │
        │Dog  │ 40  │ 41  │

::t0 | spread("col1", "col2, key:="col1", value:="col2"):: will produce the following table:

║ col0║ 30| 40║
├─────┼───┼─-─┤
│  Cat│ 31│   │
│  Dog│   │ 41│

Note the blank cell values, which may require the use the function to contains to
test for the existence of a field in a row struct in subsequent manipulations.
`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			key, val := args[1], args[2]
			return builtinSpreadGather(
				args[0].Table(),
				true,
				ast,
				symbol.Intern(key.Str()),
				symbol.Intern(val.Str()),
				nil)
		},
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},                                      // table
		FormalArg{Name: symbol.Key, Required: true, Types: []ValueType{StringType}, DefaultValue: NewString("key")},     // key colname
		FormalArg{Name: symbol.Value, Required: true, Types: []ValueType{StringType}, DefaultValue: NewString("value")}, // value colname
	)
}

package gql

import (
	"context"
	"sync"

	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

func (t *reduceTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		return 10000
	}
	t.lenOnce.Do(func() { t.len = DefaultTableLen(ctx, t) })
	return t.len
}

func (t *reduceTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	MarshalTableOutline(ctx, enc, t)
}

type reduceKeyHash struct {
	hash hash.Hash
	key  Value
}

type reduceTable struct {
	hash       hash.Hash
	ast        ASTNode
	srcTable   Table
	keyExpr    *Func
	reduceExpr *Func
	mapExpr    *Func

	once    sync.Once
	rowMap  map[hash.Hash]Value
	rowKeys []reduceKeyHash

	lenOnce sync.Once
	len     int
}

type reduceTableScanner struct {
	parent *reduceTable
	index  int
}

func (sc *reduceTableScanner) Scan() bool {
	sc.index++
	return sc.index < len(sc.parent.rowKeys)
}

func (t *reduceTable) Attrs(ctx context.Context) TableAttrs {
	return TableAttrs{
		Name:    "reduce",
		Columns: []TSVColumn{TSVColumn{Name: "key"}, TSVColumn{Name: "value"}},
	}
}

func (sc *reduceTableScanner) Value() Value {
	k := &sc.parent.rowKeys[sc.index]
	val, ok := sc.parent.rowMap[k.hash]
	if !ok {
		Panicf(sc.parent.ast, "reducetable: key %v not found", *k)
	}
	return NewStruct(NewSimpleStruct(
		StructField{Name: symbol.Key, Value: k.key},
		StructField{Name: symbol.Value, Value: val}))
}

func (t *reduceTable) Hash() hash.Hash              { return t.hash }
func (t *reduceTable) Prefetch(ctx context.Context) { go Recover(func() { t.init(ctx) }) }

func (t *reduceTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	if start > 0 {
		return &NullTableScanner{}
	}
	t.init(ctx)
	return &reduceTableScanner{
		parent: t,
		index:  -1,
	}
}

func (t *reduceTable) init(ctx context.Context) {
	t.once.Do(func() {
		t.rowMap = map[hash.Hash]Value{}
		srcScanner := t.srcTable.Scanner(ctx, 0, 1, 1)
		for srcScanner.Scan() {
			srcRow := srcScanner.Value()
			key := t.keyExpr.Eval(ctx, srcRow)
			keyHash := key.Hash()
			if t.mapExpr != nil {
				srcRow = t.mapExpr.Eval(ctx, srcRow)
			}
			accVal := t.rowMap[keyHash]
			if !accVal.Valid() {
				accVal = Null
				t.rowKeys = append(t.rowKeys, reduceKeyHash{keyHash, key})
				t.rowMap[keyHash] = srcRow
				continue
			}
			t.rowMap[keyHash] = t.reduceExpr.Eval(ctx, accVal, srcRow)
		}
	})
}

func builtinNewReduce(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	srcTable := args[0].Table()
	keyExpr := args[1].Func()
	reduceExpr := args[2].Func()
	mapExpr := args[3].Func()
	shards := int(args[4].Int())
	h := hashNewReduceCall(srcTable, keyExpr, reduceExpr, mapExpr)
	if shards <= 0 {
		t := &reduceTable{
			hash:       h,
			ast:        ast,
			srcTable:   srcTable,
			keyExpr:    keyExpr,
			reduceExpr: reduceExpr,
			mapExpr:    mapExpr,
		}
		return NewTable(t)
	}

	var tableBuf marshal.Encoder
	mctx := newMarshalContext(ctx)
	marshalParallelReduceTable(mctx, &tableBuf, h, ast, srcTable, keyExpr, reduceExpr, mapExpr)
	t := &parallelReduceTable{
		hash:            h,
		ast:             ast,
		src:             srcTable,
		keyExpr:         keyExpr,
		reduceExpr:      reduceExpr,
		mapExpr:         mapExpr,
		nshards:         shards,
		marshalledEnv:   mctx.marshal(),
		marshalledTable: tableBuf.Bytes(),
	}
	return NewTable(t)
}

func hashNewReduceCall(table Table, keyExpr, reduceExpr, mapExpr *Func) hash.Hash {
	h := hash.Hash{
		0xa1, 0xfc, 0xb5, 0xd8, 0xf6, 0x6a, 0xe9, 0xa4,
		0x26, 0x45, 0xff, 0x9f, 0xb8, 0x27, 0xea, 0x3e,
		0xa2, 0xb3, 0x8b, 0x9d, 0x16, 0x2a, 0x6a, 0xb5,
		0x2f, 0x32, 0xdd, 0xf0, 0xb1, 0x15, 0x00, 0x0b}
	h = h.Merge(table.Hash())
	h = h.Merge(keyExpr.Hash())
	h = h.Merge(reduceExpr.Hash())
	if mapExpr != nil {
		h = h.Merge(mapExpr.Hash())
	}
	return h
}

func init() {
	RegisterBuiltinFunc("reduce",
		`
    tbl | reduce(keyexpr, reduceexpr [,map:=mapexpr] [,shards:=nshards])

Arg types:

- _keyexpr_: one-arg function
- _reduceexpr_: two-arg function
- _mapexpr_: one-arg function (default: ::|row|row::)
- _nshards_: int (default: 0)

Reduce groups rows by their _keyexpr_ value. It then invokes _reduceexpr_ for
rows with the same key.

Argument _reduceexpr_ is invoked repeatedly to combine rows or values with the same key.

  - The optional 'map' argument specifies argument to _reduceexpr_.
    The default value (identity function) is virtually never a good function, so
    You should always specify a _mapexpr_ arg.

  - _reduceexpr_must produce a value of the same type as the input args.

  - The _reduceexpr_ must be a commutative expression, since the values are
    passed to _reduceexpr_ in an specified order. If you want to preserve the
    ordering of values in the original table, use the [cogroup](#cogroup)
    function instead.

  - If the source table contains only one row for particular key, the
    _reduceexpr_ is not invoked. The 'value' column of the resulting table will
    the row itself, or the value of the _mapexpr_, if the 'map' arg is set.

If _nshards_ >0, it enables distributed execution.
See the [distributed execution](#distributed-execution) section for more details.

Example: Imagine table ::t0:::

        ║col0 ║ col1║
        ├─────┼─────┤
        │Bat  │  3  │
        │Bat  │  4  │
        │Bat  │  1  │
        │Cat  │  4  │
        │Cat  │  8  │

::t0 | reduce(&col0, |a,b|a+b, map:=&col1):: will create the following table:

        ║key  ║ value║
        ├─────┼──────┤
        │Bat  │ 8    │
        │Cat  │ 12   │

::t0 | reduce(&col0, |a,b|a+b, map:=1):: will count the occurrences of col0 values:

        ║key  ║ value║
        ├─────┼──────┤
        │Bat  │ 3    │
        │Cat  │ 2    │


A slightly silly example, ::t0 | reduce(&col0, |a,b|a+b, map:=&col1*2):: will
produce the following table.

        ║key  ║ value║
        ├─────┼──────┤
        │Bat  │ 16   │
        │Cat  │ 24   │

> Note: ::t0| reduce(t0, &col0, |a,b|a+b.col1):: looks to be the same as
::t0 | reduce(&col0, |a,b|a+b, map:=&col1)::, but the former is an error. The result of the
_reduceexpr_ must be of the same type as the inputs. For this reason, you
should always specify a _mapexpr_.
`,
		builtinNewReduce,
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},              // table
		FormalArg{Positional: true, Required: true, Closure: true, ClosureArgs: anonRowFuncArg}, // keyexpr
		FormalArg{Positional: true, Required: true, Closure: true,
			ClosureArgs: []ClosureFormalArg{{symbol.AnonAcc, symbol.Invalid}, {symbol.AnonVal, symbol.Invalid}}}, // reduceexpr
		FormalArg{Name: symbol.Map, Closure: true, ClosureArgs: anonRowFuncArg, DefaultValue: NewFunc(nil)}, // mapexpr
		FormalArg{Name: symbol.Shards, DefaultValue: NewInt(0)},                                             // shards:=nnn
		FormalArg{Name: symbol.Row, Symbol: true, DefaultSymbol: symbol.AnonRow})
}

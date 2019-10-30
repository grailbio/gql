package gql

import (
	"context"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

var parallelCogroupFunc = bigslice.Func(func(
	marshaledConfig []byte,
	tableHash hash.Hash,
	outBTSVPath string,
	marshaledSrcTable []byte,
	nshards int) (slice bigslice.Slice) {
	ctx := newUnmarshalContext(marshaledConfig)
	ast, srcTable, keyExpr, mapExpr := unmarshalCogroupArgs(ctx, marshaledSrcTable)
	type shardState struct {
		scanner          TableScanner
		nrows            int
		keyExpr, mapExpr *Func
	}

	slice = bigslice.ReaderFunc(nshards,
		func(shard int, scanner **shardState, keys, rows []Value) (n int, err error) {
			if *scanner == nil {
				Logf(ast, "start shard %d/%d for table %+v", shard, nshards, srcTable.Attrs(ctx.ctx))
				*scanner = &shardState{
					scanner: srcTable.Scanner(ctx.ctx, shard, shard+1, nshards),
					keyExpr: keyExpr,
				}
				if mapExpr != nil {
					(*scanner).mapExpr = mapExpr
				}
			}
			if len(keys) != len(rows) {
				Panicf(ast, "keys and values don't match %d %d", len(keys), len(rows))
			}
			for i := range rows {
				if !(*scanner).scanner.Scan() {
					return i, sliceio.EOF
				}
				(*scanner).nrows++
				if (*scanner).nrows%10000000 == 0 {
					Logf(ast, "read %+v: shard %d/%d: %d rows", srcTable.Attrs(ctx.ctx), shard, nshards, (*scanner).nrows)
				}
				row := (*scanner).scanner.Value()
				key := (*scanner).keyExpr.Eval(ctx.ctx, row)
				if (*scanner).mapExpr != nil {
					row = (*scanner).mapExpr.Eval(ctx.ctx, row)
				}
				rows[i] = row
				keys[i] = key
			}
			return len(rows), nil
		})
	slice = bigslice.Cogroup(slice)

	slice = bigslice.Scan(slice, func(shard int, scan *sliceio.Scanner) error {
		Logf(ast, "write shard %d/%d in %v", shard, nshards, outBTSVPath)
		w := NewBTSVShardWriter(ctx.ctx, outBTSVPath, shard, nshards, TableAttrs{})
		var (
			key    Value
			values []Value
		)
		nrows := 0
		for scan.Scan(ctx.ctx, &key, &values) {
			subTableHash := hash.Hash{
				0x6a, 0x59, 0xe5, 0x5a, 0x29, 0x53, 0x9d, 0xdb,
				0x00, 0x65, 0x25, 0x16, 0xb5, 0x43, 0xf5, 0x62,
				0x88, 0x87, 0x63, 0x76, 0x1a, 0xc5, 0xf1, 0xf4,
				0x67, 0x9d, 0xf5, 0x4e, 0x24, 0xa0, 0x43, 0x8c}
			subTableHash = subTableHash.Merge(tableHash).Merge(key.Hash())
			subTable := NewTable(NewSimpleTable(values, subTableHash, TableAttrs{}))
			row := NewStruct(NewSimpleStruct(
				StructField{Name: symbol.Key, Value: key},
				StructField{Name: symbol.Value, Value: subTable}))
			w.Append(row)
		}
		if err := scan.Err(); err != nil {
			Panicf(ast, "scan: %v", err)
		}
		Logf(ast, "wrote %d rows in %d/%d in %v", nrows, shard, nshards, outBTSVPath)
		w.Close(ctx.ctx)
		return nil
	})
	return
})

// parallelCogroupTable implements a table that does filter, then map.
type parallelCogroupTable struct {
	hashOnce sync.Once
	hash     hash.Hash
	ast      ASTNode // source-code location.
	// The table to read from.
	src Table
	// Bindings for keyExpr and redcueExpr
	keyExpr *Func
	mapExpr *Func

	// # of bigslice shards to run.
	nshards int

	marshalledEnv, marshalledTable []byte

	once      sync.Once
	btsvTable Table

	lenOnce sync.Once
	len     int
}

func marshalCogroupArgs(ctx MarshalContext, enc *marshal.Encoder, ast ASTNode, src Table, keyExpr, mapExpr *Func) {
	enc.PutGOB(&ast)
	src.Marshal(ctx, enc)
	keyExpr.Marshal(ctx, enc)
	mapExpr.Marshal(ctx, enc)
}

func (t *parallelCogroupTable) init(ctx context.Context) {
	t.once.Do(func() {
		tableHash := t.Hash()
		cacheName := tableHash.String() + ".btsv"
		btsvPath, found := LookupCache(ctx, cacheName)
		if found {
			Logf(t.ast, "cache hit: %s", btsvPath)
		} else {
			Logf(t.ast, "start bigslice for table %v", btsvPath)
			if _, err := bsSession.Run(ctx, parallelCogroupFunc, t.marshalledEnv, tableHash, btsvPath, t.marshalledTable, t.nshards); err != nil {
				log.Panic(err)
			}
			ActivateCache(ctx, cacheName, btsvPath)
			Logf(t.ast, "finished bigslice for table %v", btsvPath)
		}
		t.btsvTable = NewBTSVTable(btsvPath, t.ast, tableHash)
	})
}

func (t *parallelCogroupTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		return 100000
	}
	t.lenOnce.Do(func() { t.len = DefaultTableLen(ctx, t) })
	return t.len
}

func (t *parallelCogroupTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	t.init(ctx.ctx)
	t.btsvTable.Marshal(ctx, enc)
}

func (t *parallelCogroupTable) Attrs(ctx context.Context) TableAttrs {
	return TableAttrs{Name: "cogroup", Path: t.src.Attrs(ctx).Path}
}

func (t *parallelCogroupTable) Prefetch(ctx context.Context) { t.init(ctx) }
func (t *parallelCogroupTable) Hash() hash.Hash {
	t.hashOnce.Do(func() {
		if t.hash == hash.Zero { // hash != Zero if it is unmarshalled on a remote machine.
			t.hash = hashCogroupCall(t.src, t.keyExpr, t.mapExpr)
		}
	})
	return t.hash
}

func (t *parallelCogroupTable) Scanner(ctx context.Context, start, limit, nshards int) TableScanner {
	t.init(ctx)
	return t.btsvTable.Scanner(ctx, start, limit, nshards)
}

func unmarshalCogroupArgs(ctx UnmarshalContext, data []byte) (ast ASTNode, src Table, keyExpr, mapExpr *Func) {
	dec := marshal.NewDecoder(data)
	dec.GOB(&ast)
	src = unmarshalTable(ctx, dec)
	keyExpr = unmarshalFunc(ctx, dec)
	mapExpr = unmarshalFunc(ctx, dec)
	marshal.ReleaseDecoder(dec)
	return
}

func builtinCogroup(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	srcTable := args[0].Table()
	keyExpr := args[1].Func()
	mapExpr := args[2].Func()
	shards := int(args[3].Int())
	if shards <= 0 {
		Panicf(ast, "cogroup: shards must be >0, but found %d", shards)
	}
	var tableBuf marshal.Encoder
	mctx := newMarshalContext(ctx)
	marshalCogroupArgs(mctx, &tableBuf, ast, srcTable, keyExpr, mapExpr)
	t := &parallelCogroupTable{
		ast:             ast,
		src:             srcTable,
		keyExpr:         keyExpr,
		mapExpr:         mapExpr,
		nshards:         shards,
		marshalledEnv:   mctx.marshal(),
		marshalledTable: tableBuf.Bytes(),
	}
	return NewTable(t)
}

func hashCogroupCall(table Table, keyExpr, mapExpr *Func) hash.Hash {
	h := hash.Hash{
		0xfb, 0xff, 0x03, 0x4e, 0x20, 0x97, 0xf5, 0xd2,
		0x1a, 0xa2, 0xbc, 0xac, 0xe1, 0xc0, 0xfe, 0xae,
		0x07, 0xf2, 0x6f, 0x13, 0x0a, 0xaa, 0x1f, 0xec,
		0xbc, 0xbb, 0x2f, 0x4f, 0x4f, 0xb0, 0x55, 0xaa}
	h = h.Merge(table.Hash())
	h = h.Merge(keyExpr.Hash())
	if mapExpr != nil {
		h = h.Merge(mapExpr.Hash())
	}
	return h
}

func init() {
	RegisterBuiltinFunc("cogroup",
		`
    tbl | cogroup(keyexpr [,mapexpr=mapexpr] [,shards=nshards])

Arg types:

- _keyexpr_: one-arg function
- _mapexpr_: one-arg function (default: ::|row|row::)
- _nshards_: int (default: 1)

Cogroup groups rows by their _keyexpr_ value.  It is the same as Apache Pig's
reduce function. It achieves an effect similar to SQL's "GROUP BY" statement.

Argument _keyexpr_ is any expression that can be computed from row contents. For
each unique key as computed by _keyexpr_, cogroup emits a two-column row of form

    {key: keyvalue, value: rows}

where _keyvalue_ is the value of keyexpr, and _rows_ is a table containing all
the rows in tbl with the given key.

If argument _mapexpr_ is set, the _value_ column of the output will be the
result of applying the _mapexpr_.

Example: Imagine table t0:

        ║col0 ║ col1║
        ├─────┼─────┤
        │Bat  │  3  │
        │Bat  │  1  │
        │Cat  │  4  │
        │Bat  │  4  │
        │Cat  │  8  │

::t0 | cogroup(&col0):: will create the following table:

        ║key  ║ value║
        ├─────┼──────┤
        │Bat  │ tmp1 │
        │Cat  │ tmp2 │

where table tmp1 is as below:

        ║col0 ║ col1║
        ├─────┼─────┤
        │Bat  │  3  │
        │Bat  │  1  │
        │Bat  │  4  │

table tmp2 is as below:

        ║col0 ║ col1║
        ├─────┼─────┤
        │Cat  │  4  │
        │Cat  │  8  │

::t0 | cogroup(&col0, map:=&col1):: will create the following table:

        ║key  ║ value║
        ├─────┼──────┤
        │Bat  │ tmp3 │
        │Cat  │ tmp4 │

Each row in table tmp1 is a scalar, as below

        │  3  │
        │  1  │
        │  4  │

Similarly, table tmp2 looks like below.

        │  4  │
        │  8  │

The cogroup function always uses bigslice for execution.  The _shards_ parameter
defines parallelism. See the "distributed execution" section for more details.
`,
		builtinCogroup,
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},                          // table
		FormalArg{Positional: true, Required: true, Closure: true, ClosureArgs: anonRowFuncArg},             // keyexpr
		FormalArg{Name: symbol.Map, Closure: true, ClosureArgs: anonRowFuncArg, DefaultValue: NewFunc(nil)}, // mapexpr
		FormalArg{Name: symbol.Shards, DefaultValue: NewInt(1)},                                             // shards:=nnn
		FormalArg{Name: symbol.Row, Symbol: true, DefaultSymbol: symbol.AnonRow})
}

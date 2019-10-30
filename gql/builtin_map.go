package gql

import (
	"context"

	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/symbol"
)

func hashMapFilterTable(table Table, filterExpr *Func, mapExprs []*Func) hash.Hash {
	h := hash.Hash{
		0xce, 0xe1, 0x2f, 0xe6, 0x80, 0xff, 0x8f, 0x91,
		0x1f, 0x7f, 0x18, 0x19, 0x7b, 0xb9, 0x5b, 0x72,
		0x13, 0xda, 0xbd, 0xa2, 0x6d, 0x98, 0x9f, 0x87,
		0xd1, 0xe1, 0xfe, 0xd4, 0x9c, 0x9f, 0xa9, 0x22}
	h = h.Merge(table.Hash())
	if filterExpr != nil {
		h = h.Merge(filterExpr.Hash())
	}
	for _, e := range mapExprs {
		h = h.Merge(e.Hash())
	}
	return h
}

func init() {
	RegisterBuiltinFunc("map",
		`
    _tbl | map(expr[, expr, expr, ...] [, filter:=filterexpr] [, shards:=nshards])

Arg types:

- _expr_: one-arg function
- _filterexpr_: one-arg boolean function (default: ::|_|true::)
_ _nshards_: int (default: 0)

Map picks rows that match _filterexpr_ from _tbl_, then applies _expr_ to each
matched row.  If there are multiple _expr_s, the resulting table will apply each
of the expression to every matched row in _tbl_ and combine them in the output.

If _filterexpr_ is omitted, it will match any row.
If _nshards_ > 0, it enables distributed execution.
See the [distributed execution](#distributed-execution) section for more details.

Example: Imagine table ⟪t0⟫ with following contents:

        ║col0 ║ col1║
        ├─────┼─────┤
        │Cat  │ 3   │
        │Dog  │ 8   │

    t0 | map({f0:&col0+&col0, f1:&col1*&col1})

will produce the following table

        ║f0      ║ f1   ║
        ├────────┼──────┤
        │CatCat  │ 9    │
        │DogDog  │ 64   │

The above example is the same as below.

    t0 | map(|r|{f0:r.col0+r.col0, f1:r.col1*r.col1}).


The next example

    t0 | map({f0:&col0+&col0}, {f0:&col0}, filter:=&col1>4)

will produce the following table

        ║f0      ║
        ├────────┤
        │DogDog  │
        │Dog     │

`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			srcTable := args[0].Table()
			n := len(args)
			filter, shards, maps := args[n-3], args[n-2].Int(), args[1:n-3]

			mapExprs := make([]*Func, len(maps))
			for i := range maps {
				mapExprs[i] = maps[i].Value.Func(ast)
			}
			filterExpr := filter.Value.Func(ast)
			return NewMapFilterTable(ctx, ast, srcTable, filterExpr, mapExprs, int(shards))
		},
		func(ast ASTNode, args []AIArg) AIType {
			filter := args[len(args)-3]
			if filter.Expr != nil {
				if exprType := filter.Type.FuncReturnType(ast); !exprType.Is(BoolType) {
					Panicf(ast, "filter '%s' is not bool (%v)", filter.Expr, exprType)
				}
			}
			return AITableType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},
		FormalArg{Positional: true, Required: true, Variadic: true, Closure: true, ClosureArgs: anonRowFuncArg, DefaultValue: NewFunc(nil)}, // map expr
		FormalArg{Name: symbol.Filter, Closure: true, ClosureArgs: anonRowFuncArg, DefaultValue: NewFunc(nil)},                              // filter expr
		FormalArg{Name: symbol.Shards, DefaultValue: NewInt(0)},                                                                             // shards:=NNN
		FormalArg{Name: symbol.Row, Symbol: true, DefaultSymbol: symbol.AnonRow})
}

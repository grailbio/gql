package gql

import (
	"context"

	"github.com/grailbio/gql/symbol"
)

func init() {
	RegisterBuiltinFunc("minn",
		`
    tbl | minn(n, keyexpr [, shards:=nshards])

Arg types:

- _n_: int
- _keyexpr_: one-arg function
- _nshards_: int (default: 0)

Minn picks _n_ rows that stores the _n_ smallest _keyexpr_ values. If _n_<0, minn sorts
the entire input table.  Keys are compared lexicographically.
Note that we also have a
::sort(keyexpr, shards:=nshards):: function that's equivalent to ::minn(-1, keyexpr, shards:=nshards)::

The _nshards_ arg enables distributed execution.
See the [distributed execution](#distributed-execution) section for more details.

Example: Imagine table t0:

        ║col0 ║ col1║ col2║
        ├─────┼─────┼─────┤
        │Bat  │  3  │ abc │
        │Bat  │  4  │ cde │
        │Cat  │  4  │ efg │
        │Cat  │  8  │ ghi │

::minn(t0, 2, -&col1):: will create

        ║col0 ║ col1║ col2║
        ├─────┼─────┼─────┤
        │Cat  │  8  │ ghi │
        │Cat  │  4  │ efg │

::minn(t0, -&col0):: will create


        ║col0 ║ col1║ col2║
        ├─────┼─────┼─────┤
        │Cat  │  4  │ efg │
        │Cat  │  8  │ ghi │

You can sort using multiple keys using {}. For example,
::t0 | minn(10000, {&col0,-&col2}):: will sort two rows first by col0, then by -col2 in case of a
tie.

        ║col0 ║ col1║ col2║
        ├─────┼─────┼─────┤
        │Cat  │  8  │ ghi │
        │Cat  │  4  │ efg │
        │Bat  │  4  │ cde │
        │Bat  │  3  │ abc │

`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			srcTable := args[0].Table()
			minn := args[1].Int()
			keyExpr := args[2].Func()
			shards := int(args[4].Int())
			return NewTable(NewMinNTable(
				ctx, ast, TableAttrs{Name: "minn", Path: srcTable.Attrs(ctx).Path},
				srcTable, keyExpr, minn, shards))
		},
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},              // table
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}},                // n
		FormalArg{Positional: true, Required: true, Closure: true, ClosureArgs: anonRowFuncArg}, // sortkey
		FormalArg{Name: symbol.Row, Symbol: true, DefaultSymbol: symbol.AnonRow},                // row:=varname
		FormalArg{Name: symbol.Shards, DefaultValue: NewInt(0)})                                 // shards:=nnn
}

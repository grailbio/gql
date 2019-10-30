package gql

import (
	"context"

	"github.com/grailbio/gql/symbol"
)

func init() {
	RegisterBuiltinFunc("sort",
		`
    tbl | sort(sortexpr [, shards:=nshards])

::tbl | sort(expr):: is a shorthand for ::tbl | minn(-1, expr)::`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			srcTable := args[0].Table()
			keyExpr := args[1].Func()
			shards := int(args[3].Int())
			return NewTable(NewMinNTable(
				ctx, ast, TableAttrs{Name: "sort", Path: srcTable.Attrs(ctx).Path},
				srcTable, keyExpr, -1, shards))
		},
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},              // table
		FormalArg{Positional: true, Required: true, Closure: true, ClosureArgs: anonRowFuncArg}, // sortkey
		FormalArg{Name: symbol.Row, Symbol: true, DefaultSymbol: symbol.AnonRow},
		FormalArg{Name: symbol.Shards, DefaultValue: NewInt(0)}) // shards:=nnn
}

package gql

import (
	"context"

	"github.com/grailbio/gql/symbol"
)

func init() {
	RegisterBuiltinFunc("filter",
		`
    tbl | filter(expr [,map:=mapexpr] [,shards:=nshards])

Arg types:

- _expr_: one-arg boolean function
- _mapexpr_: one-arg function (default: ::|row|row::)
- _nshards_: int (default: 0)

Functions [map](#map) and filter are actually the same functions, with slightly
different syntaxes.  ::tbl|filter(expr, map=mapexpr):: is the same as
::tbl|map(mapexpr, filter:=expr)::.
`, func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			srcTable := args[0].Table()
			filterExpr := args[1].Func()
			mapExprs := []*Func{}
			if args[2].Func() != nil {
				mapExprs = []*Func{args[2].Func()}
			}
			shards := int(args[3].Int())
			return NewMapFilterTable(ctx, ast, srcTable, filterExpr, mapExprs, shards)
		},
		func(ast ASTNode, args []AIArg) AIType {
			if exprType := args[1].Type.FuncReturnType(ast); !exprType.Is(BoolType) {
				Panicf(ast, "filter '%s' is not bool (%v)", args[1].Expr, args[1].Type)
			}
			return AITableType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},                          // table
		FormalArg{Positional: true, Required: true, Closure: true, ClosureArgs: anonRowFuncArg},             // filter expr.
		FormalArg{Name: symbol.Map, Closure: true, ClosureArgs: anonRowFuncArg, DefaultValue: NewFunc(nil)}, // map:=expr
		FormalArg{Name: symbol.Shards, DefaultValue: NewInt(0)},                                             // shards:=NNN
		FormalArg{Name: symbol.Row, Symbol: true, DefaultSymbol: symbol.AnonRow})
}

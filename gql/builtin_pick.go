package gql

import (
	"context"

	"github.com/grailbio/gql/symbol"
)

func init() {
	RegisterBuiltinFunc("pick",
		`
    tbl | pick(expr)

Arg types:

- _expr_: one-arg boolean function

Pick picks the first row in the table that satisfies _expr_.  If no such row is
found, it returns NA.

Imagine table t0:

        ║col0 ║ col1║
        ├─────┼─────┤
        │Cat  │ 10  │
        │Dog  │ 20  │

::t0 | pick(&col1>=20):: will return {Dog:20}.
::t0 | pick(|row|row.col1>=20):: is the same thing.
`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			table := args[0].Table()
			pickExpr := args[1].Func()
			scanner := table.Scanner(ctx, 0, 1, 1)
			for scanner.Scan() {
				expr := pickExpr.Eval(ctx, scanner.Value())
				if expr.Bool(ast) {
					return scanner.Value()
				}
			}
			return Null
		},
		func(ast ASTNode, args []AIArg) AIType {
			if exprType := args[1].Type.FuncReturnType(ast); !exprType.Is(BoolType) {
				Panicf(ast, "arg#1 '%s' is not bool function (%v)", args[1].Expr, exprType)
			}
			return AIAnyType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}}, // table
		FormalArg{Positional: true, Required: true, Closure: true, ClosureArgs: anonRowFuncArg},
		FormalArg{Name: symbol.Row, Symbol: true, DefaultSymbol: symbol.AnonRow})
}

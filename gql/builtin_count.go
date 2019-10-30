package gql

import "context"

func init() {
	RegisterBuiltinFunc("count",
		`
tbl | count()

Count counts the number of rows in the table.

Example: imagine table t0:

        ║ col1║
        ├─────┤
        │  3  │
        │  4  │
        │  8  │

::t0 | count():: will produce 3.
`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			return NewInt(int64(args[0].Table().Len(ctx, Exact)))
		},
		func(ast ASTNode, args []AIArg) AIType { return AIIntType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}})
}

package gql

import "context"

func init() {
	RegisterBuiltinFunc("force",
		`
    tbl | force()

Force is a performance hint. It is logically a no-op; it just
produces the contents of the source table unchanged.  Underneath, this function
writes the source-table contents in a file. When this expression is evaluated
repeatedly, force will read contents from the file instead of
running _tbl_ repeatedly.
`,

		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			srcTable := args[0].Table()
			return NewTable(materializeTable(ctx, srcTable, nil))
		},
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}})
}

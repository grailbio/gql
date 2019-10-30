package gql

import (
	"context"

	"github.com/grailbio/gql/hash"
)

func newBuiltinTable(name string, rows []Value) Table {
	h := hash.Hash{
		0x95, 0xc0, 0xf1, 0x99, 0xb5, 0x3d, 0x75, 0x8b,
		0xa3, 0xb6, 0x45, 0x5f, 0xdf, 0xbb, 0xaa, 0xcd,
		0x7b, 0x23, 0xd7, 0x8f, 0x7a, 0xe4, 0x7a, 0x0a,
		0x31, 0x42, 0xeb, 0x4d, 0x3c, 0x34, 0x0e, 0x9b}
	h = h.Merge(hashValues(rows))
	return NewSimpleTable(rows, h, TableAttrs{Name: name})
}

func builtinTable(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	// TODO(saito) cache.
	rows := make([]Value, len(args))
	for i, arg := range args {
		rows[i] = arg.Value
	}
	return NewTable(newBuiltinTable("table", rows))
}

func init() {
	RegisterBuiltinFunc("table",
		`
table(expr...)

Arg types:

- _expr_: any

Table creates a new table consisting of the given values.`,
		builtinTable,
		func(ast ASTNode, _ []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Variadic: true, DefaultValue: Null})
}

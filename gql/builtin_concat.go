package gql

import (
	"context"
	"strings"
)

func init() {
	RegisterBuiltinFunc("concat",
		`
    concat(tbl...)

Arg types:

- _tbl_: table

::concat(tbl1, tbl2, ..., tblN):: concatenates the rows of tables _tbl1_, ..., _tblN_
into a new table. Concat differs from flatten in that it attempts to maintain
simple tables simple: that is, tables that are backed by (in-memory) values
are retained as in-memory values; thus concat is designed to build up small(er)
table values, e.g., in a map or reduce operation.

`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			tables := make([]Table, len(args))
			simple := true
			for i, arg := range args {
				tables[i] = arg.Table()
				// TODO: we could condition this on the numebr of rows and spill
				// to cache if the in-memory tables become too large.
				if _, ok := tables[i].(*simpleTable); !ok {
					simple = false
				}
			}
			if simple {
				t := tables[0].(*simpleTable)
				for i := 1; i < len(tables); i++ {
					t = appendSimpleTable(t, tables[i].(*simpleTable).rows...)
				}
				return NewTable(t)
			}
			// Fall back to a flatten table.
			var b strings.Builder
			b.WriteString("concat")
			for _, t := range tables {
				b.WriteString("_")
				b.WriteString(t.Attrs(ctx).Name)
			}
			rows := make([]Value, len(args))
			for i := range rows {
				rows[i] = args[i].Value
			}
			return NewTable(NewFlatTable(ast, []Table{newBuiltinTable(b.String(), rows)}, false))
		},
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Variadic: true, Types: []ValueType{TableType}},
	)
}

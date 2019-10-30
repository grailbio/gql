package gql

import (
	"context"

	"github.com/grailbio/gql/symbol"
)

func init() {
	RegisterBuiltinFunc("table_attrs",
		`
   tbl |table_attrs()

Example:

     t := read("foo.tsv")
     table_attrs(t).path  (=="foo.tsv")

Table_attrs returns table attributes as a struct with three fields:

 - Field 'type' is the table type, e.g., "tsv", "mapfilter"
 - Field 'name' is the name of the table. It is some random string.
 - Field 'path' is name of the file the table is read from.
   "path" is nonempty only for tables created directly by read(),
   tables that are result of applying map or filter to table created by read().`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			table := args[0].Table()
			attrs := table.Attrs(ctx)
			return NewStruct(NewSimpleStruct(
				StructField{symbol.Name, NewString(attrs.Name)},
				StructField{symbol.Path, NewString(attrs.Path)}))
		},
		func(ast ASTNode, _ []AIArg) AIType { return AIStructType },
		FormalArg{Positional: true, Required: true})
}

package gql

import (
	"context"

	"github.com/grailbio/gql/symbol"
)

func init() {
	RegisterBuiltinFunc("optionalfield",
		`Usage: optional_field(struct, field [, default:=defaultvalue])

This function acts like struct.field. However, if the field is missing, it
returns the defaultvalue. If defaultvalue is omitted, it returns NA.

Example:

  optionalfield({a:10,b:11}, a) == 10
  optionalfield({a:10,b:11}, c, default:12) == 12
  optionalfield({a:10,b:11}, c) == NA
`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			st := args[0].Struct()
			fieldID := args[1].Symbol
			if val, ok := st.Value(fieldID); ok {
				return val
			}
			return args[2].Value
		},
		func(ast ASTNode, args []AIArg) AIType { return AIAnyType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{StructType}}, // table
		FormalArg{Positional: true, Required: true, Symbol: true},
		FormalArg{Name: symbol.Default, DefaultValue: Null},
	)
}

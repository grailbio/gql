package gql

import (
	"context"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/symbol"
)

func init() {
	RegisterBuiltinFunc("write",
		`Usage: write(table, "path" [,shards:=nnn] [,type:="format"])

Write table contents to a file. The optional argument "type" specifies the file
format. The value should be either "tsv", "btsv", or "bed".  If type argument is
omitted, the file format is auto-detected from the extension of the "path" -
".tsv" for the TSV format, ".btsv" for the BTSV format, ".bed" for the BED format.

- When writing a btsv file, the write function accepts the "shards"
  parameter. It sets the number of rangeshards. For example,

    read("foo.tsv") | write("bar.btsv", shards:=64)

  will create a 64way-sharded bar.btsv that has the same content as
  foo.tsv. bar.btsv is actually a directory, and shard files are created
  underneath the directory.

.`, func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			table := args[0].Table()
			path := args[1].Str()
			nShard := int(args[2].Int())
			var fh FileHandler
			if t := args[3].Str(); t != "" {
				fh = GetFileHandlerByName(t)
			} else {
				fh = GetFileHandlerByPath(path)
			}
			log.Printf("write %v (%v): started", path, fh)
			fh.Write(ctx, path, ast, table, nShard, overwriteFiles)
			log.Printf("write %v (%v): finished", path, fh)
			return True
		},
		func(ast ASTNode, args []AIArg) AIType { return AIBoolType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},                // table
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},               // path
		FormalArg{Name: symbol.Shards, Types: []ValueType{IntType}, DefaultValue: NewInt(1)},      // shards:=nnn
		FormalArg{Name: symbol.Type, Types: []ValueType{StringType}, DefaultValue: NewString("")}, // type:="btsv"
	)
}

func init() {
	RegisterBuiltinFunc("writecols",
		`Usage: writecols(table, "path-template", [gzip:=false])

Write table contents to a set of files, each containing a single column of the
table. The naming of the files is determined using a templating (golang's
text/template). For example, a template of the form:

cols-{{.Name}}-{{.Number}}.ctsv

will have .Name replaced with name of the column and .Number with the index
of the column. So for a table with two columns 'A' and 'B', the files
will cols-A-0.ctsv and cols-B-1.cstv.

Files may be optionally gzip compressed if the gzip named parameter is specified
as true.
.`, func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			table := args[0].Table()
			template := args[1].Str()
			gzipFiles := args[2].Bool()
			WriteColumnarTSV(ctx, table, template, gzipFiles, overwriteFiles)
			return True
		},
		func(ast ASTNode, args []AIArg) AIType { return AIBoolType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},               // table
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},              // template
		FormalArg{Name: symbol.GZIP, Types: []ValueType{BoolType}, DefaultValue: False},          // gzip:=true
	)
}

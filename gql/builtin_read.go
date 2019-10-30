package gql

import (
	"context"

	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/symbol"
)

// NewTableFromFileWithHash creates a Table object that reads from the given
// file.  If the file type is not specified, this function attempts to derive it
// from the path name.  The optional arg hash is the hash of the table. If it is
// hash.Zero, it is auto-computed from the pathname and the file attributes.
func newTableFromFileWithHash(ctx context.Context, path string, ast ASTNode, fileHandler FileHandler, hash hash.Hash) Table {
	if fileHandler == nil {
		if fileHandler = GetFileHandlerByPath(path); fileHandler == nil {
			Panicf(ast, "open %s: unknown file type", path)
		}
	}
	return fileHandler.Open(ctx, path, ast, hash)
}

// NewTableFromFile creates a Table object that reads from the given
// file.  If the file type is not specified, this function attempts to derive it
// from the path name. The hash of the table is computed from the pathname and
// the file's attributes.
func NewTableFromFile(ctx context.Context, path string, ast ASTNode, fh FileHandler) Table {
	return newTableFromFileWithHash(ctx, path, ast, fh, hash.Zero)
}

func init() {
	RegisterBuiltinFunc("read",
		`Usage:

    read(path [, type:=filetype])

Arg types:

- _path_: string
- _filetype_: string


Read table contents to a file. The optional argument 'type' specifies the file format.
If the type is unspecified, the file format is auto-detected from the file extension.

- Extension ".tsv" or ".bed" loads a tsv file.

- Extension ".prio" loads a fragment file.

- Extension ".btsv" loads a btsv file.

- Extension ".bam" loads a BAM file.

- Extension ".pam" loads a PAM file.


If the type is specified, it must be one of the following strings: "tsv", "bed",
"btsv", "fragment", "bam", "pam". The type arg overrides file-type autodetection
based on path extension.

Example:
  read("blahblah", type:=tsv)
.`, func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			path := args[0].Str()
			var fh FileHandler
			if t := args[1].Str(); t != "" {
				fh = GetFileHandlerByName(t)
			}
			return NewTable(NewTableFromFile(ctx, path, ast, fh))
		},
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
		FormalArg{Name: symbol.Type, Types: []ValueType{StringType}, DefaultValue: NewString("")},
	)
}

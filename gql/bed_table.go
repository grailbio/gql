package gql

import (
	"context"

	"github.com/grailbio/base/file"
	"github.com/grailbio/gql/hash"
)

// BEDFileHandler is a FileHandler implementation for BED files.  BED is just a
// TSV file with no header line, but instead with a hard-coded set of columns.
type bedFileHandler struct{}
var singletonBEDFileHandler = &bedFileHandler{}

// Name implements FileHandler.
func (*bedFileHandler) Name() string { return "bed" }

// Open implements FileHandler.
func (fh *bedFileHandler) Open(ctx context.Context, path string, ast ASTNode, hash hash.Hash) Table {
	return NewTSVTable(path, ast, hash, fh, &TSVFormat{
		HeaderLines: 0,
		Columns: []TSVColumn{
			{Name: "chrom", Type: StringType},
			{Name: "start", Type: IntType},
			{Name: "end", Type: IntType},
			{Name: "featname", Type: StringType},
		}})
}

// Write implements FileHandler.
func (*bedFileHandler) Write(ctx context.Context, path string, ast ASTNode, table Table, nShard int, overwrite bool) {
	if _, err := file.Stat(ctx, path); err == nil {
		if !overwrite {
			Logf(ast, "write %v: file already exists and --overwrite-files=false.", path)
			return
		}
		file.RemoveAll(ctx, path)
	}
	WriteTSV(ctx, path, table, false, false)
}

func init() {
	RegisterFileHandler(singletonBEDFileHandler, `\.bed$`, `\.bed\.count$`)
}

package gqlutil

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"

	"github.com/grailbio/base/fileio"
	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/hash"
)

func randomHash() (h hash.Hash) {
	n, err := rand.Read(h[:])
	if err != nil {
		log.Panic(err)
	}
	if n != len(h) {
		log.Panicf("randomHash: read %dB, requested %dB", n, len(h))
	}
	return h
}

// NewInMemoryTableFromMemory creates an instance of an in-memory
// gql.NewSimpleTableFromMemory using the data provided via the picker
// function to populate the table. It is only suited for data sets that
// are small enough to fit in memory twice, once for the original data set
// and a second copy for the table created here.
func NewInMemoryTableFromMemory(name string, nRows int, picker func(row int) gql.Value) gql.Table {
	rows := make([]gql.Value, 0, nRows)
	for i := 0; i < nRows; i++ {
		v := picker(i)
		if v.Type() == gql.InvalidType {
			continue
		}
		rows = append(rows, v)
	}
	return gql.NewSimpleTable(rows, randomHash(), gql.TableAttrs{Name: name})
}

// NewDiskTableFromMemory is like NewInMemoryTableFromMemory except that it
// writes the table create to disk rather than keeping it in memory.
func NewDiskTableFromMemory(ctx context.Context, name string, nRows int, picker func(row int) gql.Value) (gql.Table, func() error) {
	filename := gql.GenerateStableCachePath(name + ".btsv")
	w := gql.NewBTSVShardWriter(ctx, filename, 0, 1, gql.TableAttrs{Name: name})
	for i := 0; i < nRows; i++ {
		v := picker(i)
		if v.Type() == gql.InvalidType {
			continue
		}
		w.Append(v)
	}
	w.Close(ctx)
	// TODO(saito) randomHash isn't exactly right; use the hash(pathname + modtime
	// + size).
	return gql.NewBTSVTable(filename, &gql.ASTUnknown{}, randomHash()), func() error {
		ft := fileio.DetermineAPI(filename)
		switch ft {
		case fileio.LocalAPI:
			return os.Remove(filename)
		}
		// TODO(cnicolaou): remove s3 files.
		return fmt.Errorf("unsupported file type: %v", ft)
	}
}

// TableType represents the types of table that can be created by
// CreateAndRegisterTable
type TableType int

const (
	// BTSVDiskTable represents a btsv (binary tsv) on disk table.
	BTSVDiskTable TableType = iota
	// SimpleRAMTable represents a simple implementation of an in-RAM table.
	SimpleRAMTable
)

// CreateAndRegisterTable will create a new table of the specified type from
// the data returned by the picker function and register that table in the
// supplied session as a global variable.
func CreateAndRegisterTable(ctx context.Context, tableType TableType, sess *gql.Session, name string, nRows int, picker func(row int) gql.Value) func() error {
	var table gql.Table
	var cleanup func() error
	switch tableType {
	case BTSVDiskTable:
		table, cleanup = NewDiskTableFromMemory(ctx, name, nRows, picker)
	default:
		table = NewInMemoryTableFromMemory(name, nRows, picker)
		cleanup = func() error { return nil }
	}
	sess.SetGlobal(name, gql.NewTable(table))
	return cleanup
}

// RowAsValue returns the supplied row represented as slice of StructFields
// into a gql.Value.
func RowAsValue(row []gql.StructField) gql.Value {
	return gql.NewStruct(gql.NewSimpleStruct(row...))
}

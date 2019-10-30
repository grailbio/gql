package gql

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
)

var parallelMapFunc = bigslice.Func(func(
	marshaledEnv []byte,
	hash hash.Hash,
	outBTSVPath string,
	marshaledTable []byte,
	nshards int) (slice bigslice.Slice) {
	ctx := newUnmarshalContext(marshaledEnv)
	ast := astUnknown // TODO(saito) compute proper position.
	slice = bigslice.ReaderFunc(nshards,
		func(shard int, scanner *TableScanner, rows []Value) (n int, err error) {
			if *scanner == nil {
				table := unmarshalTable(ctx, marshal.NewDecoder(marshaledTable))
				Logf(ast, "start shard %d/%d", shard, nshards)
				*scanner = table.Scanner(ctx.ctx, shard, shard+1, nshards)
			}
			for i := range rows {
				if !(*scanner).Scan() {
					return i, sliceio.EOF
				}
				v := (*scanner).Value()
				if v.Type() == TableType {
					// If the value is a subtable, force it to the storage. This improves
					// performance in a common usage where parallel map is given a list of
					// filenames, and the map function reads each file and runs an
					// expersive computation.  materializeTable causes the file-reading to
					// run in parallel. Without materializeTable, the reading will be
					// delayed until the downstream expression starts reading the result
					// of the parallel map. If the downstream reader runs serially, the
					// file reading will also happen serially, which isn't what users
					// want.
					v = NewTable(materializeTable(ctx.ctx, v.Table(ast), nil))
				}
				rows[i] = v
			}
			return len(rows), nil
		},
	)
	slice = bigslice.Scan(slice, func(shard int, scan *sliceio.Scanner) error {
		Logf(ast, "write shard %d/%d in %v", shard, nshards, outBTSVPath)
		w := NewBTSVShardWriter(ctx.ctx, outBTSVPath, shard, nshards, TableAttrs{})
		var v Value
		nrows := 0
		for scan.Scan(ctx.ctx, &v) {
			w.Append(v)
			nrows++
		}
		if err := scan.Err(); err != nil {
			Panicf(ast, "%s: %v", outBTSVPath, err)
		}
		Logf(ast, "%s: wrote %d rows in shard %d/%d", outBTSVPath, nrows, shard, nshards)
		w.Close(ctx.ctx)
		return nil
	})
	return
})

// mapFilterTable implements a table that does filter, then map.
type parallelMapFilterTable struct {
	hash hash.Hash
	ast  ASTNode // source-code location.
	// The table to read from.
	src Table
	// filter function, if nonnil, is invoked on each input row.
	// Only the rows that evaluates true will be passed to the mapper.
	filterExpr *Func
	// mapExpr, if nonnil, specifies the transformation of an input row to the
	// output row.  If nil, the input row is yielded as is.
	mapExprs []*Func
	// # of bigslice shards to run.
	nshards int
	once    sync.Once

	marshalledEnv, marshalledTable []byte

	btsvTable Table

	exactLenOnce sync.Once
	exactLen     int
}

func (t *parallelMapFilterTable) init(ctx context.Context) {
	t.once.Do(func() {
		cacheName := t.hash.String() + ".btsv"
		btsvPath, found := LookupCache(ctx, cacheName)
		if found {
			Logf(t.ast, "cache hit: %s", btsvPath)
		} else {
			Logf(t.ast, "start parallel mapreduce, shards=%d", t.nshards)
			if _, err := bsSession.Run(ctx, parallelMapFunc, t.marshalledEnv, t.hash, btsvPath, t.marshalledTable, t.nshards); err != nil {
				log.Panic(err)
			}
			ActivateCache(ctx, cacheName, btsvPath)
		}
		t.btsvTable = NewBTSVTable(btsvPath, t.ast, t.hash)
	})
}

// Len implements Table interface
func (t *parallelMapFilterTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		return t.src.Len(ctx, Approx)
	}
	t.exactLenOnce.Do(func() {
		t.exactLen = DefaultTableLen(ctx, t)
	})
	return t.exactLen
}

// Marshal implements Table interface
func (t *parallelMapFilterTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	marshalMapFilterTable(ctx, enc, t.hash, t.ast, t.src, t.filterExpr, t.mapExprs)
}

// Attrs implements Table interface
func (t *parallelMapFilterTable) Attrs(ctx context.Context) TableAttrs {
	srcAttrs := t.src.Attrs(ctx)
	mapDesc := strings.Builder{}
	mapDesc.WriteByte('(')
	for i, e := range t.mapExprs {
		if i > 0 {
			mapDesc.WriteByte(',')
		}
		mapDesc.WriteString(e.String())
	}
	mapDesc.WriteByte(')')
	return TableAttrs{
		Name: "parallelmap",
		Path: srcAttrs.Path,
		Description: fmt.Sprintf("src:=%s, filter:=%v, map:=%s",
			srcAttrs.Description,
			t.filterExpr,
			mapDesc.String()),
	}
}

// Prefetch implements Table interface
func (t *parallelMapFilterTable) Prefetch(ctx context.Context) {}

// Hash implements Table interface
func (t *parallelMapFilterTable) Hash() hash.Hash { return t.hash }

// Scanner implements Table interface
func (t *parallelMapFilterTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	t.init(ctx)
	return t.btsvTable.Scanner(ctx, start, limit, total)
}

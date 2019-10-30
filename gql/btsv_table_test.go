package gql_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/gqltest"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/symbol"
)

func TestBTSVShardedReader(t *testing.T) {
	t.Parallel()
	const nRows = 1000000
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()
	tmpPath := filepath.Join(tmpDir, "shardedbtsv.btsv")

	doScan := func(r gql.Table, increment, nshard int) {
		start := 0
		want := int64(100)
		for start < nshard {
			limit := start + increment
			if limit > nshard {
				limit = nshard
			}
			sc := r.Scanner(ctx, start, limit, nshard)
			for sc.Scan() {
				require.Equal(t, want, sc.Value().Int(nil))
				want++
			}
			start = limit
		}
		require.Equal(t, want, int64(nRows+100))
	}

	createBTSVShard := func(shard, nshard int, startRow, limitRow int) {
		w := gql.NewBTSVShardWriter(ctx, tmpPath, shard, nshard, gql.TableAttrs{})
		for i := startRow; i < limitRow; i++ {
			w.Append(gql.NewInt(int64(i) + 100))
		}
		w.Close(ctx)
	}

	_ = gqltest.NewSession()
	createBTSVShard(0, 3, 0, nRows/3)
	createBTSVShard(1, 3, nRows/3, nRows*2/3)
	createBTSVShard(2, 3, nRows*2/3, nRows)
	r := gql.NewBTSVTable(tmpPath, &gql.ASTUnknown{}, hash.Zero)
	doScan(r, 1, 1)
	doScan(r, 3, 500)
}

func TestShardedBTSVToTSVConversion(t *testing.T) {
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	btsvPath := filepath.Join(tmpDir, "test.btsv")
	ctx := context.Background()
	col0 := symbol.Intern("col0")
	col1 := symbol.Intern("col1")

	_ = gqltest.NewSession()
	w := gql.NewBTSVShardWriter(ctx, btsvPath, 0, 2, gql.TableAttrs{})
	for i := 0; i < 5; i++ {
		w.Append(gql.NewStruct(gql.NewSimpleStruct(
			gql.StructField{Name: col0, Value: gql.NewInt(int64(i + 100))})))
	}
	w.Close(ctx)

	w = gql.NewBTSVShardWriter(ctx, btsvPath, 1, 2, gql.TableAttrs{})
	for i := 0; i < 5; i++ {
		w.Append(gql.NewStruct(gql.NewSimpleStruct(
			gql.StructField{Name: col1, Value: gql.NewInt(int64(i + 200))},
		)))
	}
	w.Close(ctx)

	btsv := gql.NewBTSVTable(btsvPath, &gql.ASTUnknown{}, hash.Zero)
	tsvPath := filepath.Join(tmpDir, "test.tsv")
	gql.TSVFileHandler().Write(ctx, tsvPath, &gql.ASTUnknown{}, btsv, 1, false)

	tsv := gql.NewTSVTable(tsvPath, &gql.ASTUnknown{}, hash.Zero, nil, nil)
	sc := tsv.Scanner(ctx, 0, 1, 1)
	for i := 0; i < 5; i++ {
		assert.True(t, sc.Scan())
		assert.Equal(t, fmt.Sprintf("{col0:%d,col1:NA}", i+100), sc.Value().String())
	}
	for i := 0; i < 5; i++ {
		assert.True(t, sc.Scan())
		assert.Equal(t, fmt.Sprintf("{col0:NA,col1:%d}", i+200), sc.Value().String())
	}
	assert.False(t, sc.Scan())
}

func TestBTSVShardedScannerForSmallTable(t *testing.T) {
	ctx := context.Background()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	btsvPath := filepath.Join(tmpDir, "test.btsv")
	_ = gqltest.NewSession()
	value := func(row int) int64 {
		return int64(row + 100)
	}

	const totalRow = 1500
	row := 0
	{
		w := gql.NewBTSVShardWriter(ctx, btsvPath, 0, 2, gql.TableAttrs{})
		for row < 1000 {
			w.Append(gql.NewInt(value(row)))
			row++
		}
		w.Close(ctx)
	}
	{
		w := gql.NewBTSVShardWriter(ctx, btsvPath, 1, 2, gql.TableAttrs{})
		for row < totalRow {
			w.Append(gql.NewInt(value(row)))
			row++
		}
		w.Close(ctx)
	}

	tbl := gql.NewTableFromFile(ctx, btsvPath, &gql.ASTUnknown{}, nil)
	for start := 0; start < totalRow; start += 10 {
		sc := tbl.Scanner(ctx, start, start+10, totalRow)
		for i := start; i < start+10; i++ {
			require.True(t, sc.Scan())
			val := sc.Value().Int(nil)
			require.Equalf(t, val, value(i), "start=%d", start)
		}
		require.False(t, sc.Scan())
	}
}

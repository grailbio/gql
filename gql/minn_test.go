package gql_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/gqltest"
	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/assert"
)

func testMinNSmall(t *testing.T, shards int) {
	path := "./testdata/cancer_genes_cos_positive_sorted.bed"

	env := gqltest.NewSession()
	table := gqltest.Eval(t, fmt.Sprintf("read(`%s`) | minn(3, {$chrom,$start,$featname}, shards:=%d)", path, shards), env)
	assert.Equal(t,
		[]string{
			"{chrom:chr1,start:2487804,end:2495267,featname:uc001ajr.3}",
			"{chrom:chr1,start:2487804,end:2495188,featname:uc001ajt.1}",
			"{chrom:chr1,start:2487804,end:2492974,featname:uc009vlf.1}",
		},
		gqltest.ReadTable(table))
	table = gqltest.Eval(t, fmt.Sprintf("minn(read(`%s`), 3, {-$chrom,-$start,-$featname},shards:=%d)", path, shards), env)
	assert.Equal(t,
		[]string{
			"{chrom:chrX,start:133507341,end:133562822,featname:uc011mvk.2}",
			"{chrom:chrX,start:133507341,end:133549321,featname:uc010nrr.3}",
			"{chrom:chrX,start:133507341,end:133562822,featname:uc004exk.3}",
		},
		gqltest.ReadTable(table))
}

func testMinNNested(t *testing.T, shards int) {
	path := "./testdata/cancer_genes_cos_positive_sorted.bed"
	env := gqltest.NewSession()
	table := gqltest.Eval(t, fmt.Sprintf("read(`%s`) | minn(3, {$chrom,$start,$featname}, shards:=%d) | minn(2,$end, shards:=%d)", path, shards, shards), env)
	assert.Equal(t,
		[]string{
			"{chrom:chr1,start:2487804,end:2492974,featname:uc009vlf.1}",
			"{chrom:chr1,start:2487804,end:2495188,featname:uc001ajt.1}",
		},
		gqltest.ReadTable(table))
}

func TestMinNSmall(t *testing.T)         { testMinNSmall(t, 0) }
func TestParallelMinNSmall(t *testing.T) { testMinNSmall(t, 1) }

func TestMinNNested(t *testing.T)         { testMinNNested(t, 0) }
func TestParallelMinNNested(t *testing.T) { testMinNNested(t, 1) }

func testMinNLarge(t *testing.T, shards int) {
	t.Parallel()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	tmpPath := filepath.Join(tmpDir, "minn.btsv")

	const n = 1000000
	vals := make([]int, n)
	w := gql.NewBTSVShardWriter(context.Background(), tmpPath, 0, 1, gql.TableAttrs{})
	for i := 0; i < n; i++ {
		vals[i] = rand.Int()
		w.Append(gql.NewInt(int64(vals[i])))
	}
	w.Close(context.Background())
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })

	env := gqltest.NewSession()
	gql.MinNMaxRowsPerShard = n / 5
	gql.MinNMinRowsPerShard = n / 20
	table := gqltest.Eval(t, fmt.Sprintf("minn(read(`%s`), 4, _, shards:=%d)", tmpPath, shards), env)
	assert.Equal(t,
		[]string{
			fmt.Sprintf("%d", vals[0]),
			fmt.Sprintf("%d", vals[1]),
			fmt.Sprintf("%d", vals[2]),
			fmt.Sprintf("%d", vals[3]),
		},
		gqltest.ReadTable(table))

	table = gqltest.Eval(t, fmt.Sprintf("minn(read(`%s`), 4, -_, shards:=%d)", tmpPath, shards), env)
	assert.Equal(t,
		[]string{
			fmt.Sprintf("%d", vals[n-1]),
			fmt.Sprintf("%d", vals[n-2]),
			fmt.Sprintf("%d", vals[n-3]),
			fmt.Sprintf("%d", vals[n-4]),
		},
		gqltest.ReadTable(table))
}

func TestMinNLarge(t *testing.T)         { testMinNLarge(t, 0) }
func TestParallelMinNLarge(t *testing.T) { testMinNLarge(t, 2) }

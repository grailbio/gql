package gql_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/grailbio/base/log"
	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/gqltest"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/symbol"
)

func doSmallReduceTest(t *testing.T, parallel bool) {
	env := gqltest.NewSession()
	doEval := func(expr string) gql.Value {
		if parallel {
			re := regexp.MustCompile(`\)$`)
			expr = re.ReplaceAllString(expr, ", shards:=1)")
			t.Logf("Eval %s", expr)
		}
		return gqltest.Eval(t, expr, env)
	}

	gqltest.Eval(t, `T0 := table(
{i:0, s:1},
{i:3, s:2},
{i:0, s:3},
{i:3, s:4},
{i:1, s:5})`, env)
	assert.Equal(t,
		[]string{
			"{key:0,value:4}",
			"{key:1,value:5}",
			"{key:3,value:6}",
		},
		gqltest.ReadTableSorted(doEval(`reduce(T0, $i, _acc + _val, map:=$s)`)))
	assert.Equal(t,
		[]string{
			"{key:0,value:2}",
			"{key:1,value:1}",
			"{key:3,value:2}",
		},
		gqltest.ReadTableSorted(doEval(`reduce(T0, $i, _acc + _val, map:=1)`)))
}

func TestSmallReduce(t *testing.T) {
	doSmallReduceTest(t, false)
}

func TestSmallParallelReduce(t *testing.T) {
	doSmallReduceTest(t, true)
}

func doLargeReduceTest(t *testing.T, nRow, randSeed int) {
	r := rand.New(rand.NewSource(int64(randSeed)))
	env := gqltest.NewSession()
	ctx := context.Background()

	rows := []gql.Value{}
	colK := symbol.Intern("k")
	colV := symbol.Intern("v")

	model := map[int64]int64{}
	for i := 0; i < nRow; i++ {
		key := int64(r.ExpFloat64() * float64(nRow))
		val := r.Int63n(int64(nRow))
		if v, ok := model[key]; ok {
			log.Debug.Printf("key=%d val=%d,%d", key, v, val)
		}
		model[key] += val
		rows = append(rows, gql.NewStruct(gql.NewSimpleStruct(
			gql.StructField{colK, gql.NewInt(key)},
			gql.StructField{colV, gql.NewInt(val)})))
	}
	name := fmt.Sprintf("testreduce%d", randSeed)
	tbl := gql.NewSimpleTable(rows, hash.String(name), gql.TableAttrs{Name: name})

	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	path := filepath.Join(tmpDir, "test.tsv")
	gql.TSVFileHandler().Write(ctx, path, &gql.ASTUnknown{}, tbl, 1, false)

	actual := gqltest.Eval(t, fmt.Sprintf(
		"read(`%s`) | reduce($k, _acc + _val, map:=$v, shards:=2)", path), env).Table(nil)
	assert.Equal(t, len(model), actual.Len(ctx, gql.Exact))
	sc := actual.Scanner(ctx, 0, 1, 1)
	keyCol := symbol.Intern("key")
	valueCol := symbol.Intern("value")
	var n int
	for sc.Scan() {
		key := gql.MustStructValue(sc.Value().Struct(nil), keyCol).Int(nil)
		val := gql.MustStructValue(sc.Value().Struct(nil), valueCol).Int(nil)
		assert.Equalf(t, model[key], val, "key=%d", key)
		n++
	}
	assert.Equal(t, n, len(model))
}

func TestLargeParallelReduce0(t *testing.T) {
	doLargeReduceTest(t, 30, 0)
}

func TestLargeParallelReduce1(t *testing.T) {
	doLargeReduceTest(t, 2000, 1)
}

func TestLargeParallelReduce2(t *testing.T) {
	doLargeReduceTest(t, 10000, 2)
}

func TestReduceWithVariablesInInnerScope(t *testing.T) {
	env := gqltest.NewSession()
	path := "./testdata/data.tsv"

	tbl := gqltest.Eval(t, fmt.Sprintf(`
test_table := table(
  {cola:1, colb:read("%s")},
  {cola:2, colb:read("%s")});
locs := map(test_table, map(yh.colb, {$A, $B, cola: yh.cola}), row:=yh);
flatten(locs, subshard:=true) |
reduce({$A,$B}, float(_acc) + float(_val), map:=$A, shards:=1);`, path, path), env)
	assert.Equal(t,
		[]string{
			"{key:{A:1,B:/a},value:2}",
			"{key:{A:2,B:s3://a},value:4}",
			"{key:{A:NA,B:s3://a},value:0}"},
		gqltest.ReadTable(tbl))
}

func TestReduceDeadlock(t *testing.T) {
	env := gqltest.NewSession()
	tbl := gqltest.Eval(t, "out := table({x:1}); y := out | map({$x}, shards:=1) | sort($x)", env)
	assert.Equal(t,
		[]string{"{x:1}"},
		gqltest.ReadTable(tbl))
}

func doBenchmarkReduce(b *testing.B, expr string, env *gql.Session) {
	gqltest.Eval(b, expr, env)
}

func BenchmarkReduce(b *testing.B) {
	env := gqltest.NewSession()
	path := testutil.GetFilePath("//go/src/grail.com/bio/fragments/testdata/medium.prio")
	gqltest.Eval(b, fmt.Sprintf(`t := read("%s")`, path), env)

	for i := 0; i < b.N; i++ {
		doBenchmarkReduce(b, `flatten(table(t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t)) | reduce({$reference, $start}, _acc+_val, map:=1) | pick(false)`, env)
	}
}

func BenchmarkReduceParallel(b *testing.B) {
	env := gqltest.NewSession()
	path := testutil.GetFilePath("//go/src/grail.com/bio/fragments/testdata/medium.prio")
	gqltest.Eval(b, fmt.Sprintf(`t := read("%s")`, path), env)
	for i := 0; i < b.N; i++ {
		doBenchmarkReduce(b, `flatten(table(t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t)) | reduce({$reference, $start}, _acc+_val, map:=1, shards:=4) | pick(false)`, env)
	}
}

package gql_test

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/testutil/h"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/gqltest"
)

func evalCogroup(t *testing.T, expr string, parallel bool, env *gql.Session) []string {
	if parallel {
		re := regexp.MustCompile(`\)$`)
		expr = re.ReplaceAllString(expr, ", shards:=1)")
		t.Logf("Eval %s", expr)
	}
	return gqltest.ReadTable(gqltest.Eval(t, expr, env))
}

func testSmallCogroup(t *testing.T, parallel bool) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `t0 := table(
{i:0, s:1},
{i:3, s:2},
{i:0, s:3},
{i:3, s:4},
{i:1, s:5})`, env)
	expect.EQ(t,
		evalCogroup(t, `cogroup(t0, $i)`, parallel, env),
		[]string{
			"{key:0,value:[{i:0,s:1},{i:0,s:3}]}",
			"{key:1,value:[{i:1,s:5}]}",
			"{key:3,value:[{i:3,s:2},{i:3,s:4}]}",
		})

	expect.EQ(t,
		evalCogroup(t, `cogroup(t0, $i, map:=$s)`, parallel, env),
		[]string{
			"{key:0,value:[1,3]}",
			"{key:1,value:[5]}",
			"{key:3,value:[2,4]}",
		})

	expect.EQ(t,
		[]string{
			"{key:0,count:2}",
			"{key:1,count:1}",
			"{key:3,count:2}",
		},
		evalCogroup(t, `cogroup(t0, $i, map:=$s) | map({$key, count:count($value)})`, parallel, env))
}

func TestSmallCogroup(t *testing.T)         { testSmallCogroup(t, false) }
func TestSmallCogroupParallel(t *testing.T) { testSmallCogroup(t, true) }

func testNestedCogroup1(t *testing.T, parallel bool) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `t0 := table(
{i:0, s:1},
{i:3, s:2},
{i:0, s:3},
{i:3, s:4},
{i:1, s:5})`, env)

	expect.That(t,
		evalCogroup(t, `t0 | cogroup($i) | map($value) | flatten() | cogroup($s)`, parallel, env),
		h.ElementsAre(
			`{key:1,value:[{i:0,s:1}]}`,
			`{key:2,value:[{i:3,s:2}]}`,
			`{key:3,value:[{i:0,s:3}]}`,
			`{key:4,value:[{i:3,s:4}]}`,
			`{key:5,value:[{i:1,s:5}]}`,
		))
}

func TestNestedCogroup1(t *testing.T)         { testNestedCogroup1(t, false) }
func TestNestedCogroupParallel1(t *testing.T) { testNestedCogroup1(t, true) }

// Test ashenoy's double cogroup that crashed gql before 2018-10.
func testNestedCogroup2(t *testing.T, parallel bool) {
	tempDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	save := func(filename, text string) (path string) {
		path = filepath.Join(tempDir, filename)
		assert.NoError(t, ioutil.WriteFile(path, []byte(text), 0644))
		return
	}

	env := gqltest.NewSession()

	readsTSV := save("reads.tsv", `id	read	strand
20	6.21077.[5:0]reads.6-7.AGCTCC+GTAAGT	ForwardNormal
202	6.21080.[7:0]reads.6-7.GTAAGT+AGCTCC	ReverseNormal
230	6.21082.[3:0]reads.6-7.AGCTCC+TCCTGG	ForwardNormal
480	6.21083.[28:0]reads.6-7.TCTCAT+GTATCT	ReverseNormal
`)

	fragsTSV := save("frags.tsv", `
id	chromosome	start1	end1	start2	end2
20	chr1	350000	350200	345000	350200
202	chr1	345000	350200	350000	350200
230	chr1	20000	20400	20050	20480
480	chr1	50	200	50	200
`)

	gqltest.Eval(t, fmt.Sprintf(`reads := read("%s")`, readsTSV), env)
	gqltest.Eval(t, fmt.Sprintf(`genomicFragments := read("%s")`, fragsTSV), env)

	expr := `
process := func(table) {
  // Add the dir column value across rows
  sumdir := (table | reduce(0, _acc+_val, map:=$dir) | pick(true)).value;
  // Append the sumdir value to each row
  table | map({_./.*/, sumdir});
};

x := join({reads: reads, geneFrags: genomicFragments}, reads.id == geneFrags.id)
| filter($reads_strand == "ForwardNormal" || $reads_strand == "ReverseNormal")
| map({id:$reads_read,
       chr: $geneFrags_chromosome,
       start1: $geneFrags_start1,
       end1: $geneFrags_end1,
       start2: $geneFrags_start2,
       end2: $geneFrags_end2,
       geneFragLength: max($geneFrags_end1, $geneFrags_end2) - min($geneFrags_start1, $geneFrags_start2),
       fwd_umi: cond($reads_strand == "ForwardNormal",
        regexp_replace($reads_read, ".*[.]([ATCG]+)[+]([ATCG]+)", "$1"),
        regexp_replace($reads_read, ".*[.]([ATCG]+)[+]([ATCG]+)", "$2")),
       rev_umi: cond($reads_strand == "ForwardNormal",
        regexp_replace($reads_read, ".*[.]([ATCG]+)[+]([ATCG]+)", "$2"),
        regexp_replace($reads_read, ".*[.]([ATCG]+)[+]([ATCG]+)", "$1")),
       dir: cond($reads_strand == "ForwardNormal", 1, 0)})
| cogroup({$chr, $fwd_umi, $rev_umi, min($start1, $start2), max($end1, $end2)});

flatten(x | map(row.value | map({$id, $fwd_umi, $rev_umi, $chr, $start1, $start2, $end1, $end2, $geneFragLength, $dir, count: count(row.value)}), row:=row))`

	expect.That(t,
		gqltest.ReadTable(gqltest.Eval(t, expr, env)),
		h.ElementsAre(
			`{id:6.21077.[5:0]reads.6-7.AGCTCC+GTAAGT,fwd_umi:AGCTCC,rev_umi:GTAAGT,chr:chr1,start1:350000,start2:345000,end1:350200,end2:350200,geneFragLength:5200,dir:1,count:2}`,
			`{id:6.21080.[7:0]reads.6-7.GTAAGT+AGCTCC,fwd_umi:AGCTCC,rev_umi:GTAAGT,chr:chr1,start1:345000,start2:350000,end1:350200,end2:350200,geneFragLength:5200,dir:0,count:2}`,
			`{id:6.21082.[3:0]reads.6-7.AGCTCC+TCCTGG,fwd_umi:AGCTCC,rev_umi:TCCTGG,chr:chr1,start1:20000,start2:20050,end1:20400,end2:20480,geneFragLength:480,dir:1,count:1}`,
			`{id:6.21083.[28:0]reads.6-7.TCTCAT+GTATCT,fwd_umi:GTATCT,rev_umi:TCTCAT,chr:chr1,start1:50,start2:50,end1:200,end2:200,geneFragLength:150,dir:0,count:1}`))
}

func TestNestedCogroup2(t *testing.T)         { testNestedCogroup2(t, false) }
func TestNestedCogroupParallel2(t *testing.T) { testNestedCogroup2(t, true) }

package gql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/grailbio/gql/gqltest"
)

func TestSimpleJoin(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `
  tbl0 := table({c0:10});
  tbl1 := table({c0:10});`, env)
	gqltest.Eval(t, `join({xt0: tbl0, xt1: tbl1}, xt0.c0==xt1.c0, map:={xt0.c0})`, env)
}

func TestTwoWayJoin0(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{fi:0, fs:"ab2"},
{fi:2, fs:"ab1"},
{fi:3, fs:"ab0"})`, env)
	gqltest.Eval(t, `T1 := table(
{fj:1, fs:"cd2"},
{fj:2, fs:"cd1"},
{fj:4, fs:"ab0"})`, env)

	assert.Equal(t,
		[]string{`{fi:2,fs:cd1}`},
		gqltest.ReadTable(gqltest.Eval(t, "join({T0,T1}, T0.fi==T1.fj, map:={fi: T0.fi, fs:T1.fs})", env)))

	assert.Equal(t,
		[]string{`{fi:2,fs:cd1}`},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fi==t1.fj, map:={fi: t0.fi, fs:t1.fs})", env)))

	assert.Equal(t,
		[]string{
			`{t0_fi:0,t0_fs:ab2}`,
			`{t1_fj:1,t1_fs:cd2}`,
			`{t0_fi:2,t0_fs:ab1,t1_fj:2,t1_fs:cd1}`,
			`{t0_fi:3,t0_fs:ab0}`,
			`{t1_fj:4,t1_fs:ab0}`},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fi?==?t1.fj)", env)))

	assert.Equal(t,
		[]string{`{fi:2,fs:cd1}`},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fi==t1.fj, map:={fi: t0.fi, fs:t1.fs})", env)))

	assert.Equal(t,
		[]string{`{fi:2,fs:cd1}`},
		gqltest.ReadTable(gqltest.Eval(t, "join({t1:T1,t0:T0}, t1.fj==t0.fi, map:={fi: t0.fi, fs:t1.fs})", env)))

	assert.Equal(t,
		[]string{
			`{t0_fi:3,t0_fs:ab0,t1_fj:4,t1_fs:ab0}`,
			`{t0_fi:2,t0_fs:ab1}`,
			`{t0_fi:0,t0_fs:ab2}`,
			`{t1_fj:2,t1_fs:cd1}`,
			`{t1_fj:1,t1_fs:cd2}`},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fs?==?t1.fs)", env)))
	assert.Equal(t,
		[]string{`{t0_fi:2,t0_fs:ab1,t1_fj:2,t1_fs:cd1}`},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fi==t1.fj)", env)))
	assert.Equal(t,
		[]string{`{t0_fi:3,t0_fs:ab0,t1_fj:4,t1_fs:ab0}`},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fs==t1.fs)", env)))
	assert.Equal(t,
		[]string{
			`{t0_fi:0,t0_fs:ab2}`,
			`{t0_fi:2,t0_fs:ab1,t1_fj:2,t1_fs:cd1}`,
			`{t0_fi:3,t0_fs:ab0}`},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fi==?t1.fj)", env)))
	assert.Equal(t,
		[]string{
			`{t0_fi:3,t0_fs:ab0,t1_fj:4,t1_fs:ab0}`,
			`{t0_fi:2,t0_fs:ab1}`,
			`{t0_fi:0,t0_fs:ab2}`},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fs==?t1.fs)", env)))
}

func TestTwoWayJoin1(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{f01:1, f02:"cd2"},
{f01:2, f02:"cd2"},
{f01:3, f02:"ab0"})`, env)
	gqltest.Eval(t, `T1 := table(
{f11:1, f12:"cd2"},
{f11:2, f12:"cd1"},
{f11:4, f12:"ab0"})`, env)

	// Two eqjoin conditions for the same pair of tables.  The tables are sorted
	// by the first condition, and the 2nd condition is used to post-filter the
	// results.
	assert.Equal(t,
		[]string{"{f01:1,f02:cd2,f11:1,f12:cd2}"},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.f01==t1.f11 && t0.f02==t1.f12, map:={f01:t0.f01,f02:t0.f02, f11:t1.f11, f12: t1.f12})", env)))

	assert.Equal(t,
		[]string{"{f01:2,f02:cd2,f11:2,f12:cd1}"},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.f01==t1.f11 && t0.f02!=t1.f12, map:={f01:t0.f01,f02:t0.f02, f11:t1.f11, f12: t1.f12})", env)))
}

func TestSelfJoin(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{f0:0, f1:2, f2:"a"},
{f0:2, f1:3, f2:"b"},
{f0:3, f1:4, f2:"c"})`, env)
	assert.Equal(t,
		[]string{
			"{f0:2,f2x:b,f2y:a}",
			"{f0:3,f2x:c,f2y:b}"},
		gqltest.ReadTable(gqltest.Eval(t,
			"join({tx:T0,ty:T0}, tx.f0==ty.f1, map:={f0:tx.f0, f2x:tx.f2, f2y:ty.f2})", env)))
}

func TestExcludeJoin(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{fi:0, fs:"ab2"},
{fi:2, fs:"ab1"},
{fi:3, fs:"ab0"})`, env)
	gqltest.Eval(t, `T1 := table(
{fj:1, fs:"cd2"},
{fj:2, fs:"cd1"},
{fj:4, fs:"ab0"})`, env)

	// Pick all rows in T0 that don't appear in T1.
	assert.Equal(t,
		[]string{
			"{fi:0,fs:ab2}",
			"{fi:3,fs:ab0}"},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fi==?t1.fj && isnull(t1.fj), map:={t0./.*/})", env)))

	// Pick all rows in T1 that don't appear in T0.
	assert.Equal(t,
		[]string{
			"{fj:1,fs:cd2}",
			"{fj:4,fs:ab0}"},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fi?==t1.fj && isnull(t0.fi), map:={t1./.*/})", env)))
}

// 3-way join with a single set of eqjoin columns.
func TestThreeWayJoin0(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `t0 := table(
{f0:0, f1:"ab2", f2: "x0"},
{f0:2, f1:"ab1", f2: "x1"},
{f0:3, f1:"ab0", f2: "x2"})`, env)
	gqltest.Eval(t, `t1 := table(
{f3:0, f4:"cd2", f12:20},
{f3:2, f4:"cd1", f12:21},
{f3:4, f4:"ab0", f12:22})`, env)
	gqltest.Eval(t, `t2 := table(
{f21:10, f22:1},
{f21:11, f22:2},
{f21:12, f22:5})`, env)
	assert.Equal(t,
		[]string{},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:t0,t1:t1,t2:t2}, t0.f0==t1.f3 && t1.f3==t2.f22 && t2.f21==t1.f12, map:={f0:t0.f0, f2:t0.f2, f4:t1.f4, f21:t2.f21})", env)))
	assert.Equal(t,
		[]string{
			"{f0:2,f2:x1,f4:cd1,f21:11}",
		},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:t0,t1:t1,t2:t2}, t0.f0==t1.f3 && t1.f3==t2.f22 && t2.f21==t2.f21, map:={f0:t0.f0, f2:t0.f2, f4:t1.f4, f21:t2.f21})", env)))
	assert.Equal(t,
		[]string{
			"{f0:2,f2:x1,f4:cd1,f21:11}",
		},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:t0,t1:t1,t2:t2}, t0.f0==t1.f3 && t1.f3==t2.f22, map:={f0:t0.f0, f2:t0.f2, f4:t1.f4, f21:t2.f21})", env)))
	assert.Equal(t,
		[]string{},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:t0,t1:t1,t2:t2}, t0.f0==t1.f3 && t1.f3==t2.f21, map:={f0:t0.f0, f2:t0.f2, f4:t1.f4, f21:t2.f21})", env)))
}

// 3-way join with two sets of eqjoin columns.
func TestThreeWayJoin1(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `t0 := table(
{f0:0, f1:"ab2", f2: "x0"},
{f0:2, f1:"ab1", f2: "x1"},
{f0:3, f1:"ab0", f2: "x2"})`, env)
	gqltest.Eval(t, `t1 := table(
{f3:0, f4:"cd2"},
{f3:2, f4:"cd1"},
{f3:4, f4:"ab0"})`, env)
	gqltest.Eval(t, `t2 := table(
{f5:"x0", f6:10},
{f5:"x1", f6:11},
{f5:"x2", f6:12})`, env)
	assert.Equal(t,
		[]string{
			"{f2:x0,f4:cd2,f6:10}",
			"{f2:x1,f4:cd1,f6:11}",
		},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:t0,t1:t1,t2:t2}, t0.f0==t1.f3 && t0.f2==t2.f5, map:={f2:t0.f2, f4:t1.f4, f6:t2.f6})", env)))
}

func TestCrossJoin0(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{fi:0, fs:"ab2"},
{fi:2, fs:"ab1"},
{fi:2, fs:"ab0"})`, env)
	gqltest.Eval(t, `T1 := table(
{fj:1, ft:"cd3"},
{fj:2, ft:"cd1"},
{fj:2, ft:"cd2"})`, env)

	assert.Equal(t,
		[]string{
			`{t0_fi:2,t0_fs:ab1,t1_fj:2,t1_ft:cd1}`,
			`{t0_fi:2,t0_fs:ab0,t1_fj:2,t1_ft:cd1}`,
			`{t0_fi:2,t0_fs:ab1,t1_fj:2,t1_ft:cd2}`,
			`{t0_fi:2,t0_fs:ab0,t1_fj:2,t1_ft:cd2}`,
		},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fi==t1.fj)", env)))

	assert.Equal(t,
		[]string{
			`{t0_fi:0,t0_fs:ab2}`,
			`{t0_fi:2,t0_fs:ab1,t1_fj:2,t1_ft:cd1}`,
			`{t0_fi:2,t0_fs:ab0,t1_fj:2,t1_ft:cd1}`,
			`{t0_fi:2,t0_fs:ab1,t1_fj:2,t1_ft:cd2}`,
			`{t0_fi:2,t0_fs:ab0,t1_fj:2,t1_ft:cd2}`,
		},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fi==?t1.fj)", env)))

	assert.Equal(t,
		[]string{
			`{t0_fi:0,t0_fs:ab2}`,
			`{t1_fj:1,t1_ft:cd3}`,
			`{t0_fi:2,t0_fs:ab1,t1_fj:2,t1_ft:cd1}`,
			`{t0_fi:2,t0_fs:ab0,t1_fj:2,t1_ft:cd1}`,
			`{t0_fi:2,t0_fs:ab1,t1_fj:2,t1_ft:cd2}`,
			`{t0_fi:2,t0_fs:ab0,t1_fj:2,t1_ft:cd2}`,
		},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:T0,t1:T1}, t0.fi?==?t1.fj)", env)))
}

func TestCrossJoin1(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `T2 := table(
{f21:2, f22:"ef4"},
{f21:3, f22:"ef5"})`, env)
	gqltest.Eval(t, `T3 := table(
{f31:2, f32:"ge6"},
{f31:4, f32:"ge7"})`, env)

	// Join with no "where" clause will create a simple cross product.
	assert.Equal(t,
		[]string{
			"{t2_f21:2,t2_f22:ef4,t3_f31:2,t3_f32:ge6}",
			"{t2_f21:2,t2_f22:ef4,t3_f31:4,t3_f32:ge7}",
			"{t2_f21:3,t2_f22:ef5,t3_f31:2,t3_f32:ge6}",
			"{t2_f21:3,t2_f22:ef5,t3_f31:4,t3_f32:ge7}",
		},
		gqltest.ReadTable(gqltest.Eval(t, "join({t2:T2,t3:T3}, true)", env)))

	// Add a where clause that involves just t2. It will create a cross product then
	// post-filter the result.
	assert.Equal(t,
		[]string{
			"{t2_f21:3,t2_f22:ef5,t3_f31:2,t3_f32:ge6}",
			"{t2_f21:3,t2_f22:ef5,t3_f31:4,t3_f32:ge7}",
		},
		gqltest.ReadTable(gqltest.Eval(t, "join({t2:T2,t3:T3}, t2.f21==3)", env)))
}

// 3-way join with a single set of eqjoin columns.
//
// TODO(saito) As of 2018/08, this code runs using repeated mergejoin, not
// hashjoin. Implement a real hash join.
func TestHashJoin(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `t0 := table(
{f01: "ab0", f02: 1},
{f01: "cd0", f02: 2})`, env)
	gqltest.Eval(t, `t1 := table(
{f11: "ab1", f12: 3},
{f11: "cd1", f12: 4})`, env)
	gqltest.Eval(t, `t2 := table(
{f21: "ab0", f22:"ab1", f23: 5},
{f21: "cd0", f22:"cd1", f23: 6})`, env)
	assert.Equal(t,
		[]string{
			"{f02:1,f12:3,f23:5}",
			"{f02:2,f12:4,f23:6}"},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:t0,t1:t1,t2:t2}, t0.f01==t2.f21 && t1.f11==t2.f22, map:={f02:t0.f02, f12:t1.f12, f23:t2.f23})", env)))
	// Shuffle the join expression.
	assert.Equal(t,
		[]string{
			"{f02:1,f12:3,f23:5}",
			"{f02:2,f12:4,f23:6}"},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:t0,t1:t1,t2:t2}, t1.f11==t2.f22 && t0.f01==t2.f21, map:={f02:t0.f02, f12:t1.f12, f23:t2.f23})", env)))
	assert.Equal(t,
		[]string{
			"{f02:1,f12:3,f23:5}",
			"{f02:2,f12:4,f23:6}"},
		gqltest.ReadTable(gqltest.Eval(t, "join({t0:t0,t1:t1,t2:t2}, t0.f01==t2.f21 && t2.f22==t1.f11, map:={f02:t0.f02, f12:t1.f12, f23:t2.f23})", env)))
}

func TestJoinNested(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `t0 := table(
{f01: "ab0", f02: 1},
{f01: "cd0", f02: 2})`, env)
	gqltest.Eval(t, `t1 := table(
{f11: "ab0", f12: 3},
{f11: "cd1", f12: 4})`, env)
	gqltest.Eval(t, `t2 := table(
{f21: "ab0", f22: 5},
{f21: "cd0", f22: 6})`, env)
	assert.Equal(t,
		[]string{"{f01:ab0,f02:1,f12:3,f22:5}"},
		gqltest.ReadTable(gqltest.Eval(t,
			"jt := join({t0:t0,t1:t1}, t0.f01==t1.f11, map:={f01:t0.f01, f02:t0.f02, f12:t1.f12});"+
				"join({jt:jt,t2:t2}, jt.f01==t2.f21, map:={f01:jt.f01, f02:jt.f02, f12:jt.f12, f22:t2.f22})",
			env)))
}

func TestJoinInsideFunction(t *testing.T) {
	t.Parallel()
	env := gqltest.NewSession()
	gqltest.Eval(t, `testfunc := func(tbl0) {
  tbl1 := table({c0:10, c1:11}, {c0:20, c1:21});
  join({xt0: tbl0, xt1: tbl1}, xt0.c0==xt1.c0, map:={xt0.c0, xt1.c1})
};

foo := testfunc(table({c0:10}));
`, env)
	assert.Equal(t, gqltest.ReadTable(gqltest.Eval(t, "foo", env)),
		[]string{"{c0:10,c1:11}"})
	assert.Equal(t, gqltest.ReadTable(gqltest.Eval(t, `table(table({c0:10}), table({c0:20})) | map(testfunc(_))`, env)),
		[]string{"[{c0:10,c1:11}]", "[{c0:20,c1:21}]"})
}

func BenchmarkLargeRNAJoin(b *testing.B) {
	b.StopTimer()
	env := gqltest.NewSession()
	//id	read	primary	spliced
	//6811	6.21077.[5:0]reads.6-7.AGCTCC+GTAAGT	true	false
	gqltest.Eval(b, `reads := read("s3://grail-ashenoy/RNA/reads.tsv")`, env)
	//id	gene	sense
	//6811	ENSG00000146904.4	AntiSense
	//6826	ENSG00000146904.4	AntiSense
	gqltest.Eval(b, `genes := read("s3://grail-ashenoy/RNA/genes.tsv")`, env)
	//id	transcript	start1	end1	start2	end2
	//6811	ENST00000497891.1	527	619	527	619
	//6811	ENST00000275815.3	2873	2965	2873	2965
	//6826	ENST00000275815.3	3017	3158	2980	3119
	gqltest.Eval(b, `txpts := read("s3://grail-ashenoy/RNA/transcript_fragments.tsv")`, env)
	//gene	transcript
	//ENSG00000223972.4	ENST00000456328.2
	//ENSG00000223972.4	ENST00000515242.2
	gqltest.Eval(b, `gene_map := read("s3://grail-ashenoy/RNA/gene_txpt_map.tsv")`, env)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		gqltest.Eval(b, `joined := join(
    {reads:reads,genes:genes,txpts:txpts,gene_map:gene_map},
    reads.id==genes.id
    && reads.id==txpts.id
    && genes.gene==gene_map.gene
    && txpts.transcript==gene_map.transcript);
    joined | write("/tmp/joined.btsv");`, env)
	}
}

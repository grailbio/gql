package gql_test

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/grail"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/base/vcontext"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/gqltest"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/gql/termutil"
	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetGlobal(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, "foo := 20 + 15", env)
	gqltest.Eval(t, "bar := foo * 3", env)
	val := gqltest.Eval(t, "bar + 3", env)
	assert.Equal(t, val.Int(nil), int64(20+15)*3+3)
}

func TestSetGlobalConst(t *testing.T) {
	gql.RegisterGlobalConst("blahblahconst", gql.NewInt(12345))
	env := gqltest.NewSession()
	assert.Equal(t, int64(12345), gqltest.Eval(t, "blahblahconst", env).Int(nil))
}

func TestLogicalOps(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, true, gqltest.Eval(t, "true || false", env).Bool(nil))
	assert.Equal(t, true, gqltest.Eval(t, "false || true", env).Bool(nil))
	assert.Equal(t, false, gqltest.Eval(t, "false || false", env).Bool(nil))
	assert.Equal(t, false, gqltest.Eval(t, "true && false", env).Bool(nil))
	assert.Equal(t, false, gqltest.Eval(t, "false && true", env).Bool(nil))
	assert.Equal(t, true, gqltest.Eval(t, "true && true", env).Bool(nil))
	assert.Equal(t, false, gqltest.Eval(t, "!true", env).Bool(nil))
	assert.Equal(t, true, gqltest.Eval(t, "!false", env).Bool(nil))
}

func TestComments(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, int64(1020), gqltest.Eval(t, "1020 /*comment*/", env).Int(nil))
}

func TestNegateNumeral(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, gqltest.Eval(t, "-10", env).Int(nil), int64(-10))
	gqltest.Eval(t, "ABC:=123", env)
	assert.Equal(t, gqltest.Eval(t, "-ABC", env).Int(nil), int64(-123))
	assert.Equal(t, gqltest.Eval(t, "-10.0", env).Float(nil), float64(-10.0))
	assert.Equal(t, gqltest.Eval(t, "10.5e6", env).Float(nil), float64(10.5e6))
	assert.Equal(t, gqltest.Eval(t, "-3h", env).Duration(nil), time.Duration(-3*time.Hour))
}

func TestNegateString(t *testing.T) {
	onetest := func(a, b string) bool {
		env := gqltest.NewSession()
		gqltest.Eval(t, fmt.Sprintf("A:=`%s`", a), env)
		gqltest.Eval(t, fmt.Sprintf("B:=`%s`", b), env)
		nega := gqltest.Eval(t, "-A", env)
		negb := gqltest.Eval(t, "-B", env)
		return nega.Str(nil) < negb.Str(nil)
	}
	for _, test := range []struct {
		a, b     string
		expected bool // true if negate(a)<negate(b)
	}{
		{"bb", "b", true},
		{"a", "b", false},
		{"b", "b", false},
		{"", "b", false},
	} {
		t.Logf("Test %+v", test)
		require.Equal(t, onetest(test.a, test.b), test.expected, "test %+v", test)
	}
	// TODO(saito) randomized test!
}

func TestTableLiteral(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t,
		[]string{},
		gqltest.ReadTable(gqltest.Eval(t, "table()", env)))
	assert.Equal(t,
		[]string{"10", "15"},
		gqltest.ReadTable(gqltest.Eval(t, "table(10,15)", env)))
}

func TestStructLiteral(t *testing.T) {
	env := gqltest.NewSession()
	val := gqltest.Eval(t, "{1,2,`abc`}", env)
	assert.Equal(t, `{f0:1,f1:2,f2:abc}`, val.String())
	val = gqltest.Eval(t, "{x:1,y:2,`abc`}", env)
	assert.Equal(t, `{x:1,y:2,f2:abc}`, val.String())

	gqltest.Eval(t, "foo := 20", env)
	gqltest.Eval(t, "bar := {col0: 21, col1: 22}", env)
	assert.Equal(t, `{foo:20,col0:21,f2:12}`, gqltest.Eval(t, "{foo,bar.col0,12}", env).String())

	dataPath := "./testdata/data.tsv"
	assert.Equal(
		t,
		[]string{
			"{A:1,B:/a}",
			"{A:2,B:s3://a}",
			"{A:NA,B:s3://a}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`) | map({&A,&B})", dataPath), env)))
	assert.Equal(
		t,
		[]string{
			"{C:e1,dd:12}",
			"{C:e1,dd:12}",
			"{C:NA,dd:NA}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`) | map({_.C,dd:$D})", dataPath), env)))
}

func TestStructRegex(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, "T0 := {f0:1,f1:2,g0:`abc`}", env)
	assert.Equal(t,
		"{f0:1,g0:abc}",
		gqltest.Eval(t, "{T0./.*0/}", env).String())
	assert.Equal(t,
		[]string{"{f1:2}"},
		gqltest.ReadTable(gqltest.Eval(t, "table(T0) | map({_./.*1/})", env)))
	assert.Equal(t,
		[]string{"{f1:2}"},
		gqltest.ReadTable(gqltest.Eval(t, "table(T0) | map({/.*1/})", env)))
}

func printValueLong(v gql.Value) string {
	out := termutil.NewBufferPrinter()
	args := gql.PrintArgs{
		Out:                out,
		Mode:               gql.PrintCompact,
		MaxInlinedTableLen: math.MaxInt64,
	}
	v.Print(vcontext.Background(), args)
	return out.String()
}

func TestCond(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, "3", printValueLong(gqltest.Eval(t, "if 2==2 3 else 4", env)))
	assert.Equal(t, "9", printValueLong(gqltest.Eval(t, "if 2==2 {x:=3; x*x} else {x:=4;x*x}", env)))
	assert.Equal(t, "16", printValueLong(gqltest.Eval(t, "if 2!=2 {x:=3; x*x} else {x:=4;x*x}", env)))
	assert.Equal(t, "5", printValueLong(gqltest.Eval(t, "if 2!=2 3 else if 3!=3 4 else 5", env)))
	assert.Equal(t, "4", printValueLong(gqltest.Eval(t, "if 2!=2 3 else if 3==3 4 else 5", env)))
	assert.Equal(t, "3", printValueLong(gqltest.Eval(t, "if 2==2 3 else if 3==3 4 else 5", env)))

	// Old form.
	assert.Equal(t, "3", printValueLong(gqltest.Eval(t, "cond(2==2, 3, 4)", env)))
	assert.Equal(t, "4", printValueLong(gqltest.Eval(t, "cond(2==3, 3, 4)", env)))
	assert.Equal(t, "4", printValueLong(gqltest.Eval(t, "cond(2!=2, 3, 4)", env)))
}

func TestLambda(t *testing.T) {
	env := gqltest.NewSession()
	// Legacy form
	gqltest.Eval(t, `f1 := func(b) {b*b}`, env)
	assert.Equal(t, int64(4), gqltest.Eval(t, "f1(2)", env).Int(nil))

	// New form
	gqltest.Eval(t, `func f2(a, b) (a+b*b)`, env)
	assert.Equal(t, int64(11), gqltest.Eval(t, "f2(2, 3)", env).Int(nil))

	gqltest.Eval(t, `func f3(a, b, c) (a + b*b + c*c*c)`, env)
	assert.Equal(t, int64(2+9+64), gqltest.Eval(t, "f3(2,3,4)", env).Int(nil))

	gqltest.Eval(t, `t0 := table({k:2, v:3})`, env)
	gqltest.Eval(t, `func f4(a) a.k`, env)
	assert.Equal(t, int64(2), gqltest.Eval(t, "f4(pick(t0,true))", env).Int(nil))

	gqltest.Eval(t, `f5 := |x,y|x*y`, env)
	assert.Equal(t, int64(20), gqltest.Eval(t, "f5(4,5)", env).Int(nil))

	gqltest.Eval(t, `f6 := |x,y|{z:=x*y; z*z}`, env)
	assert.Equal(t, int64(400), gqltest.Eval(t, "f6(4,5)", env).Int(nil))
}

func TestLambdaLocalVariable(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `f1 := func(b) { x := b+1; y := x * 2; y*y }`, env)
	assert.Equal(t, int64(36), gqltest.Eval(t, "f1(2)", env).Int(nil))
	assert.Equal(t, int64(64), gqltest.Eval(t, "f1(3)", env).Int(nil))
}

func TestLambdaNestedLocalVariable(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `xx := 10`, env)
	// In xx := xx * 10, the RHS resolve to 10.  `xx + b` will use the locally
	// bound xx.
	gqltest.Eval(t, `f1 := func(b) { xx := xx * 10; xx + b }`, env)
	assert.Equal(t, int64(102), gqltest.Eval(t, "f1(2)", env).Int(nil))
	assert.Equal(t, int64(103), gqltest.Eval(t, "f1(3)", env).Int(nil))
}

func TestBlock(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `xx := 10`, env)
	// In xx := xx * 10, the RHS resolve to 10.  `xx + b` will use the locally
	// bound xx.
	gqltest.Eval(t, `f1 := func(b) { xx := xx * 10; xx + b }`, env)
	assert.Equal(t, int64(102), gqltest.Eval(t, "f1(2)", env).Int(nil))
	assert.Equal(t, int64(103), gqltest.Eval(t, "f1(3)", env).Int(nil))
}

func TestTranspose(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{key:0, value:table({c0:"ab0", c1:1, c2: 123}, {i:0, c0:"ab1", c1:1, c2: 234})},
{key:1, value:table({c0:"ab0", c1:2, c2: 345}, {i:1, c0:"ab1", c1:2, c2: 456})},
{key:2, value:table({c0:"ab2", c1:3, c2: 567})})`, env)
	assert.Equal(t,
		[]string{
			"{key:0,ab0_1:123,ab1_1:234}",
			"{key:1,ab0_2:345,ab1_2:456}",
			"{key:2,ab2_3:567}"},
		gqltest.ReadTable(gqltest.Eval(t, `transpose(T0, {&key}, {&c0,&c1,&c2})`, env)))

	assert.Equal(t,
		[]string{
			"{key:0,ab0_1:100,ab1_1:100}",
			"{key:1,ab0_2:101,ab1_2:101}",
			"{key:2,ab2_3:102}"},
		gqltest.ReadTable(gqltest.Eval(t, `transpose(T0, {&key}, |key,val|{val.c0,val.c1,key.key+100})`, env)))
}

func TestDate(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, "2017-12-22T03:05:32-0700", printValueLong(gqltest.Eval(t, `L1 := 2017-12-22T03:05:32-0700`, env)))
	assert.Equal(t, "2017-12-22T03:05:32+0700", printValueLong(gqltest.Eval(t, `2017-12-22T03:05:32+0700`, env)))
	assert.Equal(t, "2017-12-22T03:05:32+0000", printValueLong(gqltest.Eval(t, `2017-12-22T03:05:32Z`, env)))

	assert.True(t, gqltest.Eval(t, `2017-12-22 > 2017-11-22`, env).Bool(nil))
	assert.True(t, gqltest.Eval(t, `2017-12-22 == 2017-12-22`, env).Bool(nil))
	assert.False(t, gqltest.Eval(t, `2017-12-22 == 2017-12-23`, env).Bool(nil))
	assert.False(t, gqltest.Eval(t, `2017-12-22 != 2017-12-22`, env).Bool(nil))
	assert.True(t, gqltest.Eval(t, `2017-12-22 != 2017-12-23`, env).Bool(nil))

	assert.True(t, gqltest.Eval(t, `2017-12-22 >= 2017-12-21`, env).Bool(nil))
	assert.True(t, gqltest.Eval(t, `2017-12-22 >= 2017-12-22`, env).Bool(nil))
	assert.False(t, gqltest.Eval(t, `2017-12-22 >= 2017-12-23`, env).Bool(nil))

	assert.True(t, gqltest.Eval(t, `2017-12-22 > 2017-12-21`, env).Bool(nil))
	assert.False(t, gqltest.Eval(t, `2017-12-22 > 2017-12-22`, env).Bool(nil))
	assert.False(t, gqltest.Eval(t, `2017-12-22 > 2017-12-23`, env).Bool(nil))
}

func TestDuration(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, gqltest.Eval(t, "3h", env).Duration(nil), 3*time.Hour)
	assert.Equal(t, gqltest.Eval(t, "3h1s", env).Duration(nil), 3*time.Hour+time.Second)
	assert.Equal(t, gqltest.Eval(t, "3h1s123us", env).Duration(nil), 3*time.Hour+time.Second+123*time.Microsecond)
	assert.Equal(t, gqltest.Eval(t, "3h1s124µs", env).Duration(nil), 3*time.Hour+time.Second+124*time.Microsecond)
	assert.True(t, gqltest.Eval(t, `3h == 3h0m`, env).Bool(nil))
	assert.False(t, gqltest.Eval(t, `3h == 3h1s`, env).Bool(nil))
}

func TestPipe(t *testing.T) {
	env := gqltest.NewSession()

	// Verify that "&" expansion works for each pipeline component independently.
	gqltest.Eval(t, `T0 := table(
{i:0, s:"ab0"},
{i:1, s:"ab1"},
{i:2, s:"ab2"})`, env)
	assert.Equal(t,
		[]string{"{xx:10}", "{xx:11}", "{xx:12}"},
		gqltest.ReadTable(gqltest.Eval(t, "T0 | map({&i}) | map({xx:&i+10})", env)))
}

func TestMapSmall(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{i:0, s:"ab0"},
{i:1, s:"ab1"},
{i:2, s:"ab2"})`, env)
	assert.Equal(t,
		[]string{"{i:0}", "{i:1}", "{i:2}"},
		gqltest.ReadTable(gqltest.Eval(t, "T0 | map({&i})", env)))
	assert.Equal(t,
		[]string{"{i:0}", "{i:1}", "{i:2}"},
		gqltest.ReadTable(gqltest.Eval(t, "T0 | map({i:blah.i}, row:=blah)", env)))
	assert.Equal(t,
		[]string{"{i:0}", "{i:1}", "{i:4}"},
		gqltest.ReadTable(gqltest.Eval(t, "T0 | map({i:&i*&i})", env)))
	assert.Equal(t,
		[]string{"0", "1", "4"},
		gqltest.ReadTable(gqltest.Eval(t, "T0 | map(&i*&i)", env)))
	assert.Equal(t,
		[]string{
			`{i:3,s:ab0x}`,
			`{i:4,s:ab1x}`,
			`{i:5,s:ab2x}`},
		gqltest.ReadTable(gqltest.Eval(t, "T0 | map({i:&i+3, s:&s+`x`})", env)))
}

func TestMapMultipleOutputs(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{i:0, s:"ab0"},
{i:1, s:"ab1"},
{i:2, s:"ab2"})`, env)
	assert.Equal(t,
		[]string{"{i:0}", "{i:1}", "{i:1}", "{i:4}", "{i:2}", "{i:9}"},
		gqltest.ReadTable(gqltest.Eval(t, "map(T0, {i:&i}, {i:(&i+1)*(&i+1)})", env)))

	assert.Equal(t,
		[]string{`{i:ab1}`, `{i:ab1ab1}`, `{i:ab2}`, `{i:ab2ab2}`},
		gqltest.ReadTable(gqltest.Eval(t, "map(T0, {i:&s}, {i:&s+&s}, filter:=&i>0)", env)))
}

func TestFirstN(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{i:0, s:"ab0"},
{i:1, s:"ab1"},
{i:2, s:"ab2"})`, env)
	assert.Equal(t,
		[]string{"{i:0,s:ab0}", "{i:1,s:ab1}"},
		gqltest.ReadTable(gqltest.Eval(t, "firstn(T0, 2)", env)))
	assert.Equal(t,
		[]string{"{i:0,s:ab0}"},
		gqltest.ReadTable(gqltest.Eval(t, "firstn(T0, 1)", env)))
}

func TestFlatten(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{fi:0, fs:"ab2"},
{fi:3, fs:"ab0"})`, env)
	gqltest.Eval(t, `T1 := table(
{fj:1, fs:"cd2"},
{fj:4, fs:"ab0"})`, env)
	gqltest.Eval(t, `T2 := table(
{fj:1, fk:11},
{fj:4, fj:12})`, env)

	for _, shards := range []int{1, 3, 5, 15, 21} {
		t.Logf("Flatten shards: %d", shards)
		assert.Equal(t,
			[]string{"{fi:0,fs:ab2}", "{fi:3,fs:ab0}", "{fj:1,fs:cd2}", "{fj:4,fs:ab0}"},
			gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("table(T0, T1) | flatten(subshard := true) | map(_, shards:=%d)", shards), env)))
	}
	assert.Equal(t,
		[]string{"{fi:0,fs:ab2}", "{fi:3,fs:ab0}", "{fj:1,fs:cd2}", "{fj:4,fs:ab0}"},
		gqltest.ReadTable(gqltest.Eval(t, "table(T0, T1) | flatten()", env)))
}

func TestConcat(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{fi:0, fs:"ab2"},
{fi:3, fs:"ab0"})`, env)
	gqltest.Eval(t, `T1 := table(
{fj:1, fs:"cd2"},
{fj:4, fs:"ab0"})`, env)
	gqltest.Eval(t, `T2 := table(
{fj:1, fk:11},
{fj:4, fj:12})`, env)

	assert.Equal(t,
		[]string{"{fi:0,fs:ab2}", "{fi:3,fs:ab0}", "{fj:1,fs:cd2}", "{fj:4,fs:ab0}"},
		gqltest.ReadTable(gqltest.Eval(t, "concat(T0, T1)", env)))
	assert.Equal(t,
		[]string{"{fi:0,fs:ab2}", "{fi:3,fs:ab0}", "{fj:1,fs:cd2}", "{fj:4,fs:ab0}", "{fj:1,fk:11}", "{fj:4,fj:12}"},
		gqltest.ReadTable(gqltest.Eval(t, "concat(flatten(table(T0, T1)), T2)", env)))
}

func TestParallelMap1(t *testing.T) {
	env := gqltest.NewSession()
	path := "./testdata/data.tsv"

	v := gqltest.Eval(t, fmt.Sprintf("read(`%s`) | map({&A, &B}, filter:=&A==2, shards:=3)", path), env)
	assert.Equal(t, []string{"{A:2,B:s3://a}"}, gqltest.ReadTable(v))
}

func TestParallelMapWithLambda(t *testing.T) {
	path := "./testdata/data.tsv"

	env := gqltest.NewSession()
	gqltest.Eval(t, `filterExpr := func(row) { vv := row; vv.A == 2 }`, env)
	v := gqltest.Eval(t, fmt.Sprintf("read(`%s`) | map({&A, &B}, filter:=filterExpr(_), shards:=3)", path), env)
	assert.Equal(t, []string{"{A:2,B:s3://a}"}, gqltest.ReadTable(v))
}

func TestMapScalar(t *testing.T) {
	env := gqltest.NewSession()
	val := gqltest.Eval(t, "map(table(10,11,15), _*_)", env)
	assert.Equal(t, []string{"100", "121", "225"}, gqltest.ReadTable(val))
}

func TestMapTable(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table(
{fi:0, fs:"ab2"},
{fi:2, fs:"ab1"},
{fi:2, fs:"ab0"})`, env)

	assert.Equal(t,
		[]string{
			"{fi:3,fs:ab2x}", "{fi:5,fs:ab1x}", "{fi:5,fs:ab0x}",
		},
		gqltest.ReadTable(gqltest.Eval(t, "map(T0, {fi: _.fi+3, fs:&fs+\"x\"})", env)))
}

func testMapNestedTables(t *testing.T, parallel bool) {
	env := gqltest.NewSession()
	gqltest.Eval(t, `T0 := table({fi:0, fs:"ab2"})`, env)
	gqltest.Eval(t, `T1 := table({fi:1, fs:"cd3"})`, env)
	gqltest.Eval(t, `T2 := table({table: T1, id: 11}, {table: T0, id: 10})`, env)

	shards := 0
	if parallel {
		shards = 1
	}
	assert.Equal(t,
		[]string{`{tbl:[{fi:1,fs:cd3}]}`, `{tbl:[]}`},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf(`T2 | map({tbl: &table | filter(|row|row.fi > 0)}, shards:=%d)`, shards), env)))
	assert.Equal(t,
		[]string{
			"[{id:11,fi2:101,fs2:cd3y}]",
			"[{id:10,fi2:100,fs2:ab2y}]"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf(`T2 | map(|r|(r.table | map(|x|{id:r.id, fi2:x.fi*x.fi+100, fs2:x.fs+"y"})), shards:=%d)`, shards), env)))
	assert.Equal(t,
		[]string{
			"[{fi2:101,fs2:cd3y}]", "[{fi2:100,fs2:ab2y}]",
		},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf(`T2 | map(&table | map(|x|{fi2:x.fi*x.fi+100, fs2:x.fs+"y"}), shards:=%d)`, shards), env)))
	assert.Equal(t,
		[]string{"[[{id:11,fi2:101,fs2:cd3y}],[{id:10,fi2:100,fs2:ab2y}]]"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf(`table(T2) | map(|t|(t | map(|r|(r.table | map(|x|{id:r.id, fi2:x.fi*x.fi+100, fs2:x.fs+"y"})))), shards:=%d)`, shards), env)))
}

func TestMapNestedTables(t *testing.T)         { testMapNestedTables(t, false) }
func TestMapNestedTablesParallel(t *testing.T) { testMapNestedTables(t, true) }

func Test2ndOrderLambda(t *testing.T) {
	env := gqltest.NewSession()
	if false {
		gqltest.Eval(t, `
f0 := func(a) {c := 10; func() { a + c }};
f1 := func() {c1 := 20; func(a) { a + c1 }};
`, env)
		assert.Equal(t, gqltest.Eval(t, "f0(5)()", env).Int(nil), int64(15))
		assert.Equal(t, gqltest.Eval(t, "g0 := f0(7); g0()", env).Int(nil), int64(17))
		assert.Equal(t, gqltest.Eval(t, "g0()", env).Int(nil), int64(17))
		assert.Equal(t, gqltest.Eval(t, "f1()(5)", env).Int(nil), int64(25))
		assert.Equal(t, gqltest.Eval(t, "g1(7)", env).Int(nil), int64(27))
		assert.Equal(t, gqltest.ReadTable(gqltest.Eval(t, "t0 | map(g1(_))", env)),
			[]string{"20", "21", "22"})
	}
	gqltest.Eval(t, "t0 := table(0,1,2)", env)
	gqltest.Eval(t, `
f1 := func() {c1 := 20; func(a) { a + c1 }};
g1 := f1();
`, env)
	fmt.Println("START2")
	assert.Equal(t, gqltest.ReadTable(gqltest.Eval(t, "t0 | map(g1(_), shards:=1)", env)),
		[]string{"20", "21", "22"})
}

func TestIsNull(t *testing.T) {
	env := gqltest.NewSession()
	assert.True(t, gqltest.Eval(t, "isnull(NA)", env).Bool(nil))
	assert.True(t, gqltest.Eval(t, "isnull(-NA)", env).Bool(nil))
	assert.False(t, gqltest.Eval(t, "isnull(10)", env).Bool(nil))
}

func TestIntOps(t *testing.T) {
	for _, test := range []struct {
		expr     string
		expected int
	}{
		{"1+int(`2`)", 3},
		{"1+int(2.0)", 3},
		{"int('a')+1", 98},
		{"1+2", 3},
		{"1-2", -1},
		{"5*3", 15},
		{"1+5*3", 16},
		{"(1+5)*3", 18},
		{"1-2-3", -4},
		{"10 / 3", 3},
		{"10 % 3", 1},
		{"land(0x3, 0x1)", 1},
		{"lor(0x3, 0x1)", 3},
		{"int(1s)", int(time.Second)},
	} {
		t.Run(test.expr, func(t *testing.T) {
			env := gqltest.NewSession()
			t.Logf("Expr: %v", test.expr)
			val := gqltest.Eval(t, test.expr, env)
			require.Equal(t, val.Int(nil), int64(test.expected),
				"expr %v  |  %v", test.expr, val)
		})
	}
}

func TestFloatOps(t *testing.T) {
	for _, test := range []struct {
		expr     string
		expected float64
	}{
		{"1.0+float(2)", 3.0},
		{"1.0+2.0", 3.0},
		{"1.0-2.0", -1.0},
		{"5.0*3.5", 17.5},
		{"1.5+5.0*3.0", 16.5},
		{"(1.5+5.0)*3.0", 19.5},
		{"1.0-2.0-3.5", -4.5},
		{"10.0 / 2.5", 4},
		{"1.0 / 4.0", 0.25},
		{"1.0 / float(5)", 0.2},
		{"float(1s)", 1.0},
	} {
		t.Run(test.expr, func(t *testing.T) {
			env := gqltest.NewSession()
			val := gqltest.Eval(t, test.expr, env)
			require.Equal(t, val.Float(nil), test.expected,
				"expr %v  |  %v", test.expr, val)
		})
	}
}

func TestDurationOps(t *testing.T) {
	for _, test := range []struct {
		expr     string
		expected time.Duration
	}{
		{"3h+1s", 3*time.Hour + time.Second},
		{"3h-1s", 3*time.Hour - time.Second},
	} {
		t.Run(test.expr, func(t *testing.T) {
			env := gqltest.NewSession()
			val := gqltest.Eval(t, test.expr, env)
			require.Equal(t, val.Duration(nil), test.expected, "expr %v  |  %v", test.expr, val)
		})
	}
}

func TestMinMax(t *testing.T) {
	env := gqltest.NewSession()
	require.Equal(t, gqltest.Eval(t, "max(1)", env).Int(nil), int64(1))
	require.Equal(t, gqltest.Eval(t, "max(1,2,3)", env).Int(nil), int64(3))
	require.Equal(t, gqltest.Eval(t, "max(3,1,2)", env).Int(nil), int64(3))
	require.Equal(t, gqltest.Eval(t, "max(3,1,2)+10", env).Int(nil), int64(13))
	require.Equal(t, gqltest.Eval(t, "min(3,1,2)+10", env).Int(nil), int64(11))
	require.Equal(t, gqltest.Eval(t, "max(3h,3h1s)", env).Duration(nil), 3*time.Hour+1*time.Second)

	require.Equal(t, gqltest.Eval(t, `max("a","ab","abc")`, env).Str(nil), "abc")
	require.Equal(t, gqltest.Eval(t, `min("a","ab","abc")`, env).Str(nil), "a")
}

func TestUnionRow(t *testing.T) {
	for _, test := range []struct {
		expr     string
		expected string
	}{
		{"unionrow({a:11}, {b:10})", "{a:11,b:10}"},
		{"unionrow({a:11}, {a:10})", "{a:10}"},
		{"unionrow({a:11,b:12}, {a:10})", "{a:10,b:12}"},
	} {
		env := gqltest.NewSession()
		val := gqltest.Eval(t, test.expr, env)
		require.Equal(t, test.expected, val.String())
	}
}

func TestSprintf(t *testing.T) {
	env := gqltest.NewSession()
	require.Equal(t, "foo10-bar", gqltest.Eval(t, `sprintf("foo%d-%s", 10, "bar")`, env).Str(nil))
	require.Equal(t, "foo10", gqltest.Eval(t, `sprintf("foo%d", 10)`, env).Str(nil))
	require.Equal(t, "foo10-bar-5.5", gqltest.Eval(t, `sprintf("foo%d-%s-%.1f", 10, "bar", 5.5)`, env).Str(nil))
}

func TestStringOps(t *testing.T) {
	for _, test := range []struct {
		expr     string
		expected string
	}{
		{"string(123)", "123"},
		{"`ab` + `cd`", "abcd"},
	} {
		env := gqltest.NewSession()
		val := gqltest.Eval(t, test.expr, env)
		require.Equal(t, val.Str(nil), test.expected,
			"expr %v  |  %v", test.expr, val)
	}
}

func TestPredicates(t *testing.T) {
	for _, test := range []struct {
		expr     string
		expected bool
	}{
		{"true==true", true},
		{"true==false", false},
		{"true!=false", true},
		{"!(10==11)", true},
		{"NA==NA", true},
		{"NA==-NA", false},
		{"0==NA", false},
		{"10>10", false},
		{"10>9", true},
		{"10>11", false},
		{"10<10", false},
		{"10<9", false},
		{"10<11", true},
		{"10>=10", true},
		{"10>=9", true},
		{"10>=11", false},
		{"10<=10", true},
		{"10<=9", false},
		{"10<=11", true},
		{"10==10", true},
		{"10==11", false},
		{"10!=10", false},
		{"10!=11", true},
		{"`foo2`>`foo`", true},
		{"10.5>10.0", true},
		{"isset(0xf, 1)", true},
		{"isset(1, 0xf)", false},
		{"isset(0, 0)", true},
		{"isset(0xff, 0)", true},
		{"isset(0xf, 0xff)", false},
		{"isset(0x1ff, 0xff)", true},
	} {
		t.Run(test.expr, func(t *testing.T) {
			env := gqltest.NewSession()
			val := gqltest.Eval(t, test.expr, env)
			require.Equal(t, val.Bool(nil), test.expected,
				"expr %v  |  %v", test.expr, val)
		})
	}
}

func TestBED(t *testing.T) {
	gql.MaxTSVRowsInMemory = 10
	bedPath := "./testdata/test4.bed"
	env := gqltest.NewSession()
	val := gqltest.Eval(t, fmt.Sprintf("BED := read(`%s`)", bedPath), env)

	assert.Equal(t,
		[]string{
			"{chrom:chr1,start:5,end:85,featname:region1}",
			"{chrom:chr1,start:100,end:200,featname:region2}",
			"{chrom:chr1,start:300000,end:300180,featname:region3}",
			"{chrom:chr2,start:300,end:382,featname:region4}"},
		gqltest.ReadTable(val))

	bcPath := "./testdata/small.bincounts.tsv"
	gqltest.Eval(t, fmt.Sprintf("BC := read(`%s`)", bcPath), env)
	if false {
		val = gqltest.Eval(t, "join({bed:BED, bc:BC}, bed.chrom==bc.chrom && bed.start < bc.end && bed.end > bc.start)", env)
		assert.Equal(t,
			[]string{
				"{chrom:chr1,start:5,end:85,featname:region1}",
				"{chrom:chr1,start:100,end:180,featname:region2}",
				"{chrom:chr2,start:300,end:382,featname:region3}"},
			gqltest.ReadTable(val))
	}
}

func TestBED3(t *testing.T) {
	bedPath := "./testdata/test4.bed"
	env := gqltest.NewSession()
	gqltest.Eval(t, fmt.Sprintf("BED := read(`%s`)", bedPath), env)
	bcPath := "./testdata/small.bincounts.tsv"
	gqltest.Eval(t, fmt.Sprintf("BC := read(`%s`)", bcPath), env)
	val := gqltest.Eval(t, "BC | joinbed(BED)", env)
	assert.Equal(t,
		[]string{
			"{chrom:chr1,start:0,end:100000,gc:2014,count:9527,length:100000,density:0.09527}",
			"{chrom:chr1,start:2,end:9999,gc:2014,count:9527,length:9997,density:0.0952701}",
			"{chrom:chr1,start:300000,end:400000,gc:0,count:0,length:100000,density:0}",
			"{chrom:chr2,start:0,end:100000,gc:72880,count:89592,length:100000,density:0.89592}"},
		gqltest.ReadTable(val))

	val = gqltest.Eval(t, "BC | joinbed(BED, map:=|r,feat|{r.chrom,r.start,feat.featname})", env)
	assert.Equal(t,
		[]string{"{chrom:chr1,start:0,featname:region1}",
			"{chrom:chr1,start:0,featname:region2}",
			"{chrom:chr1,start:2,featname:region1}",
			"{chrom:chr1,start:2,featname:region2}",
			"{chrom:chr1,start:300000,featname:region3}",
			"{chrom:chr2,start:0,featname:region4}"},
		gqltest.ReadTable(val))
}

// Test reading a BED file with three columns (no featname).
func TestBEDNoFeature(t *testing.T) {
	bedPath := "./testdata/test3.bed"
	env := gqltest.NewSession()
	assert.Equal(t,
		[]string{
			"{chrom:chr1,start:5,end:85,featname:NA}",
			"{chrom:chr1,start:100,end:200,featname:NA}",
			"{chrom:chr1,start:300000,end:300180,featname:NA}",
			"{chrom:chr2,start:300,end:382,featname:NA}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("BED := read(`%s`)", bedPath), env)))

	bcPath := "./testdata/small.bincounts.tsv"
	gqltest.Eval(t, fmt.Sprintf("BC := read(`%s`)", bcPath), env)
	val := gqltest.Eval(t, "BC | joinbed(BED, map:=|r,feat|{r.chrom,r.start, featstart:feat.start})", env)
	assert.Equal(t,
		[]string{
			"{chrom:chr1,start:0,featstart:5}",
			"{chrom:chr1,start:0,featstart:100}",
			"{chrom:chr1,start:2,featstart:5}",
			"{chrom:chr1,start:2,featstart:100}",
			"{chrom:chr1,start:300000,featstart:300000}",
			"{chrom:chr2,start:0,featstart:300}"},
		gqltest.ReadTable(val))
}

// Overwrite a .tsv file with different contents.
func TestRepeatedReadsWithContentsChange(t *testing.T) {
	ctx := context.Background()
	env := gqltest.NewSession()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	dataPath := file.Join(tmpDir, "test.tsv")
	require.NoError(t, file.WriteFile(ctx, dataPath, []byte(`
col0	col1
key0	10
key0	11
`)))
	table := gqltest.Eval(t, fmt.Sprintf("read(`%s`)", dataPath), env)
	assert.Equal(t,
		[]string{
			"{col0:key0,col1:10}",
			"{col0:key0,col1:11}"},
		gqltest.ReadTable(table))

	assert.Equal(t,
		[]string{"{key:key0,value:21}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`) | reduce(&col0, |a,b|(a+b), map:=&col1)", dataPath), env)))

	require.NoError(t, file.WriteFile(ctx, dataPath, []byte(`
col0	col1
key1	20
key1	21
`)))
	assert.Equal(t,
		[]string{
			"{col0:key1,col1:20}",
			"{col0:key1,col1:21}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`)", dataPath), env)))

	assert.Equal(t,
		[]string{"{key:key1,value:41}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`) | reduce(&col0, |a,b|(a+b), map:=&col1)", dataPath), env)))
}

func TestReadTSVCustomExtension(t *testing.T) {
	ctx := vcontext.Background()
	env := gqltest.NewSession()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	dataPath := file.Join(tmpDir, "testtest")
	require.NoError(t, file.WriteFile(ctx, dataPath, []byte(`
col0	col1
key0	val0
key1	val1
`)))
	table := gqltest.Eval(t, fmt.Sprintf("read(`%s`, type:=`tsv`)", dataPath), env)
	assert.Equal(t,
		[]string{
			"{col0:key0,col1:val0}",
			"{col0:key1,col1:val1}"},
		gqltest.ReadTable(table))
}

func TestDistributeTSVCustomExtension(t *testing.T) {
	ctx := context.Background()
	env := gqltest.NewSession()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	dataPath := file.Join(tmpDir, "testtest")
	require.NoError(t, file.WriteFile(ctx, dataPath, []byte(`
col0	col1
key0	val0
key1	val1
`)))
	table := gqltest.Eval(t, fmt.Sprintf("read(`%s`, type:=`tsv`) | map(_, shards := 5)", dataPath), env)
	assert.Equal(t,
		[]string{
			"{col0:key0,col1:val0}",
			"{col0:key1,col1:val1}"},
		gqltest.ReadTable(table))
}

func TestReadTSVWithComments(t *testing.T) {
	ctx := context.Background()
	env := gqltest.NewSession()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	dataPath := file.Join(tmpDir, "test.tsv")
	require.NoError(t, file.WriteFile(ctx, dataPath, []byte(`# blah blah
col0	col1
# blah blah
key0	val0
# blah blah
key1	val1
`)))
	table := gqltest.Eval(t, fmt.Sprintf("read(`%s`)", dataPath), env)
	assert.Equal(t,
		[]string{
			"{col0:key0,col1:val0}",
			"{col0:key1,col1:val1}"},
		gqltest.ReadTable(table))
}

func TestReadTidyTSV(t *testing.T) {
	dataPath := "./testdata/data.tsv"
	env := gqltest.NewSession()
	table := gqltest.Eval(t, fmt.Sprintf("T0 := read(`%s`)", dataPath), env)
	assert.Equal(
		t,
		[]string{
			"{A:1,B:/a,C:e1,D:12,E:xx}",
			"{A:2,B:s3://a,C:e1,D:12,E:xx}",
			"{A:NA,B:s3://a,C:NA,D:NA,E:xx}"},
		gqltest.ReadTable(table))
	assert.Equal(
		t,
		"tsv",
		printValueLong(gqltest.Eval(t, "table_attrs(T0).name", env)))
	assert.Regexp(
		t,
		"/data.tsv$",
		printValueLong(gqltest.Eval(t, "table_attrs(T0).path", env)))
}

func TestReadEmptyTSV1(t *testing.T) {
	dataPath := "./testdata/conta.tsv"
	env := gqltest.NewSession()
	assert.Equal(
		t,
		[]string{"{sample:P004400,conta_call:false,cf:0,sum_log_lr:0,avg_log_lr:0,snps:813298,depth:45,pos_lr_all:0,pos_lr_x:NA,pos_lr_chr_cv:NA,y_count:0.2531,pregnancy:NA,excluded_regions:10,error_rate:3.28e-05,T>A:3.03e-05,G>A:2.12e-05,C>A:7e-05,A>T:2.9e-05,G>T:6.78e-05,C>T:2.1e-05,A>G:2.5e-05,T>G:2.55e-05,C>G:2.91e-05,A>C:2.5e-05,T>C:2.21e-05,G>C:2.78e-05}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`)", dataPath), env)))
}

func TestReadEmptyTSVWithDict(t *testing.T) {
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	env := gqltest.NewSession()
	dataPath := filepath.Join(tmpDir, "empty.tsv")
	gqltest.Eval(t, fmt.Sprintf("table() | write(`%s`)", dataPath), env)
	assert.Equal(t, []string{}, gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`)", dataPath), env)))
}

func TestReadEmptyTSVWithoutDict(t *testing.T) {
	ctx := context.Background()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	env := gqltest.NewSession()

	testWithContents := func(data string) {
		dataPath := filepath.Join(tmpDir, "empty.tsv")
		require.NoError(t, file.WriteFile(ctx, dataPath, []byte(data)))
		assert.Equal(t, []string{}, gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`)", dataPath), env)))
	}
	testWithContents("")
	testWithContents("\n")
	testWithContents("\n\n")
}

func TestBTSV(t *testing.T) {
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	tmpPath := filepath.Join(tmpDir, "btsvtest.btsv")
	srcPath := "./testdata/data2.tsv"
	env := gqltest.NewSession()
	val := gqltest.Eval(t, fmt.Sprintf("T0 := read(`%s`)", srcPath), env)
	gqltest.Eval(t, fmt.Sprintf("write(T0, `%s`)", tmpPath), env)
	val2 := gqltest.Eval(t, fmt.Sprintf("read(`%s`)", tmpPath), env)
	assert.Equal(t, 3, val2.Table(nil).Len(context.Background(), gql.Exact))
	assert.Equal(t,
		gqltest.ReadTable(val),
		gqltest.ReadTable(val2))
}

func TestBTSVScalarRows(t *testing.T) {
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	tmpPath := filepath.Join(tmpDir, "scalarrows.btsv")
	env := gqltest.NewSession()
	gqltest.Eval(t, fmt.Sprintf("table(1,2,3) | write(`%s`)", tmpPath), env)
	assert.Equal(t,
		[]string{"1", "2", "3"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`)", tmpPath), env)))
}

func TestBTSVNestedCol(t *testing.T) {
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	tmpPath := filepath.Join(tmpDir, "nestedcol.btsv")
	env := gqltest.NewSession()
	// gqltest.Eval(t, fmt.Sprintf("table({f0:{a:1,b:2}, c:3}) | write(`%s`)", tmpPath), env)
	gqltest.Eval(t, fmt.Sprintf("table({c0:{a:1,b:2}, c:3}, {c1:{a:11,b:12}, c:13}) | write(`%s`)", tmpPath), env)
	assert.Equal(t,
		[]string{"{c0:{a:1,b:2},c:3}", "{c1:{a:11,b:12},c:13}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`)", tmpPath), env)))
}

func TestReadTSV(t *testing.T) {
	dataPath := "./testdata/data2.tsv"
	env := gqltest.NewSession()
	val := gqltest.Eval(t, fmt.Sprintf("read(`%s`)", dataPath), env)
	assert.Equal(t,
		[]string{
			"{A:1,B:/a,C:e1,D:1.2,E:x,F:2017-05-18 16:01:42.893 -0700 PST,G:2017-05-18}",
			"{A:2,B:s3://a,C:e1,D:1000,E:y,F:2017-01-12T12:03:44Z,G:2017-01-12}",
			"{A:NA,B:s3://a,C:NA,D:NA,E:z,F:NA,G:NA}"},
		gqltest.ReadTable(val))
}

func TestWriteTSV(t *testing.T) {
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	tmpPath := filepath.Join(tmpDir, "writetsv.tsv")

	dataPath := "./testdata/data.tsv"
	env := gqltest.NewSession()
	gqltest.Eval(t, fmt.Sprintf("write(read(`%s`), `%s`)", dataPath, tmpPath), env)

	assert.Equal(t,
		[]string{
			"{A:1,B:/a,C:e1,D:12,E:xx}",
			"{A:2,B:s3://a,C:e1,D:12,E:xx}",
			"{A:NA,B:s3://a,C:NA,D:NA,E:xx}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`)", tmpPath), env)))
}

func TestWriteBED(t *testing.T) {
	ctx := context.Background()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	env := gqltest.NewSession()

	// 3-column BED
	tmpPath := filepath.Join(tmpDir, "writetsv.bed")
	gqltest.Eval(t, fmt.Sprintf("table({chrom:`chr1`,start:10,end:20},{chrom:`chr2`,start:11,end:21}) | write(`%s`)", tmpPath), env)
	data, err := file.ReadFile(ctx, tmpPath)
	assert.NoError(t, err)
	assert.Equal(t, string(data), `chr1	10	20
chr2	11	21
`)

	// 4-column BED
	tmpPath = filepath.Join(tmpDir, "writetsv2.bed")
	gqltest.Eval(t, fmt.Sprintf("table({chrom:`chr1`,start:10,end:20,feat:`xx`},{chrom:`chr2`,start:11,end:21,feat:`yy`}) | write(`%s`)", tmpPath), env)
	data, err = file.ReadFile(ctx, tmpPath)
	assert.NoError(t, err)
	assert.Equal(t, string(data), `chr1	10	20	xx
chr2	11	21	yy
`)
}

func TestWriteFormat(t *testing.T) {
	ctx := context.Background()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	tmpPath := filepath.Join(tmpDir, "test.btsv")
	dataPath := "./testdata/data.tsv"
	env := gqltest.NewSession()
	gqltest.Eval(t, fmt.Sprintf("write(read(`%s`), `%s`, type:=`tsv`)", dataPath, tmpPath), env)

	// Make sure the file is a tsv, not btsv, by checking its first line.
	data, err := file.ReadFile(ctx, tmpPath)
	assert.NoError(t, err)
	assert.Equal(t, strings.Split(string(data), "\n")[0], "A\tB\tC\tD\tE")

	// Read the file back as tsv.
	gqltest.Eval(t, fmt.Sprintf("write(read(`%s`), `%s`, type:=`tsv`)", dataPath, tmpPath), env)
	assert.Equal(t,
		[]string{
			"{A:1,B:/a,C:e1,D:12,E:xx}",
			"{A:2,B:s3://a,C:e1,D:12,E:xx}",
			"{A:NA,B:s3://a,C:NA,D:NA,E:xx}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`,type:=`tsv`)", tmpPath), env)))

	// Don't write a data dictionary.
	tmpPath = filepath.Join(tmpDir, "test_without_dict.tsv")
	gqltest.Eval(t, fmt.Sprintf("write(read(`%s`), `%s`)", dataPath, tmpPath), env)

	// Don't write a data dictionary.
	tmpPath = filepath.Join(tmpDir, "test_with_dict.tsv")
	gqltest.Eval(t, fmt.Sprintf("write(read(`%s`), `%s`)", dataPath, tmpPath), env)
}

func TestWriteOverwrite(t *testing.T) {
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	tmpPath := filepath.Join(tmpDir, "writetsv.tsv")
	env := gqltest.NewSession()
	gqltest.Eval(t, fmt.Sprintf("write(table({A:100,B:200}), `%s`)", tmpPath), env)

	old := gql.TestSetOverwriteFiles(true)
	defer gql.TestSetOverwriteFiles(old)
	gqltest.Eval(t, fmt.Sprintf("write(table({A:10,B:20},{A:20,B:30}), `%s`)", tmpPath), env)
	assert.Equal(t,
		[]string{"{A:10,B:20}", "{A:20,B:30}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`)", tmpPath), env)))

	old = gql.TestSetOverwriteFiles(false)
	defer gql.TestSetOverwriteFiles(old)
	gqltest.Eval(t, fmt.Sprintf("write(table({A:11,B:21}), `%s`)", tmpPath), env)
	assert.Equal(t,
		[]string{"{A:10,B:20}", "{A:20,B:30}"},
		gqltest.ReadTable(gqltest.Eval(t, fmt.Sprintf("read(`%s`)", tmpPath), env)))
}

func TestWriteWritePasses(t *testing.T) {
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	tmpPath := filepath.Join(tmpDir, "writetsv.tsv")
	env := gqltest.NewSession()
	gql.TestClearCache()

	ctx := context.Background()
	table := gqltest.Eval(t, "table({A:10,B:20},{A:20,B:30})", env)
	// TODO(saito) This test uses to check the number of passes a Write has
	// performed.  Now that info is removed, the ttest writes w/o any validation.
	// Reinstate that check.
	gql.TSVFileHandler().Write(ctx, tmpPath, &gql.ASTUnknown{}, table.Table(nil), 1, true)

	// Just reordering the columns shouldn't trigger two pass writes.
	table = gqltest.Eval(t, "table({A:10,B:20},{B:30, A:20})", env)
	gql.TSVFileHandler().Write(ctx, tmpPath, &gql.ASTUnknown{}, table.Table(nil), 1, true)

	// The two rows have different layouts, so the table must first be dumped to BTSV.
	table = gqltest.Eval(t, "table({A:10,B:20},{B:30, C:20})", env)
	gql.TSVFileHandler().Write(ctx, tmpPath, &gql.ASTUnknown{}, table.Table(nil), 1, true)
}

func TestFilterScalar(t *testing.T) {
	env := gqltest.NewSession()
	val := gqltest.Eval(t, "filter(table(10,11,15,30,32), _%5==0)", env)
	assert.Equal(t, []string{"10", "15", "30"}, gqltest.ReadTable(val))
}

func TestFilterScalarWithRowVar(t *testing.T) {
	env := gqltest.NewSession()
	val := gqltest.Eval(t, "filter(table(10,11,15,30,32), bbb%5==0, row:=bbb)", env)
	assert.Equal(t, []string{"10", "15", "30"}, gqltest.ReadTable(val))
}

func TestFilterWithCustomVariable(t *testing.T) {
	t.Skip("not ready yet")
	env := gqltest.NewSession()
	val := gqltest.Eval(t, "filter(table(10,11,15,30,32), xxx%5==0, row:=xxx)", env)
	assert.Equal(t, []string{"10", "15", "30"}, gqltest.ReadTable(val))
}

func TestTSV1(t *testing.T) {
	//gql.RegisterTSVPathRegexp("tsv1", regexp.MustCompile("tsv1.*.tsv$"))
	path := "./testdata/tsv1_0.tsv"
	env := gqltest.NewSession()
	val := gqltest.Eval(t, fmt.Sprintf("read(`%s`)", path), env)
	assert.Equal(
		t,
		[]string{
			"{A:NA,B:1234,C:2}",
			"{A:col0,B:NA,C:3.1}",
			"{A:col1,B:456,C:0.34}"},
		gqltest.ReadTable(val))
}

func TestCount(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, int64(4), gqltest.Eval(t, "count(table(1,2,3,5))", env).Int(nil))
}

func TestCountFilterTable0(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, gqltest.Eval(t, "table(1,2,3,4) | filter(_%2==0) | count()", env).Int(nil), int64(2))
}

func TestCountFilterTable1(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, gqltest.Eval(t, "table(1,2,3,4) | filter(_%2==0, shards:=2) | count()", env).Int(nil), int64(2))
}

func TestCountFlattenedTable0(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, gqltest.Eval(t, "table(table(1,2,3), table(4)) | flatten() | count()", env).Int(nil), int64(4))
}

func TestCountFlattenedTable1(t *testing.T) {
	env := gqltest.NewSession()
	v := gqltest.Eval(t, fmt.Sprintf("table(table(1,2,3), table(4)) | flatten(subshard:=true) | count()"), env)
	assert.Equal(t, v.Int(nil), int64(4))
}

func TestPick(t *testing.T) {
	env := gqltest.NewSession()
	val := gqltest.Eval(t, "pick(table(10,11,15,30,32), _%5==1)", env)
	assert.Equal(t, "11", printValueLong(val))

	assert.Equal(t, "15", printValueLong(gqltest.Eval(t, "table(10,11,15,30,32) | pick(_==15)", env)))

	val = gqltest.Eval(t, "pick(table(10,11,15,30,32), false)", env)
	assert.Equal(t, "NA", printValueLong(val))

	dataPath := "./testdata/data.tsv"
	assert.Equal(t,
		"{A:2,B:s3://a,C:e1,D:12,E:xx}",
		gqltest.Eval(t, fmt.Sprintf("read(`%s`) | pick(&A==2)", dataPath), env).String())
	assert.Equal(t,
		"NA",
		gqltest.Eval(t, fmt.Sprintf("read(`%s`) | pick(&A==100)", dataPath), env).String())
}

func TestCompositeLiteral(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, "A := {f0:1,f1:`foo`,f2:3.0}", env)
	val := gqltest.Eval(t, "table(A, {A.f0+1, A.f1+`1`, A.f2+1.0})", env)
	assert.Equal(t,
		[]string{"{f0:1,f1:foo,f2:3}", "{f0:2,f1:foo1,f2:4}"},
		gqltest.ReadTable(val))
}

func TestFilterTable(t *testing.T) {
	dataPath := "./testdata/data.tsv"

	env := gqltest.NewSession()
	// table := gqltest.Eval(t, fmt.Sprintf("filter(read(`%s`), A<2)", dataPath), env)
	table := gqltest.Eval(t, fmt.Sprintf("read(`%s`) | filter(&A<2)", dataPath), env)
	assert.Equal(t,
		[]string{
			`{A:1,B:/a,C:e1,D:12,E:xx}`,
		},
		gqltest.ReadTable(table))
	//	assert.Equal(t, "table:data[struct:data{A:A,B:B,C:C,D:D,E:E}]",
	//		table.Type().String())
	table = gqltest.Eval(t, fmt.Sprintf("read(`%s`) | filter(true) | map({FA:&A, FB:&B})", dataPath), env)
	assert.Equal(t,
		[]string{
			`{FA:1,FB:/a}`,
			`{FA:2,FB:s3://a}`,
			`{FA:NA,FB:s3://a}`,
		},
		gqltest.ReadTable(table))
	//	assert.Equal(t, "table:maptable[struct:anon{FA:A,FB:B}]", table.Type().String())
}

func TestMinN(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, "T := table(10, 5, 3, 4)", env)
	val := gqltest.Eval(t, "minn(T, -1, _)", env)
	assert.Equal(t, []string{"3", "4", "5", "10"}, gqltest.ReadTable(val))
	assert.Equal(t, []string{"3", "4", "5", "10"}, gqltest.ReadTable(val))
}

func TestSortTable(t *testing.T) {
	dataPath := "./testdata/data.tsv"

	// Sort in descending A order
	env := gqltest.NewSession()
	table := gqltest.Eval(t, fmt.Sprintf("sort(read(`%s`), {-&A})", dataPath), env)
	assert.Equal(t,
		[]string{
			"{A:NA,B:s3://a,C:NA,D:NA,E:xx}",
			"{A:2,B:s3://a,C:e1,D:12,E:xx}",
			"{A:1,B:/a,C:e1,D:12,E:xx}",
		},
		gqltest.ReadTable(table))

	// Sort in descending A, then pick only the B column as "RR".
	table = gqltest.Eval(t, fmt.Sprintf("read(`%s`) | sort({-&A}) | map({RR:&B})", dataPath), env)
	assert.Equal(t,
		[]string{
			`{RR:s3://a}`,
			`{RR:s3://a}`,
			`{RR:/a}`,
		},
		gqltest.ReadTable(table))

	// Sort in ascending A order
	table = gqltest.Eval(t, fmt.Sprintf("read(`%s`) | sort({&A})", dataPath), env)
	assert.Equal(t,
		[]string{
			"{A:1,B:/a,C:e1,D:12,E:xx}",
			"{A:2,B:s3://a,C:e1,D:12,E:xx}",
			"{A:NA,B:s3://a,C:NA,D:NA,E:xx}",
		},
		gqltest.ReadTable(table))
}

// Check that NA can be casted to any data type.
func TestNull(t *testing.T) {
	var na gql.Value = gql.Null
	assert.Equal(t,
		[]string{},
		gqltest.ReadTable(gql.NewTable(na.Table(nil))))
	assert.Equal(t, "{}", gql.NewStruct(na.Struct(nil)).String())
}

func TestRegexp(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, "fxx", gqltest.Eval(t, `regexp_replace("foo", "o", "x")`, env).Str(nil))
	assert.Equal(t, "fxooyb", gqltest.Eval(t, `regexp_replace("foob", "(o+)", "x${1}y")`, env).Str(nil))
	assert.True(t, gqltest.Eval(t, `regexp_match("dog", "o+")`, env).Bool(nil))
	assert.False(t, gqltest.Eval(t, `regexp_match("dog", "^o+")`, env).Bool(nil))
}

func TestString(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, int64(3), gqltest.Eval(t, `string_len("foo")`, env).Int(nil))
	assert.Equal(t, int64(0), gqltest.Eval(t, `string_len("")`, env).Int(nil))

	assert.Equal(t, "do", gqltest.Eval(t, `substring("dogge", 0, 2)`, env).Str(nil))
	assert.Equal(t, "ogge", gqltest.Eval(t, `substring("dogge", 1, 10)`, env).Str(nil))
	assert.Equal(t, "gge", gqltest.Eval(t, `substring("dogge", 2)`, env).Str(nil))

	assert.Equal(t, "hello10", gqltest.Eval(t, `sprintf("hello%d", 10)`, env).Str(nil))

	assert.True(t, gqltest.Eval(t, `string_has_prefix("foo", "f")`, env).Bool(nil))
	assert.True(t, gqltest.Eval(t, `string_has_prefix("foo", "fo")`, env).Bool(nil))
	assert.False(t, gqltest.Eval(t, `string_has_prefix("foo", "g")`, env).Bool(nil))
	assert.True(t, gqltest.Eval(t, `string_has_suffix("foo", "o")`, env).Bool(nil))
	assert.True(t, gqltest.Eval(t, `string_has_suffix("foo", "oo")`, env).Bool(nil))
	assert.False(t, gqltest.Eval(t, `string_has_suffix("foo", "g")`, env).Bool(nil))

	assert.Equal(t, int64(2), gqltest.Eval(t, `string_count("good dog", "g")`, env).Int(nil))
	assert.Equal(t, int64(2), gqltest.Eval(t, `string_count("greatgreatgood", "great")`, env).Int(nil))
	assert.Equal(t, int64(3), gqltest.Eval(t, `string_count("greatgreatgrim", "gr")`, env).Int(nil))
	assert.Equal(t, int64(0), gqltest.Eval(t, `string_count("greatgreatgrim", "greatgrimfoo")`, env).Int(nil))

	assert.Equal(t, "daga", gqltest.Eval(t, `string_replace("dogo", "o", "a")`, env).Str(nil))
	assert.Equal(t, "doga", gqltest.Eval(t, `string_replace("dogoo", "oo", "a")`, env).Str(nil))
	assert.Equal(t, "dogoo", gqltest.Eval(t, `string_replace("dogoo", "x", "a")`, env).Str(nil))
}

func TestHash(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, int64(7067957609529580592), gqltest.Eval(t, `hash64("foo")`, env).Int(nil))
}

func TestRandomSamplingUsingHash(t *testing.T) {
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	path := filepath.Join(tmpDir, "test.btsv")
	r := rand.New(rand.NewSource(0))
	env := gqltest.NewSession()
	const nRow = 10000
	generateTestTSV(path, nRow, 1, r)

	expr := fmt.Sprintf("read(`%s`) | filter(hash64(&f0) %% 5 == 0) | count()", path)
	n := gqltest.Eval(t, expr, env).Int(nil)
	assert.Truef(t, n >= nRow/5-30 && n <= nRow/5+30, "n=%d", n)

	expr = fmt.Sprintf("read(`%s`) | filter(hash64(&f0) %% 8 == 0) | count()", path)
	n = gqltest.Eval(t, expr, env).Int(nil)
	assert.Truef(t, n >= nRow/8-30 && n <= nRow/8+30, "n=%d", n)
}

func TestOptionalField(t *testing.T) {
	env := gqltest.NewSession()
	assert.Equal(t, int64(10), gqltest.Eval(t, `optionalfield({a:10,b:11}, a)`, env).Int(nil))
	assert.Equal(t, int64(11), gqltest.Eval(t, `optionalfield({a:10,b:11}, b)`, env).Int(nil))
	assert.Equal(t, int64(12), gqltest.Eval(t, `optionalfield({a:10,b:11}, c, default:=12)`, env).Int(nil))
	assert.Equal(t, gql.NullType, gqltest.Eval(t, `optionalfield({a:10,b:11}, c)`, env).Type())
}

func TestLoad(t *testing.T) {
	ctx := context.Background()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	path := filepath.Join(tmpDir, "test.gql")
	assert.NoError(t, file.WriteFile(ctx, path, []byte("zzz := 10 * 21")))
	// Nested load
	env := gqltest.NewSession()
	path2 := filepath.Join(tmpDir, "test2.gql")
	assert.NoError(t, file.WriteFile(ctx, path2, []byte(fmt.Sprintf("load `%s`; zzz*2", path))))
	assert.Equal(t, int64(420), gqltest.Eval(t, fmt.Sprintf("load `%s`", path2), env).Int(nil))
}

func TestPrintTable(t *testing.T) {
	doPrint := func(v gql.Value) string {
		out := termutil.NewBufferPrinter()
		args := gql.PrintArgs{Out: out, Mode: gql.PrintValues}
		v.Print(context.Background(), args)
		return out.String()
	}

	env := gqltest.NewSession()
	gqltest.Eval(t, "t0 := table(0,1,2,3,4,5)", env)
	gqltest.Eval(t, "t1 := t0 | map(_+1000, _+2000,_+3000,_+4000);", env)

	// Pretty printing
	tbl := gqltest.Eval(t, `table(1,5)`, env)
	assert.Equal(t, doPrint(tbl),
		`║ #║ _║
├──┼──┤
│ 0│ 1│
│ 1│ 5│
`)

	// Pretty print nested tables, where subtables are short.
	// The subtables printed inline in a compact form
	tbl = gqltest.Eval(t, `table(table({a:1,b:2}), table({a:3,b:4}))`, env)
	assert.Equal(t, doPrint(tbl),
		`║ #║           _║
├──┼────────────┤
│ 0│ [{a:1,b:2}]│
│ 1│ [{a:3,b:4}]│
`)

	// Print nested long subtables.
	tbl = gqltest.Eval(t, `table(t1, t1)`, env)
	assert.Equal(t, doPrint(tbl),
		`║ #║         _║
├──┼──────────┤
│ 0│ [omitted]│
│ 1│ [omitted]│
`)
}

// Run gql.Session.Eval* in parallel. This should be run in -race mode.
func TestParallelEval(t *testing.T) {
	env := gqltest.NewSession()
	dataPath := "./testdata/data.tsv"
	const n = 300
	traverse.Each(n, func(i int) error {
		expr := fmt.Sprintf(`v%d := (read("%s") | count()) + %d`, i, dataPath, i)
		gqltest.Eval(t, expr, env)
		return nil
	}) // nolint: errcheck

	for i := 0; i < n; i++ {
		assert.Equal(t, gqltest.Eval(t, fmt.Sprintf("v%d", i), env).Int(nil), int64(3+i))
	}
}

func generateTestTSV(path string, rows, cols int, r *rand.Rand) {
	ctx := context.Background()
	w := gql.NewBTSVShardWriter(ctx, path, 0, 1, gql.TableAttrs{})
	fields := make([]gql.StructField, cols)
	for j := range fields {
		fields[j].Name = symbol.Intern(fmt.Sprintf("f%d", j))
	}
	for i := 0; i < rows; i++ {
		for j := range fields {
			fields[j].Value = gql.NewInt(int64(i*cols + j))
		}
		w.Append(gql.NewStruct(gql.NewSimpleStruct(fields...)))
	}
	w.Close(ctx)
}

func BenchmarkMap(b *testing.B) {
	b.StopTimer()
	tmpDir, cleanup := testutil.TempDir(b, "", "")
	defer cleanup()
	path := filepath.Join(tmpDir, "test.btsv")
	r := rand.New(rand.NewSource(0))
	generateTestTSV(path, 10000, 4, r)
	env := gqltest.NewSession()
	expr := fmt.Sprintf("read(`%s`) | map({$f0, $f1*$f2}, filter:=$f0%%3==0) | pick(false)", path)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		gqltest.Eval(b, expr, env)
	}
}

func BenchmarkReadSmallBTSV(b *testing.B) {
	b.StopTimer()
	tmpDir, cleanup := testutil.TempDir(b, "", "")
	defer cleanup()
	path := filepath.Join(tmpDir, "test.btsv")
	r := rand.New(rand.NewSource(0))
	generateTestTSV(path, 10000, 4, r)
	env := gqltest.NewSession()
	expr := fmt.Sprintf("read(`%s`) | pick(false)", path)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		gqltest.Eval(b, expr, env)
	}
}

var btsvPathFlag = flag.String("btsv-path", "", "BTSV file for benchmarking")

func BenchmarkReadLargeBTSV(b *testing.B) {
	if *btsvPathFlag == "" {
		b.Skip("skipped")
	}
	b.StopTimer()
	env := gqltest.NewSession()
	expr := fmt.Sprintf("read(`%s`) | pick(false)", *btsvPathFlag)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		gqltest.Eval(b, expr, env)
	}
}

var (
	prioPathFlag = flag.String("prio-path", "/scratch-nvme/prio/P0097R0.prio", "Prio file for benchmarking")
	// The original file is at s3://grail-sgross/too/bloodhound/leukemia_cfdna_targets.tsv.
	bedPathFlag = flag.String("bed-path", "/scratch-nvme/bed/leukemia_cfdna_targets.tsv", "BED file for benchmarking")
)

// On ubuntu02.mpk, on 2018-10-22:
//
// BenchmarkFilterFragments-56    	       1	238402522041 ns/op
//
func BenchmarkFilterFragments(b *testing.B) {
	if *prioPathFlag == "" {
		b.Skip("skipped")
	}
	env := gqltest.NewSession()
	gqltest.Eval(b, fmt.Sprintf(`prio := read("%s")`, *prioPathFlag), env)
	gqltest.Eval(b, fmt.Sprintf(`bed := read("%s", type:="bed")`, *bedPathFlag), env)

	gqltest.Eval(b, `
methylation_fraction := func(frag) {
  methylated_sites := frag.methylation_states | filter($value == 2) | count();
  float(methylated_sites) / float(count(frag.methylation_states));
};

process_prio := func(prio) {
  prio
    | joinbed(bed, chrom:=$reference, length:=$length)
    | firstn(5000)
    | filter(count($methylation_states) >= 5 && methylation_fraction(_) > 0.9)
    | count();
};`, env)

	for i := 0; i < b.N; i++ {
		val := gqltest.Eval(b, `process_prio(prio)`, env)
		b.Logf("val (iter %d): %v", i, val.String())
	}
}

func benchAddCol(fields *[]gql.StructField, name string, val interface{}) {
	f := gql.StructField{Name: symbol.Intern(name)}
	if v, ok := val.(int); ok {
		f.Value = gql.NewInt(int64(v))
	} else if v, ok := val.(string); ok {
		f.Value = gql.NewString(v)
	} else {
		panic(val)
	}
	*fields = append(*fields, f)
}

func benchNewDenseTable() gql.Table {
	const nCol = 50
	var (
		cols []gql.StructField
		rows []gql.Value
	)
	benchAddCol(&cols, "key", "somestring")
	for i := 0; i < nCol; i++ {
		benchAddCol(&cols, fmt.Sprintf("col%02d", i), i+100)
	}
	for i := 0; i < 300000; i++ {
		rows = append(rows, gql.NewStruct(gql.NewSimpleStruct(cols...)))
	}
	return gql.NewSimpleTable(rows, hash.String("test"), gql.TableAttrs{})
}

func benchNewSparseTable() gql.Table {
	const nCol = 50
	var (
		r    = rand.New(rand.NewSource(0))
		rows []gql.Value
	)
	for i := 0; i < 300000; i++ {
		var cols []gql.StructField
		benchAddCol(&cols, "key", "somestring")
		for i := 0; i < nCol; i++ {
			if r.Float32() < 0.1 {
				benchAddCol(&cols, fmt.Sprintf("col%02d", i), i+100)
			}
		}
		rows = append(rows, gql.NewStruct(gql.NewSimpleStruct(cols...)))
	}
	return gql.NewSimpleTable(rows, hash.String("test"), gql.TableAttrs{})
}

func benchClearData(b *testing.B, dataDir string) {
	os.RemoveAll(dataDir) // nolint: errcheck
	gql.TestClearCache()
}

func BenchmarkWriteDenseTable(b *testing.B) {
	b.StopTimer()
	ctx := context.Background()
	_ = gqltest.NewSession()
	tempDir, cleanup := testutil.TempDir(b, "", "")
	defer cleanup()
	table := benchNewDenseTable()
	tempPath := tempDir + "/test.tsv"
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		benchClearData(b, tempPath)
		gql.TSVFileHandler().Write(ctx, tempPath, &gql.ASTUnknown{}, table, 1, false)
	}
}

func BenchmarkWriteColumnarDenseTable(b *testing.B) {
	b.StopTimer()
	ctx := context.Background()
	_ = gqltest.NewSession()
	tempDir, cleanup := testutil.TempDir(b, "", "")
	defer cleanup()
	table := benchNewDenseTable()
	tempPath := tempDir + "/testcol"
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		benchClearData(b, tempPath)
		gql.WriteColumnarTSV(ctx, table, tempPath+"/test{.Name}.tsv", false, false)
	}
}

func BenchmarkWriteSparseTable(b *testing.B) {
	b.StopTimer()
	ctx := context.Background()
	_ = gqltest.NewSession()
	tempDir, cleanup := testutil.TempDir(b, "", "")
	defer cleanup()
	table := benchNewSparseTable()
	tempPath := tempDir + "/test.tsv"
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		benchClearData(b, tempPath)
		gql.TSVFileHandler().Write(ctx, tempPath, &gql.ASTUnknown{}, table, 1, false)
	}
}

func BenchmarkWriteColumnarSparseTable(b *testing.B) {
	b.StopTimer()
	ctx := context.Background()
	_ = gqltest.NewSession()
	tempDir, cleanup := testutil.TempDir(b, "", "")
	defer cleanup()
	table := benchNewSparseTable()
	tempPath := tempDir + "/testcol"
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		benchClearData(b, tempPath)
		gql.WriteColumnarTSV(ctx, table, tempPath+"/test{.Name}.tsv", false, false)
	}
}

func BenchmarkStructAccesses(b *testing.B) {
	b.StopTimer()
	_ = gqltest.NewSession()

	var cols []gql.StructField
	benchAddCol(&cols, "key0", 10)
	benchAddCol(&cols, "key1", 20)
	benchAddCol(&cols, "key3", 30)
	s := gql.NewSimpleStruct(cols...)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Len()
		for j := 0; j < 3; j++ {
			_ = s.Field(j)
		}
	}
}

func TestMain(m *testing.M) {
	shutdown := grail.Init()
	status := m.Run()
	shutdown()
	os.Exit(status)
}

package gql_test

import (
	"testing"

	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/gql/gqltest"
)

func TestSpreadGather(t *testing.T) {
	env := gqltest.NewSession()

	t1 := []string{
		"{t2:1,tiers:t1,reads:0}",
		"{t2:1,tiers:t3,reads:2}",
		"{t2:11,tiers:t1,reads:10}",
		"{t2:11,tiers:t3,reads:12}",
		"{t2:21,tiers:t1,reads:20}",
		"{t2:21,tiers:t3,reads:22}",
	}
	gqltest.Eval(t, `t0 := table(
		{t1:0, t2:1, t3:2},
		{t1:10, t2:11, t3:12},
		{t1:20, t2:21, t3:22})`, env)
	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `t1 := t0 | gather("t1", "t3", key:="tiers", value:="reads")`, env)),
		t1)
	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `t2 := t1 | spread(key:="tiers", value:="reads")`, env)),
		[]string{
			"{t2:1,t1:0}",
			"{t2:1,t3:2}",
			"{t2:11,t1:10}",
			"{t2:11,t3:12}",
			"{t2:21,t1:20}",
			"{t2:21,t3:22}",
		})

	// (spread|collapse)/gather are complements of each other
	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `t3 := t2 | collapse("t1", "t3")`, env)),
		[]string{
			"{t2:1,t1:0,t3:2}",
			"{t2:11,t1:10,t3:12}",
			"{t2:21,t1:20,t3:22}",
		})
	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `t3 | gather("t1", "t3", key:="tiers", value:="reads")`, env)),
		t1)

}

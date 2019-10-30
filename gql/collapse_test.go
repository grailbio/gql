package gql_test

import (
	"testing"

	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/gql/gqltest"
)

func TestCollapse(t *testing.T) {
	env := gqltest.NewSession()
	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table() | collapse("t2")`, env)),
		[]string{})

	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table({t1:0, t2:1, t3:2}) | collapse("t2")`, env)),
		[]string{"{t1:0,t3:2,t2:1}"})

	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
				{t1:0, t2:1, t3:2},
				{t1:10, t2:11, t3:12}) | collapse("t2")`, env)),
		[]string{
			"{t1:0,t3:2,t2:1}",
			"{t1:10,t3:12,t2:11}"})

	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
						{t1:0, t2:1, t3:2},
						{t1:10, t2:11, t3:12},
						{t1:20, t2:21, t3:22}) | collapse("t2")`, env)),
		[]string{
			"{t1:0,t3:2,t2:1}",
			"{t1:10,t3:12,t2:11}",
			"{t1:20,t3:22,t2:21}"})

	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
								{t1:0, t2:1, t3:2},
								{t1:10, t2:11, t3:12},
								{t1:100, t2:11, t3:12},
								{t1:101, t2:11, t3:12},
								{t1:20, t2:21, t3:22}) | collapse("t2")`, env)),
		[]string{
			"{t1:0,t3:2,t2:1}",
			"{t1:10,t3:12,t2:11}",
			"{t1:100,t3:12,t2:11}",
			"{t1:101,t3:12,t2:11}",
			"{t1:20,t3:22,t2:21}"})

	// Test duplicates
	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
								{t1:0, t2:1, t3:2},
								{t1:10, t2:11, t3:12},
								{t1:10, t2:11, t3:12},
								{t1:10, t2:11, t3:12},
								{t1:20, t2:21, t3:22}) | collapse("t2")`, env)),
		[]string{
			"{t1:0,t3:2,t2:1}",
			"{t1:10,t3:12,t2:11}",
			"{t1:10,t3:12,t2:11}",
			"{t1:10,t3:12,t2:11}",
			"{t1:20,t3:22,t2:21}"})

	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
							{t1:0, t2:1, t3:2},
							{t1:10, t2:11},
							{t1:10, t3:12},
							{t1:10, t2:11, t3:12},
							{t1:20, t2:21, t3:22}) | collapse("t2", "t3")`, env)),
		[]string{
			"{t1:0,t2:1,t3:2}",
			"{t1:10,t2:11,t3:12}",
			"{t1:10,t2:11,t3:12}",
			"{t1:20,t2:21,t3:22}"})

	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
							{t1:0, t2:1, t3:2},
							{t1:10, t2:11},
							{t1:10, t3:12},
							{t1:10, t2:11, t3:12},
							{t1:20, t2:21, t3:22}) | collapse("t3", "t2")`, env)),
		[]string{
			"{t1:0,t3:2,t2:1}",
			"{t1:10,t3:12,t2:11}",
			"{t1:10,t3:12,t2:11}",
			"{t1:20,t3:22,t2:21}"})

	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
							{t1:0},
							{t2:11},
							{t3:12},
							{t1:20, t2:21, t3:22}) | collapse("t1", "t2", "t3")`, env)),
		[]string{
			"{t1:0,t2:11,t3:12}",
			"{t1:20,t2:21,t3:22}"})

	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
							{t1:0},
							{t2:11},
							{t3:12},
							{t1:10},
							{t2:111},
							{t3:112},
							{t1:20, t2:21, t3:22}) | collapse("t1", "t2", "t3")`, env)),
		[]string{
			"{t1:0,t2:11,t3:12}",
			"{t1:10,t2:111,t3:112}",
			"{t1:20,t2:21,t3:22}"})
	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
							{t1:0,t2:11},
							{t3:12},
							{t1:10,t2:111},
							{t3:112},
							{t1:20, t2:21, t3:22}) | collapse("t1", "t2", "t3")`, env)),
		[]string{
			"{t1:0,t2:11,t3:12}",
			"{t1:10,t2:111,t3:112}",
			"{t1:20,t2:21,t3:22}"})

	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
							{col0: "Cat", col1: 30},
							{col0: "Cat", col2: 41}) | collapse("col1", "col2")`, env)),
		[]string{"{col0:Cat,col1:30,col2:41}"})

	expect.EQ(t,
		gqltest.ReadTable(gqltest.Eval(t, `table(
							{col0: "Cat", col1: 30},
							{col0: "Cat", col1: 30, col2: 41}) | collapse("col1", "col2")`, env)),
		[]string{
			"{col0:Cat,col1:30}",
			"{col0:Cat,col1:30,col2:41}"})
}

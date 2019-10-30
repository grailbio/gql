package gql_test

import (
	"testing"

	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/testutil/h"
	"github.com/grailbio/gql/gqltest"
)

func TestParseError(t *testing.T) {
	env := gqltest.NewSession()
	expect.That(t,
		func() { gqltest.Eval(t, "10 + blah", env) },
		h.Panics(h.Regexp(`\(input\):1:5.*not found`)))

	expect.That(t,
		func() { gqltest.Eval(t, "10 + `foohah`", env) },
		h.Panics(h.Regexp(`\(input\):1:1.*invalid arg types`)))

	expect.That(t,
		func() { gqltest.Eval(t, "10 + !`foohah`", env) },
		h.Panics(h.Regexp(`\(input\):1:7.*invalid arg types`)))
}

func TestArgError(t *testing.T) {
	env := gqltest.NewSession()
	expect.That(t,
		func() { gqltest.Eval(t, `read("foo.tsv", name:="aoeu")`, env) },
		h.Panics(h.Regexp(`\(input\):1:1.*too many arguments to function.*name`)))
	expect.That(t,
		func() { gqltest.Eval(t, `read("foo.tsv", 123)`, env) },
		h.Panics(h.Regexp(`\(input\):1:1.*too many arguments to function.*123`)))
}

func TestTableError(t *testing.T) {
	env := gqltest.NewSession()
	expect.That(t,
		func() {
			gqltest.ReadTable(gqltest.Eval(t, `table(10,20,30) | map(_ + "aoeu")`, env))
		},
		h.Panics(h.Regexp(`\(input\):1:26:.*expect value of type int`)))
	expect.That(t,
		func() {
			gqltest.ReadTable(gqltest.Eval(t, `table(1,2,3) | filter(_ > "10")`, env))
		},
		h.Panics(h.Regexp(`\(input\):1:23:.*expect value of type int`)))
}

func TestSetGlobalError(t *testing.T) {
	env := gqltest.NewSession()
	gqltest.Eval(t, "foo := 20", env)
	expect.That(t,
		func() { gqltest.Eval(t, "foo := 30", env) },
		h.Panics(h.Regexp("variable 'foo' already exists")))
	expect.EQ(t, int64(20), gqltest.Eval(t, "foo", env).Int(nil))
	expect.That(t,
		func() { gqltest.Eval(t, "count := 30", env) },
		h.Panics(h.Regexp("cannot overwrite global constant 'count'")))
}

func TestChar(t *testing.T) {
	env := gqltest.NewSession()
	expect.EQ(t, 'X', gqltest.Eval(t, `'X'`, env).Char(nil))
	expect.EQ(t, '語', gqltest.Eval(t, `'語'`, env).Char(nil))
	expect.That(t,
		func() { gqltest.Eval(t, "'badchar'", env) },
		h.Panics(h.Regexp("Invalid character literal")))
}

func TestMinMaxError(t *testing.T) {
	env := gqltest.NewSession()
	expect.That(t,
		func() { gqltest.Eval(t, `max()`, env) },
		h.Panics(h.Regexp(`arg #0 is missing in call to function`)))
	expect.That(t,
		func() { gqltest.Eval(t, `min()`, env) },
		h.Panics(h.Regexp(`arg #0 is missing in call to function`)))
	expect.That(t,
		func() { gqltest.Eval(t, `max("a", 10)`, env) },
		h.Panics(h.Regexp(`invalid arg types`)))
	expect.That(t,
		func() { gqltest.Eval(t, `min("a", 10)`, env) },
		h.Panics(h.Regexp(`invalid arg types`)))
}

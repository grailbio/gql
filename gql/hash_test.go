package gql

import (
	"testing"

	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/gql/symbol"
)

func TestHashCallFrame(t *testing.T) {
	b0 := bindings{}
	b0.pushFrame1(symbol.Intern("x"), NewInt(10))
	f0 := b0.frames[len(b0.frames)-1]

	b1 := bindings{}
	b1.pushFrame1(symbol.Intern("x"), NewInt(10))
	f1 := b1.frames[len(b1.frames)-1]
	expect.EQ(t, f0.hash(), f1.hash())

	b0 = bindings{}
	b0.pushFrameN(
		[]symbol.ID{symbol.Intern("x"), symbol.Intern("y"), symbol.Intern("z")},
		[]Value{NewInt(10), NewInt(11), NewInt(12)})
	f0 = b0.frames[len(b0.frames)-1]

	b1 = bindings{}
	b1.pushFrameN(
		[]symbol.ID{symbol.Intern("x"), symbol.Intern("z"), symbol.Intern("y")},
		[]Value{NewInt(10), NewInt(12), NewInt(11)})
	f1 = b1.frames[len(b1.frames)-1]
	expect.EQ(t, f0.hash(), f1.hash())
	expect.EQ(t, f0.clone().hash(), f0.hash())
}

package gql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/grailbio/gql/symbol"
)

func TestCallFrame(t *testing.T) {
	sym0 := symbol.Intern("x")
	sym1 := symbol.Intern("y")
	sym2 := symbol.Intern("z")
	c := &callFrame{}
	_, ok := c.lookup(sym0)
	assert.False(t, ok)

	c.set(sym0, True)
	v, ok := c.lookup(sym0)
	assert.True(t, ok)
	assert.True(t, v.Bool(nil))

	c.set(sym1, NewInt(1234))
	v, ok = c.lookup(sym1)
	assert.True(t, ok)
	assert.Equal(t, int64(1234), v.Int(nil))

	c.set(sym2, NewInt(2345))
	v, ok = c.lookup(sym2)
	assert.True(t, ok)
	assert.Equal(t, int64(2345), v.Int(nil))

	v, ok = c.lookup(sym0)
	assert.True(t, ok)
	assert.True(t, v.Bool(nil))
}

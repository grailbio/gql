package symbol_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/symbol"
)

func TestIntern(t *testing.T) {
	assert.Equal(t, symbol.Intern("abc"), symbol.Intern("abc"))
	assert.False(t, symbol.Intern("abc") == symbol.Intern("cde"))
}

func TestLookup(t *testing.T) {
	for _, name := range []string{"_", "_3", "$x", "xyz"} {
		id := symbol.Intern(name)
		name2 := id.Str()
		assert.Equal(t, name, name2)
	}
}

func TestMarshal(t *testing.T) {
	id := symbol.Intern("marshaltest0")
	symbol.MarkPreInternedSymbols()

	b, err := id.MarshalBinary()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(b))

	id1 := symbol.Intern("marshaltest1")
	b1, err := id1.MarshalBinary()
	assert.NoError(t, err)
	assert.True(t, len(b1) > 9)

	var rid symbol.ID
	assert.NoError(t, rid.UnmarshalBinary(b))
	assert.Equal(t, rid, id)
	assert.NoError(t, rid.UnmarshalBinary(b1))
	assert.Equal(t, rid, id1)
}

func BenchmarkHashInterned(b *testing.B) {
	sym := symbol.Intern("abcdefghijk")
	symbol.MarkPreInternedSymbols()
	for i := 0; i < b.N; i++ {
		_ = sym.Hash()
	}
}

func BenchmarkHashNonInterned(b *testing.B) {
	sym := symbol.Intern("lmnopqrstuv")
	var h hash.Hash
	for i := 0; i < b.N; i++ {
		h = sym.Hash()
	}
	fmt.Printf("hash: %v\n", h)
}

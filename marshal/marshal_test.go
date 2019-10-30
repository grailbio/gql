package marshal_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
)

type teststruct struct {
	X int
	Y string
}

func TestBasic(t *testing.T) {
	m := marshal.NewEncoder(nil)
	require.Equal(t, 0, m.Len())
	m.PutByte(1)
	m.PutVarint(12345)
	m.PutUint64(54321)
	m.PutBytes([]byte{1, 4, 7})
	m.PutString("Hello")
	m.PutBool(true)
	m.PutBool(false)
	m.PutSymbol("test")
	m.PutSymbol("test")
	m.PutSymbol("another")
	m.PutGOB(teststruct{1234, "blah"})

	h := hash.Hash{
		0x60, 0x41, 0x5a, 0xa0, 0x8f, 0x06, 0x6e, 0xb4,
		0xfc, 0xe9, 0x51, 0xfa, 0x2a, 0xe5, 0x43, 0x47,
		0x25, 0x60, 0xc4, 0xdd, 0xd6, 0x9a, 0x88, 0x28,
		0x78, 0x0a, 0x82, 0x2c, 0xe4, 0x7b, 0x6d, 0x42}
	m.PutHash(h)

	d := marshal.NewDecoder(m.Bytes())
	require.Equal(t, byte(1), d.Byte())
	require.Equal(t, int64(12345), d.Varint())
	require.Equal(t, uint64(54321), d.Uint64())
	require.Equal(t, []byte{1, 4, 7}, d.Bytes())
	require.Equal(t, "Hello", d.String())
	require.Equal(t, true, d.Bool())
	require.Equal(t, false, d.Bool())
	require.Equal(t, "test", d.Symbol())
	require.Equal(t, "test", d.Symbol())
	require.Equal(t, "another", d.Symbol())
	v := teststruct{}
	d.GOB(&v)
	require.Equal(t, teststruct{1234, "blah"}, v)
	require.Equal(t, h, d.Hash())
	require.Equal(t, 0, d.Len())
}

// Test buffer resizing.
func doRandomTest(t *testing.T, seed int64) {
	m := marshal.NewEncoder(nil)
	expected := [][]byte{}
	r := rand.New(rand.NewSource(seed))
	const iters = 100
	for i := 0; i < iters; i++ {
		n := int(r.ExpFloat64() * 20)
		buf := make([]byte, n)
		r.Read(buf)
		expected = append(expected, buf)
		m.PutBytes(buf)
	}
	d := marshal.NewDecoder(m.Bytes())
	for len(expected) > 0 {
		var want []byte
		want, expected = expected[0], expected[1:]
		got := d.Bytes()
		if len(want) == 0 {
			require.Equalf(t, 0, len(got), "n=%d", iters-len(expected))
			continue
		}
		require.Equalf(t, want, got, "n=%d", iters-len(expected))
	}
	require.Equal(t, 0, d.Len())
}

func TestRandom(t *testing.T) {
	for i := 0; i < 1000; i++ {
		doRandomTest(t, int64(i))
	}
}

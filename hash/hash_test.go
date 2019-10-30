package hash_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/grailbio/gql/hash"
)

var (
	randomHash = hash.Hash{
		0xce, 0xce, 0x7c, 0x18, 0xdf, 0x26, 0xa8, 0x3c,
		0xfe, 0x56, 0xee, 0xd0, 0x35, 0x22, 0x8f, 0x7c,
		0x08, 0x5d, 0xf9, 0xc9, 0x80, 0x87, 0x5b, 0x35,
		0x0f, 0xd4, 0x25, 0x0d, 0xb0, 0x75, 0x83, 0x61}

	randomHash2 = hash.Hash{
		0x5f, 0xe4, 0x30, 0x98, 0xf1, 0x55, 0x26, 0x7a,
		0x50, 0x2e, 0x43, 0xa8, 0x40, 0xae, 0x5b, 0x67,
		0x9e, 0x4b, 0xbe, 0x98, 0x1a, 0x48, 0x30, 0xbd,
		0x0c, 0x63, 0x08, 0x8d, 0x5d, 0xad, 0xf7, 0x19}
)

func TestEmptyHashAdd(t *testing.T) {
	assert.NotEqual(t, hash.Bytes(nil), hash.Hash{})
	assert.NotEqual(t, hash.String(""), hash.Hash{})
}

func TestHashAdd(t *testing.T) {
	assert.Equal(t, hash.Hash{}.Add(randomHash), randomHash)
	assert.Equal(t, randomHash.Add(hash.Hash{}), randomHash)
	assert.NotEqual(t, randomHash.Add(randomHash), hash.Hash{})
	assert.Equal(t, randomHash.Add(randomHash2), randomHash2.Add(randomHash))
}

func TestHashMerge(t *testing.T) {
	assert.NotEqual(t, hash.Hash{}.Merge(randomHash), randomHash)
	assert.NotEqual(t, hash.Hash{}.Merge(randomHash), hash.Hash{})
	assert.NotEqual(t, randomHash.Merge(hash.Hash{}), randomHash)
	assert.NotEqual(t, randomHash.Merge(hash.Hash{}), hash.Hash{})
	assert.NotEqual(t, randomHash.Merge(randomHash2), randomHash2.Add(randomHash))
	assert.NotEqual(t, randomHash.Merge(randomHash), hash.Hash{})
}

// SHA512:
// BenchmarkHash-56       	   30000	     45427 ns/op

func BenchmarkHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		h := hash.Hash{
			0x5f, 0xe4, 0x30, 0x98, 0xf1, 0x55, 0x26, 0x7a,
			0x50, 0x2e, 0x43, 0xa8, 0x40, 0xae, 0x5b, 0x67,
			0x9e, 0x4b, 0xbe, 0x98, 0x1a, 0x48, 0x30, 0xbd,
			0x0c, 0x63, 0x08, 0x8d, 0x5d, 0xad, 0xf7, 0x19}
		for j := 100; j < 200; j++ {
			buf := [8]byte{}
			binary.LittleEndian.PutUint64(buf[:], uint64(j))
			h = h.Merge(hash.Bytes(buf[:]))
		}
	}
}

package gql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func runShardRange(shard, nshards, nitems int) string {
	start, limit := ScaleShardRange(shard, shard+1, nshards, nitems)
	return fmt.Sprintf("%d,%d", start, limit)
}

func TestShardRange(t *testing.T) {
	assert.Equal(t, "0,0", runShardRange(0, 10, 1))
	assert.Equal(t, "8,17", runShardRange(1, 10, 85))
	assert.Equal(t, "76,85", runShardRange(9, 10, 85))
	assert.Equal(t, "85,85", runShardRange(10, 10, 85))

	assert.Equal(t, "0,900", runShardRange(0, 2, 1800))
	assert.Equal(t, "900,1800", runShardRange(1, 2, 1800))
}

func doNextSubTable(
	scanShardStart, scanShardLimit, scanOff int, cumSubTableLen []int) (v [4]int) {
	v[0], v[1], v[2], v[3] = nextSubTable(scanShardStart, scanShardLimit, scanOff, cumSubTableLen)
	return
}

func TestNextSubTable(t *testing.T) {
	assert.Equal(t, [...]int{2, 0, 1, 1}, doNextSubTable(0, 1, 0, []int{0, 0, 1}))
	assert.Equal(t, [...]int{0, 0, 2, 2}, doNextSubTable(0, 3, 1, []int{2, 2, 4}))
	assert.Equal(t, [...]int{2, 2, 4, 3}, doNextSubTable(0, 3, 2, []int{2, 2, 4}))

	assert.Equal(t, [...]int{0, 0, 3, 3}, doNextSubTable(0, 10, 2, []int{3, 6, 10}))
	assert.Equal(t, [...]int{1, 3, 6, 6}, doNextSubTable(0, 10, 3, []int{3, 6, 10}))
	assert.Equal(t, [...]int{1, 3, 6, 6}, doNextSubTable(0, 10, 5, []int{3, 6, 10}))
	assert.Equal(t, [...]int{1, 3, 6, 5}, doNextSubTable(0, 5, 4, []int{3, 6, 10}))
}

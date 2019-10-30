package gql

import (
	"sort"

	"github.com/grailbio/base/log"
)

func ScaleShardRange(start, limit, nshards, nitems int) (int, int) {
	perShard := float64(nitems) / float64(nshards)
	scaledStart := int(perShard * float64(start))
	if scaledStart >= nitems {
		scaledStart = nitems
	}
	scaledLimit := nitems
	if limit < nshards {
		scaledLimit = int(perShard * float64(limit))
		if scaledLimit >= nitems {
			scaledLimit = nitems
		}
	}
	return scaledStart, scaledLimit
}

// nextSubTable computings bounds when reading a table that's sharded N
// ways. cumSubTableSizes[] is an N-element array, and cumSubTableSizes[i]
// should store cumulative size of subtables[0] to subtables[i].
//
// [scanShardStart,scanShardLimit) is the range that the caller is assigned to
// handle, and [0,scanOff) is the range that's been already processed
// already.
//
// This method returns the index of the subtable and part of within the subtable
// to read.
func nextSubTable(
	scanShardStart, scanShardLimit, scanOff int, cumSubTableSizes []int) (subTableIndex int, subTableStart, subTableLimit, scanLimit int) {
	if scanOff < scanShardStart {
		log.Panicf("invalid off: %d of [%d,%d)", scanOff, scanShardStart, scanShardLimit)
	}
	if scanOff >= scanShardLimit {
		return -1, 0, 0, 0
	}
	subTableStart = 0
	if true || scanOff > 0 {
		subTableIndex = sort.Search(len(cumSubTableSizes), func(i int) bool { return cumSubTableSizes[i] > scanOff })
		if subTableIndex >= len(cumSubTableSizes) {
			return -1, 0, 0, 0
		}
		if subTableIndex > 0 {
			subTableStart = cumSubTableSizes[subTableIndex-1]
		}
	}
	subTableLimit = cumSubTableSizes[subTableIndex]
	scanLimit = subTableLimit
	if scanLimit > scanShardLimit {
		scanLimit = scanShardLimit
	}
	return
}

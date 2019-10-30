package gql

import (
	"context"
	"runtime"

	"github.com/grailbio/base/log"
)

// PrefetchingTableScanner is a TableScanner that wraps another TableScanner and
// prefetches rows.
type PrefetchingTableScanner struct {
	ctx   context.Context
	s     TableScanner
	index int

	// A ring buffer of prefetched rows. Value() returns queue[index].
	queue []Value
}

// NewPrefetchingTableScanner creates a TableScanner that wraps another scanner
// and prefetches up to "depth" rows.
func NewPrefetchingTableScanner(ctx context.Context, s TableScanner, depth int) TableScanner {
	if depth <= 0 {
		depth = runtime.NumCPU() * 2
		if depth > 128 {
			depth = 128
		}
	}
	scanner := &PrefetchingTableScanner{
		ctx:   ctx,
		s:     s,
		index: -1,
		queue: make([]Value, depth),
	}
	for i := range scanner.queue {
		if scanner.s.Scan() {
			val := scanner.s.Value()
			val.Prefetch(ctx)
			scanner.queue[i] = val
		} else {
			break
		}
	}
	return scanner
}

// Scan implements the TableScanner interface.
func (s *PrefetchingTableScanner) Scan() bool {
	if s.index >= 0 {
		qi := s.index % len(s.queue)
		if s.queue[qi].Type() == InvalidType {
			return false
		}
		s.queue[qi] = Value{} // Mark EOF
		if s.s.Scan() {
			val := s.s.Value()
			val.Prefetch(s.ctx)
			s.queue[qi] = val
		}
	}
	s.index++
	qi := s.index % len(s.queue)
	return s.queue[qi].Type() != InvalidType
}

// Value implements the TableScanner interface.
func (s *PrefetchingTableScanner) Value() Value {
	qi := s.index % len(s.queue)
	if s.queue[qi].Type() == InvalidType {
		log.Panicf("table past eof")
	}
	return s.queue[qi]
}

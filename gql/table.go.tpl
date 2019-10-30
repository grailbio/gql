package grailgql

// A template for a table that provides a view over a native Go slice.
// Template parameters:
//
// ELEM should be the element type of the slice.
// QTYPE should be the corresponding GQL type, e.g., Int, Float, String,  FileName, Date.
// GTIPE should be the corresponding Go type, e.g., int64, float64, string.
//
// GTYPE is in fact fixed given QTYPE, but our template system is dumb so you need to provide it separately.
//
// Example:
// $GRAIL/go/src/github.com/base/grailbio/base/gtl/generate.py --prefix=xxxtype --package=gql --output=xxx_table.go -DELEM=xxx -DQTYPE=Int -DTYPE=int64 ../../../../github.com/grailbio/base/gtl/unsafe.go.tpl

import (
       "context"
	"sync"

	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

type zzTableImpl struct {
	recs     []ELEM
	basePos  int64
	hashOnce sync.Once
	hash     hash.Hash
	attrs    TableAttrs
}

type zzScanner struct {
	parent                 *zzTableImpl
	startIndex, limitIndex int
	index                  int
}

// Scan implements the TableScanner interface.
func (t *zzScanner) Scan() bool {
	t.index++
	return t.index < t.limitIndex
}

// Value implements the TableScanner interface.
func (t *zzScanner) Value() Value {
	return NewStruct(NewSimpleStruct(
               StructField{Name: symbol.Pos, Value: NewInt(t.parent.basePos + int64(t.index))},
               StructField{Name: symbol.Value, Value: NewQTYPE(GTYPE(t.parent.recs[t.index]))}))

}

// Prefetch implements the Table interface.
func (t *zzTableImpl) Prefetch(ctx context.Context) {}

// Len implements the Table interface.
func (t *zzTableImpl) Len(ctx context.Context, _ CountMode) int { return len(t.recs) }

// MarshalBinary implements the GOB interface.
func (t *zzTableImpl) MarshalBinary() ([]byte, error) {
	panic("use Marshal instead")
}

// UnmarshalBinary implements the GOB interface.
func (t *zzTableImpl) UnmarshalBinary() ([]byte, error) {
	panic("use Unmarshal instead")
}

// Marshal implements the Table interface.
func (t *zzTableImpl) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	if len(t.recs) < 65536 {
		MarshalTableInline(ctx, enc, t)
	} else {
		MarshalTableOutline(ctx, enc, t)
	}
}

// Scanner implements the Table interface.
func (t *zzTableImpl) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	sc := &zzScanner{parent: t}
	sc.startIndex, sc.limitIndex = ScaleShardRange(start, limit, total, len(t.recs))
	sc.index = sc.startIndex - 1 // First Scan() will increment it.
	return sc
}

// Hash implements the Table interface.
func (t *zzTableImpl) Hash() hash.Hash {
	t.hashOnce.Do(func() {
		t.hash = hash.Bytes(unsafeELEMsToBytes(t.recs))
	})
	return t.hash
}

// Attrs implements the Table interface.
func (t *zzTableImpl) Attrs(context.Context) TableAttrs { return t.attrs }

// NewZZTable creates a trivial table that stores all the rows in memory.
// If h!=zeroHash, it is computed from the contents.
func newZZTable(recs []ELEM, basePos int64, attrs TableAttrs) Table {
	// TODO(saito) set hash seed.
	v := &zzTableImpl{
		recs:    make([]ELEM, len(recs)),
		basePos: basePos,
		attrs:   attrs,
	}
	copy(v.recs, recs)
	return v
}

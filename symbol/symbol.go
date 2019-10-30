// Package symbol manages symbols. Symbols are deduped strings represented as
// small integers.
package symbol

//go:generate ../../../../github.com/grailbio/base/gtl/generate.py --prefix=symbol --PREFIX=Symbol --package=symbol --output=symbol_map.go -DKEY=string -DVALUE=ID -DHASH=hashSymbolName ../../../../github.com/grailbio/base/gtl/rcu_map.go.tpl

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
)

// ID represents an interned symbol.
type ID int32

const (
	// Invalid is a sentinel.
	Invalid = ID(0)
)

type idInfo struct {
	name string
	hash hash.Hash
}

// Singleton symbol intern table.
type table struct {
	sync.Mutex

	// max ID value of pre-interned symbols. Pre-interned symbols are symbols that
	// are interned during GQL initialization.  These symbols are guaranteed to
	// have the same ID<->name mappings, so they can be transmitted across
	// bigslice machines efficiently.
	preInterned ID

	// The readers can access the following fields using acquire loads.
	// The writers must synchronize using the mutex.
	syms   *SymbolMap
	idsPtr unsafe.Pointer // *[]idInfo
}

var symbols table

func maybeInit() {
	if symbols.syms == nil {
		const capacity = 1024
		syms := NewSymbolMap(capacity)
		ids := make([]idInfo, 0, capacity)
		syms.Store("(invalid)", 0)
		ids = append(ids, idInfo{"(invalid)", hash.String("(invalid)")})
		symbols = table{syms: syms, idsPtr: unsafe.Pointer(&ids)}
	}
}

func init() {
	maybeInit()
}

func (t *table) ids() []idInfo {
	return *(*[]idInfo)(atomic.LoadPointer(&t.idsPtr))
}

// MarkPreInternedSymbols must be called at the end of gql initialization.
func MarkPreInternedSymbols() {
	symbols.preInterned = ID(len(symbols.ids()))
	log.Debug.Printf("Pre-interned %d symbols", symbols.preInterned)
}

// Hash hashes a symbol.
func (id ID) Hash() hash.Hash {
	return symbols.ids()[id].hash
}

// MarshalBinary implements the GOB interface.
func (id ID) MarshalBinary() ([]byte, error) {
	enc := marshal.NewEncoder(nil)
	id.Marshal(enc)
	return marshal.ReleaseEncoder(enc), nil
}

// Marshal encodes the ID in binary.
func (id ID) Marshal(enc *marshal.Encoder) {
	if id < symbols.preInterned {
		enc.PutByte(0)
		enc.PutVarint(int64(id))
		return
	}
	enc.PutByte(1)
	enc.PutSymbol(id.Str())
}

// Str returns a human-readable string.
//
// Note: we don't call it String() since it makes the code deadlock prone.
func (id ID) Str() string {
	name := symbols.ids()[id].name
	if name == "" {
		log.Panicf("symboltable: id %d not found", id)
	}
	return name
}

// UnmarshalBinary implements the GOB interface.
func (id *ID) UnmarshalBinary(data []byte) error {
	dec := marshal.NewDecoder(data)
	id.Unmarshal(dec)
	if dec.Len() > 0 {
		log.Panicf("Value.UnmarshalBinary: %dB garbage at the end", dec.Len())
	}
	marshal.ReleaseDecoder(dec)
	return nil
}

// Unmarshal decodes the data produced by MarshalGOB.
func (id *ID) Unmarshal(dec *marshal.Decoder) {
	b := dec.Byte()
	switch b {
	case 0:
		*id = ID(dec.Varint())
	case 1:
		*id = Intern(dec.Symbol())
	default:
		log.Panicf("unmarshal symbol.id: corrupt data %v", b)
	}
}

// Intern finds or creates an ID for the given string.
func Intern(v string) ID {
	maybeInit()
	if v == "" {
		log.Panicf("Empty symbol")
	}
	if id, ok := symbols.syms.Load(v); ok {
		return id
	}

	symbols.Lock()
	defer symbols.Unlock()
	if id, ok := symbols.syms.Load(v); ok {
		return id
	}
	// Slow path: add a new symbol.
	ids := symbols.ids()
	id := ID(len(ids))
	if id == Invalid {
		id++
	}
	for len(ids) <= int(id) {
		ids = append(ids, idInfo{})
	}

	// Note: the reader may read the ids[id] unsynchronized, but that happen only
	// when the reader looks up an yet-unallocated ID. So as long as application
	// logic is correct, the next store works.
	ids[id] = idInfo{v, hash.String(v)}
	// The next store makes the update officially visible.
	atomic.StorePointer(&symbols.idsPtr, unsafe.Pointer(&ids))
	symbols.syms.Store(v, id)
	return id
}

// hashSymbolID is used only by symbolMap to implement the concurrent hash
// table. It's unrelated to the ID.Hash() value.
func hashSymbolName(name string) uint64 {
	h := hash.String(name)
	return *(*uint64)(unsafe.Pointer(&h[0]))
}

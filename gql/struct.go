package gql

//go:generate ../../../../github.com/grailbio/base/gtl/generate.py --PREFIX=simpleStruct2 --package=gql --output=struct2.go -DNN=2 struct.go.tpl
//go:generate ../../../../github.com/grailbio/base/gtl/generate.py --PREFIX=simpleStruct4 --package=gql --output=struct4.go -DNN=4 struct.go.tpl
//go:generate ../../../../github.com/grailbio/base/gtl/generate.py --PREFIX=simpleStruct8 --package=gql --output=struct8.go -DNN=8 struct.go.tpl
//go:generate ../../../../github.com/grailbio/base/gtl/generate.py --PREFIX=simpleStruct12 --package=gql --output=struct12.go -DNN=12 struct.go.tpl
//go:generate ../../../../github.com/grailbio/base/gtl/generate.py --PREFIX=simpleStruct16 --package=gql --output=struct16.go -DNN=16 struct.go.tpl

import (
	"unsafe"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

// StructField represents a field (column) in a struct (row).
type StructField struct {
	Name  symbol.ID
	Value Value
}

// Struct is a collection of name->value mappings. Its analogous to a row in a
// relational database.
//
// To reduce heap allocation, every struct implementation embeds the the itable
// pointer in its first word.  Take the following code snippet as an example:
//
//   v := &structN{values: ...}
//   initStruct(v)
//   var s Struct = v
//
// The memory layout will look like below:
//
//
// s (Struct)
// +------+
// | iptr	|	 --------------------------------------->+-------+
// +------+							 +-----------+						/	 |random |
// | data	|  ----------> | structImpl|-----------/   |itable |
// +------+							 +-----------+							 |stuff	 |
//											 |  values   |							 +-------+
//                       +-----------+
//                        v (structN)
//
//
// The first word of *v will point to the itable of type structN. Function
// initStruct() plumbs these the fields.  The Value object stores only the data
// pointer to the struct. Struct can be reconstructed from Value by first
// reading the data pointer, then reading the first word of the data for itable.
//
// For more details about Go interface implementation, see
// https://research.swtch.com/interfaces.
type Struct interface {
	// Len returns the number of fields (columns) in the struct.
	Len() int
	// Field returns the name and the value of the i'th field.
	//
	// REQUIRES: 0 <= i < Len()
	Field(i int) StructField
	// Value returns the value for the specified column. It returns false if the
	// column is not found.
	Value(colName symbol.ID) (Value, bool)
}

// nullStruct is a singleton empty row.
var nullStruct = NewSimpleStruct()

// StructFragment extracts a []StructField created by "expr./columnregex/"
// construct.
func (v Value) StructFragment() []StructField {
	if v.typ != StructFragmentType {
		log.Panicf("StructFragment: %v is not a frag", v)
	}
	return *(*[]StructField)(v.p)
}

// structN is the most generic implementation of a struct. It stores arbitrary
// number of fields.
type structN struct {
	StructImpl
	values []StructField
}

// Len implements Struct
func (v *structN) Len() int { return len(v.values) }

// Field implements Struct
func (v *structN) Field(i int) StructField { return v.values[i] }

// Value implements Struct
func (v *structN) Value(colName symbol.ID) (Value, bool) {
	for _, f := range v.values {
		if f.Name == colName {
			return f.Value, true
		}
	}
	return Value{}, false
}

type StructImpl struct{ iface unsafe.Pointer }
type goInterfaceImpl struct{ iface, data unsafe.Pointer }

// InitStruct initializes the itable pointer of the given struct. It must be
// called once when a new Struct object is created.
func InitStruct(s Struct) {
	iface := (*goInterfaceImpl)(unsafe.Pointer(&s))
	sp := (*StructImpl)(iface.data)
	sp.iface = iface.iface
}

// NewSimpleStruct creates a new struct from the given list of fields.  "t"
// must be of type StructKind, and its field descriptions must match the
// values'.
func NewSimpleStruct(values ...StructField) Struct {
	n := len(values)
	switch {
	case n <= 2:
		v := &simpleStruct2Impl{
			nFields: n,
		}
		InitStruct(v)
		for i, f := range values {
			v.names[i] = f.Name
			v.values[i] = f.Value
		}
		return v
	case n <= 4:
		v := &simpleStruct4Impl{
			nFields: n,
		}
		InitStruct(v)
		for i, f := range values {
			v.names[i] = f.Name
			v.values[i] = f.Value
		}
		return v
	case n <= 8:
		v := &simpleStruct8Impl{
			nFields: n,
		}
		InitStruct(v)
		for i, f := range values {
			v.names[i] = f.Name
			v.values[i] = f.Value
		}
		return v
	case n <= 12:
		v := &simpleStruct12Impl{
			nFields: n,
		}
		InitStruct(v)
		for i, f := range values {
			v.names[i] = f.Name
			v.values[i] = f.Value
		}
		return v
	case n <= 16:
		v := &simpleStruct16Impl{
			nFields: n,
		}
		InitStruct(v)
		for i, f := range values {
			v.names[i] = f.Name
			v.values[i] = f.Value
		}
		return v
	default:
		v := &structN{values: make([]StructField, n)}
		InitStruct(v)
		copy(v.values, values)
		return v
	}
}

// hashStruct returns the hash of this object. It must return the same hash value for
// the same value even when invoked on a different machine.
func hashStruct(v Struct) hash.Hash {
	h := hash.Hash{
		0xc3, 0xf0, 0x50, 0x64, 0x41, 0xa5, 0x5f, 0x5b,
		0x90, 0xaa, 0xae, 0x52, 0x0d, 0xd9, 0x28, 0x1d,
		0x71, 0x58, 0x9a, 0x2c, 0x7c, 0x95, 0xab, 0xb5,
		0x69, 0x7c, 0xf6, 0x74, 0xa0, 0xb2, 0x38, 0x6f}
	n := v.Len()
	for i := 0; i < n; i++ {
		f := v.Field(i)
		h = h.Merge(f.Name.Hash())
		h = h.Merge(f.Value.Hash())
	}
	return h
}

// MustStructValue returns the value for the specified column. It panics if the
// column is not found.
func MustStructValue(v Struct, colName symbol.ID) Value {
	value, ok := v.Value(colName)
	if !ok {
		log.Panicf("MustValue: column %v not found in %v", colName.Str(), NewStruct(v))
	}
	return value
}

// marshalStruct encodes the struct contents in binary.
func marshalStruct(v Struct, ctx MarshalContext, enc *marshal.Encoder) {
	nFields := v.Len()
	enc.PutVarint(int64(nFields))
	for fi := 0; fi < nFields; fi++ {
		val := v.Field(fi)
		val.Name.Marshal(enc)
		val.Value.Marshal(ctx, enc)
	}
}

// unmarshalStruct reconstructs a struct from bytestream produced by MarshalGOB.
func unmarshalStruct(ctx UnmarshalContext, dec *marshal.Decoder) Struct {
	n := int(dec.Varint())
	switch {
	case n <= 2:
		v := &simpleStruct2Impl{
			nFields: n,
		}
		InitStruct(v)
		for i := 0; i < n; i++ {
			v.names[i].Unmarshal(dec)
			v.values[i].Unmarshal(ctx, dec)
		}
		return v
	case n <= 4:
		v := &simpleStruct4Impl{
			nFields: n,
		}
		InitStruct(v)
		for i := 0; i < n; i++ {
			v.names[i].Unmarshal(dec)
			v.values[i].Unmarshal(ctx, dec)
		}
		return v
	case n <= 8:
		v := &simpleStruct8Impl{
			nFields: n,
		}
		InitStruct(v)
		for i := 0; i < n; i++ {
			v.names[i].Unmarshal(dec)
			v.values[i].Unmarshal(ctx, dec)
		}
		return v
	case n <= 12:
		v := &simpleStruct12Impl{
			nFields: n,
		}
		InitStruct(v)
		for i := 0; i < n; i++ {
			v.names[i].Unmarshal(dec)
			v.values[i].Unmarshal(ctx, dec)
		}
		return v
	case n <= 16:
		v := &simpleStruct16Impl{
			nFields: n,
		}
		InitStruct(v)
		for i := 0; i < n; i++ {
			v.names[i].Unmarshal(dec)
			v.values[i].Unmarshal(ctx, dec)
		}
		return v
	default:
		v := &structN{
			values: make([]StructField, n),
		}
		InitStruct(v)
		for i := range v.values {
			v.values[i].Name.Unmarshal(dec)
			v.values[i].Value.Unmarshal(ctx, dec)
		}
		return v
	}
}

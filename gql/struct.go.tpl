package gql

// A template for a struct with a fixed number of fields.
// Template parameters:
//
// NN: number of fields, as an integer.
//
// Example:
// $GRAIL/go/src/github.com/base/grailbio/base/gtl/generate.py --prefix=struct5 --package=gql --output=struct5.go -DNN=5 struct.go.tpl

import "github.com/grailbio/gql/symbol"

// Len implements Struct
func (s *ZZImpl) Len() int { return s.nFields }

// Field implements Struct
func (s *ZZImpl) Field(i int) StructField { return StructField{s.names[i], s.values[i]} }

// Value implements Struct
func (s *ZZImpl) Value(colName symbol.ID) (Value, bool) {
	for i := 0; i < s.nFields; i++ {
		if s.names[i] == colName {
			return s.values[i], true
		}
	}
	return Value{}, false
}

var _ Struct = &ZZImpl{}

type ZZImpl struct {
	StructImpl
	nFields int
	names   [NN]symbol.ID // symbol.ID is 32bit, so pack it from values.
	values  [NN]Value
}
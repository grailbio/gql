package gql

import (
	"fmt"
)

// This file is split from value_type.go only because the stringer chokes otherwise.

// MarshalJSON implements json.Marshaler.
func (v ValueType) MarshalJSON() ([]byte, error) {
	return []byte(`"` + v.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (v *ValueType) UnmarshalJSON(data []byte) error {
	s := string(data)
	for i := InvalidType; i <= FuncType; i++ {
		if s == i.String() {
			*v = i
			return nil
		}
	}
	return fmt.Errorf("ValueType.UnmarshalJson: invalid name '%s'", s)
}

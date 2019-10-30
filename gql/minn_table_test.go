package gql

import (
	"testing"

	"github.com/grailbio/gql/symbol"
	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	for _, cmp := range []struct{ v0, v1 Value }{
		{NewInt(0), NewInt(1)},
		{NewBool(false), NewBool(true)},
		{NewStruct(NewSimpleStruct(
			StructField{Name: symbol.Intern("a_bool"), Value: NewBool(false)},
			StructField{Name: symbol.Intern("an_int"), Value: NewInt(1)},
		)), NewStruct(NewSimpleStruct(
			StructField{Name: symbol.Intern("a_bool"), Value: NewBool(true)},
			StructField{Name: symbol.Intern("an_int"), Value: NewInt(1)},
		))},
	} {
		v0, v1 := cmp.v0, cmp.v1
		assert.Equal(t, Compare(nil, v0, v1), -1)
		assert.Equal(t, Compare(nil, v1, v0), 1)
		assert.Equal(t, Compare(nil, v0, v0), 0)
		assert.Equal(t, Compare(nil, v1, v1), 0)
	}
}

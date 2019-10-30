package columnsorter_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/grailbio/gql/columnsorter"
	"github.com/grailbio/gql/symbol"
)

func Example() {
	s := columnsorter.New()
	s.AddColumns([]symbol.ID{10, 11, 12})
	s.AddColumns([]symbol.ID{12, 13})
	s.AddColumns([]symbol.ID{11, 20, 12})
	s.Sort()
	fmt.Println(s.Columns())
	fmt.Println(s.Index(20))
	// Output:
	// [10 11 20 12 13]
	// 2
}

func TestOneColumn(t *testing.T) {
	s := columnsorter.New()
	s.AddColumns([]symbol.ID{10})
	s.Sort()
	assert.Equal(t, []symbol.ID{10}, s.Columns())
	assert.Equal(t, 0, s.Index(10))
}

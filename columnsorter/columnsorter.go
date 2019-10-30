// Package columnsorter performs toposort of column names. It is used to compute
// the exhausitve list of columns in a table, when not all the row may have the
// same set of columns.
//
// Thread compatible.
//
// Legal call sequence: New AddColumns* Sort (Columns|Index)*
package columnsorter

import (
	"github.com/grailbio/gql/symbol"
	"v.io/x/lib/toposort"
)

// edge represents the fact that column "from" needs to appear before column
// "to" in a row.
type edge struct{ from, to symbol.ID }

// T is the main sorter object. Use New() to create the sorter.
type T struct {
	sorter     toposort.Sorter
	edgesAdded map[edge]bool // list of column ordering constraints added so far.

	sortedCols []symbol.ID       // filled by Sort()
	colIndex   map[symbol.ID]int // filled by Sort()
}

// New creates a new sorter.
func New() *T {
	return &T{
		edgesAdded: map[edge]bool{},
	}
}

func (t *T) addEdge(from, to symbol.ID) {
	e := edge{from, to}
	if _, ok := t.edgesAdded[e]; !ok {
		t.edgesAdded[e] = true
		t.sorter.AddEdge(to, from)
	}
}

// AddColumns adds a sorted list of columns that appear in a row.  One column
// can appear multiple times across AddColumn calls.
//
// REQUIRES: Sort has not been called
func (t *T) AddColumns(colNames []symbol.ID) {
	for i := 0; i < len(colNames)-1; i++ {
		t.addEdge(colNames[i], colNames[i+1])
	}
	if len(colNames) == 1 { // Corner case: no edge specified. Just add the node.
		t.sorter.AddNode(colNames[0])
	}
}

// Sort the columns added so far, so that the resulting list is compatible with
// the column orders specified in all preceding AddColumns calls.
// After the Sort call, no AddColumn can be called.
func (t *T) Sort() {
	sorted, _ := t.sorter.Sort()
	t.sortedCols = []symbol.ID{}
	for _, c := range sorted {
		t.sortedCols = append(t.sortedCols, c.(symbol.ID))
	}
	t.colIndex = map[symbol.ID]int{}
	for i, c := range t.sortedCols {
		t.colIndex[c] = i
	}
}

// Columns returns a toposorted list of columns.
//
// REQUIRES: Sort has been called
func (t *T) Columns() []symbol.ID { return t.sortedCols }

// Index returns the index of the given column in the toposorted list.  The
// first column has index zero.
//
// REQUIRES: Sort has been called
func (t *T) Index(name symbol.ID) int {
	i, ok := t.colIndex[name]
	if !ok {
		panic(name)
	}
	return i
}

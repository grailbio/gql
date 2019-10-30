package gql

import (
	"context"
	"sync"

	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

type collapseTable struct {
	hash         hash.Hash
	ast          ASTNode
	cols         []symbol.ID
	src          Table
	exactLen     int
	exactLenOnce sync.Once
}

func (t *collapseTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Exact {
		// Best to scan the entire table.
		t.exactLenOnce.Do(func() {
			t.exactLen = DefaultTableLen(ctx, t)
		})
		return t.exactLen
	}
	// Assume that the collapse will be 'perfect' in that sense that
	// every column to be collapsed can be collapsed.
	return t.src.Len(ctx, mode) / len(t.cols)
}

func (t *collapseTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	MarshalTableOutline(ctx, enc, t)
}
func (t *collapseTable) Prefetch(ctx context.Context) { /*TODO(saito):implement*/ }
func (t *collapseTable) Hash() hash.Hash              { return t.hash }
func (t *collapseTable) Attrs(ctx context.Context) TableAttrs {
	return TableAttrs{Name: "collapse", Path: t.src.Attrs(ctx).Path}
}

func (t *collapseTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	if start > 0 {
		// collapse cannot be sharded.
		return &NullTableScanner{}
	}
	collapse := make(map[symbol.ID]bool, len(t.cols))
	for _, c := range t.cols {
		collapse[c] = true
	}
	return &collapseTableScanner{
		parent:       t,
		src:          NewPrefetchingTableScanner(ctx, t.src.Scanner(ctx, 0, 1, 1), -1),
		collapseMap:  collapse,
		collapseCols: t.cols,
		stack:        make([]Value, 0, len(t.cols)),
		stackUnique:  make(map[symbol.ID]StructField, len(t.cols)),
	}
}

type collapseTableScanner struct {
	parent *collapseTable
	src    TableScanner

	// A map of the symbols for the columns to be collapsed.
	collapseMap map[symbol.ID]bool
	// Remember the original order of the columns to be collapsed so that
	// the output table can reflect that order.
	collapseCols []symbol.ID

	// Value returned by Value() and produced by the collapseStack method.
	row Value

	// Maintain a stack of all of the rows with the same values for the
	// common columns, ie. the columns that are not being collapsed.
	// Allocations are made reused wherever possible; preallocation
	// is used to avoid dynamically growth.
	stack []Value
	// stackKeyHash is the hash of all of the non-collapse columns for each
	// row in the stack - i.e. all entries in the stack have the same
	// hash.
	stackKeyHash hash.Hash
	// All of the unique fields across all of the values in the stack, ie.
	// the columns to be collapsed.
	stackUnique map[symbol.ID]StructField
	// All of the common fields for the values in the stack - only needs
	// to be determined from one element in the stack since these are
	// by definition the same.
	scratchPad []StructField
}

func (sc *collapseTableScanner) Value() Value {
	return sc.row
}

func (sc *collapseTableScanner) Scan() bool {
	for {
		if !sc.src.Scan() {
			if len(sc.stack) == 0 {
				return false
			}
			sc.collapseAndWriteStack()
			return true
		}

		cur := sc.src.Value()
		row := cur.Struct(sc.parent.ast)
		nf := row.Len()

		h := hash.Hash{
			0xe2, 0x5d, 0x9e, 0x81, 0x96, 0x31, 0xf2, 0xfe,
			0xef, 0x67, 0x85, 0xbd, 0x2c, 0x9e, 0x66, 0xf5,
			0x46, 0x0e, 0x44, 0x6e, 0xc4, 0x81, 0x82, 0xcd,
			0xa1, 0x78, 0x17, 0x26, 0xd6, 0xc4, 0x46, 0x32,
		}

		stop := false
		for i := 0; i < nf; i++ {
			f := row.Field(i)
			if !sc.collapseMap[f.Name] {
				h = h.Merge(f.Value.Hash())
				continue
			}
			if _, ok := sc.stackUnique[f.Name]; ok {
				// Duplicate value in the columns to be collapsed so stop the collapse.
				stop = true
				continue
			}
		}
		if len(sc.stack) > 0 {
			if h != sc.stackKeyHash {
				stop = true
			}
			if stop {
				sc.collapseAndWriteStack()
				sc.appendToStack(cur, h)
				return true
			}
		}
		sc.appendToStack(cur, h)
	}
}

func (sc *collapseTableScanner) appendToStack(next Value, nextHash hash.Hash) {
	sc.stack = append(sc.stack, next)
	sc.stackKeyHash = nextHash
	row := next.Struct(sc.parent.ast)
	for i := 0; i < row.Len(); i++ {
		if f := row.Field(i); sc.collapseMap[f.Name] {
			sc.stackUnique[f.Name] = f
		}
	}
}

// collapseAndWriteStack will collapse all of the elements in the stack
// into a single element and write it to the scanner's 'row' field for
// subsequent by the Value() method.
func (sc *collapseTableScanner) collapseAndWriteStack() {
	row := sc.stack[0].Struct(sc.parent.ast)
	nf := row.Len()
	if cap(sc.scratchPad) < nf {
		sc.scratchPad = make([]StructField, 0, nf)
	}
	scratchPad := sc.scratchPad[:0]
	first := true
	for _, row := range sc.stack {
		for i := 0; i < nf; i++ {
			f := row.Struct(sc.parent.ast).Field(i)
			if !sc.collapseMap[f.Name] {
				if first {
					scratchPad = append(scratchPad, f)
				}
			}
		}
		first = false
	}

	// Create the collapsed row to be returned by Value().
	for _, c := range sc.collapseCols {
		if d, ok := sc.stackUnique[c]; ok {
			scratchPad = append(scratchPad, d)
			delete(sc.stackUnique, c)
		}
	}
	if len(sc.stackUnique) > 0 {
		Panicf(sc.parent.ast, "collapse did not consume all collapse columns")
	}
	sc.row = NewStruct(NewSimpleStruct(scratchPad...))
	sc.stack = sc.stack[:0]
}

func builtinCollapse(table Table, ast ASTNode, cols []symbol.ID) Value {
	h := collapseHashOp(table, cols)
	ct := &collapseTable{
		hash: h,
		ast:  ast,
		src:  table,
		cols: cols,
	}
	nt := NewTable(ct)
	return nt
}

func collapseHashOp(table Table, cols []symbol.ID) hash.Hash {
	h := hash.Hash{
		0xe1, 0x56, 0x5e, 0xd4, 0xa8, 0xc7, 0x7d, 0xf7,
		0xd9, 0x4d, 0xad, 0xbc, 0x16, 0x72, 0x18, 0xd8,
		0xb2, 0x24, 0xca, 0x2b, 0x19, 0x66, 0x66, 0xf7,
		0xd2, 0xf5, 0xcf, 0x06, 0x47, 0x32, 0x96, 0x9e,
	}
	h = h.Merge(table.Hash())
	for _, s := range cols {
		h = h.Merge(s.Hash())
	}
	return h
}

func init() {
	RegisterBuiltinFunc("collapse",
		`
    tbl | collapse(colname...)

Arg types:

- _colname_: string

Collapse will collapse multiple rows with non-overlapping values for the specified
columns into a single row when all of the other cell values are identical.

Example: Imagine table t0 with following contents:

        ║col0 ║ col1║ col2║
        ├─────┼─────┤─────┤
        │Cat  │ 30  │     │
        │Cat  │     │ 41  │

::t0 | collapse("col1", "col2"):: will produce the following table:

        ║col0 ║ col1║ col2║
        ├─────┼─────┤─────┤
        │Cat  │ 30  │ 41  │

Note that the collapse will stop if one of the specified columns has multiple
values, for example for t0 below:

        ║col0 ║ col1║ col2║
        ├─────┼─────┤─────┤
        │Cat  │ 30  │     │
        │Cat  │ 31  │ 41  │

::t0 | collapse("col1", "col2"):: will produce the following table:

        ║col0 ║ col1║ col2║
        ├─────┼─────┤─────┤
        │Cat  │ 30  │     │
        |Cat  │ 30  │ 41  │

`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			n := len(args)
			cols := make([]symbol.ID, 0, n)
			for _, arg := range args[1:n] {
				cols = append(cols, symbol.Intern(arg.Str()))
			}
			return builtinCollapse(args[0].Table(), ast, cols)
		},
		func(ast ASTNode, args []AIArg) AIType {
			return AITableType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},                  // table
		FormalArg{Positional: true, Required: true, Variadic: true, Types: []ValueType{StringType}}, // column names
	)
}

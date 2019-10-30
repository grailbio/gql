package gql

import (
	"context"

	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/gql/termutil"
)

type transposeTable struct {
	hash             hash.Hash
	ast              ASTNode
	src              Table
	keyExpr, colExpr *Func

	// Variables standing for the toplevel row and rows in the subtable,
	// respectively.  Currently colVar is always "_". keyVar is by default "_",
	// but can be overridden using the "row" arg.
	keyVar, colVar symbol.ID
}

type transposeTableScanner struct {
	ctx              context.Context
	parent           *transposeTable
	keyExpr, colExpr *Func // clones of parent.{key,col}Expr
	src              TableScanner
	row              Value

	tmpBuf *termutil.BufferPrinter
}

func (t *transposeTable) Len(ctx context.Context, mode CountMode) int { return t.src.Len(ctx, mode) }
func (t *transposeTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	MarshalTableOutline(ctx, enc, t)
}

func (t *transposeTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	ft := &transposeTableScanner{
		ctx:     ctx,
		parent:  t,
		keyExpr: t.keyExpr,
		colExpr: t.colExpr,
		src:     NewPrefetchingTableScanner(ctx, t.src.Scanner(ctx, start, limit, total), -1),
		tmpBuf:  termutil.NewBufferPrinter(),
	}
	return ft
}

func (sc *transposeTableScanner) transposeCols(subCols []StructField) StructField {
	n := len(subCols)
	sc.tmpBuf.Reset()
	for i := 0; i < n-1; i++ {
		arg := PrintArgs{
			Out:  sc.tmpBuf,
			Mode: PrintValues,
		}
		subCols[i].Value.Print(sc.ctx, arg)
		if i < n-2 {
			sc.tmpBuf.WriteString("_")
		}
	}
	return StructField{
		Name:  symbol.Intern(sc.tmpBuf.String()),
		Value: subCols[n-1].Value,
	}
}

func (sc *transposeTableScanner) rowCols(row Struct) []StructField {
	n := row.Len()
	f := make([]StructField, n)
	for i := 0; i < n; i++ {
		f[i] = row.Field(i)
	}
	return f
}

func (t *transposeTable) Prefetch(ctx context.Context) { /*TODO(saito):implement*/ }
func (t *transposeTable) Hash() hash.Hash              { return t.hash }
func (t *transposeTable) Attrs(ctx context.Context) TableAttrs {
	return TableAttrs{Name: "transpose", Path: t.src.Attrs(ctx).Path}
}

func (sc *transposeTableScanner) Scan() bool {
	if !sc.src.Scan() {
		return false
	}
	rowVal := sc.src.Value()
	row := rowVal.Struct(sc.parent.ast)
	if row.Len() != 2 {
		Panicf(sc.parent.ast, "transpose: table must have two columns, but found %+v", rowVal)
	}
	key := sc.keyExpr.Eval(sc.ctx, rowVal).Struct(sc.parent.ast)

	subTable := row.Field(1).Value.Table(sc.parent.ast)
	subTableScanner := subTable.Scanner(sc.ctx, 0, 1, 1)

	cols := sc.rowCols(key) // TODO(saito) uniquify
	for subTableScanner.Scan() {
		subRow := subTableScanner.Value()
		var subCols Struct
		if len(sc.colExpr.formalArgs) == 1 {
			subCols = sc.colExpr.Eval(sc.ctx, subRow).Struct(sc.parent.ast)
		} else {
			subCols = sc.colExpr.Eval(sc.ctx, rowVal, subRow).Struct(sc.parent.ast)
		}
		if subCols.Len() < 2 {
			Panicf(sc.parent.ast, "transpose: must have >=2 columns in the colexpr, but found %+v", cols)
		}
		cols = append(cols, sc.transposeCols(sc.rowCols(subCols)))
	}
	sc.row = NewStruct(NewSimpleStruct(cols...))
	return true
}

func (sc *transposeTableScanner) Value() Value { return sc.row }

func builtinTranspose(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	keyVar := args[3].Symbol
	colVar := symbol.AnonRow // variable for columns is currently fixed.

	table := args[0].Table()
	keyExpr := args[1].Func()
	colExpr := args[2].Func()

	h := hashTransposeCall(table, keyExpr, colExpr)
	t := &transposeTable{
		hash:    h,
		ast:     ast,
		src:     table,
		keyExpr: keyExpr,
		colExpr: colExpr,
		keyVar:  keyVar,
		colVar:  colVar,
	}
	return NewTable(t)
}

func hashTransposeCall(table Table, keyExpr, colExpr *Func) hash.Hash {
	h := hash.Hash{
		0xbb, 0x4b, 0x03, 0xf9, 0x48, 0xf6, 0x7d, 0xe1,
		0xf8, 0x70, 0x32, 0x95, 0x56, 0xfb, 0xba, 0xab,
		0xc4, 0x7e, 0xee, 0xd6, 0xcf, 0xec, 0xee, 0x98,
		0x78, 0xb1, 0x82, 0xc6, 0xf2, 0x59, 0x56, 0xe8}
	h = h.Merge(table.Hash())
	h = h.Merge(keyExpr.Hash())
	h = h.Merge(colExpr.Hash())
	return h
}

func init() {
	RegisterBuiltinFunc("transpose",
		`
    tbl | transpose({keycol: keyexpr}, {col0:expr0, col1:expr1, .., valcol:valexpr})

Arg types:

_keyexpr_: one-arg function
_expri_: one-arg function
_valexpr_: one-arg function

Transpose function creates a table that transposes the given table,
synthesizing column names from the cell values. _tbl_ must be a two-column table
created by [cogroup](#cogroup). Imagine table t0 created by cogroup:

t0:

        ║key  ║value ║
        ├─────┼──────┤
        │120  │ tmp1 │
        │130  │ tmp2 │


Each cell in the ::value:: column must be another table, for example:

tmp1:

        ║chrom║start║  end║count║
        ├─────┼─────┼─────┼─────┤
        │chr1 │    0│  100│  111│
        │chr1 │  100│  200│  123│
        │chr2 │    0│  100│  234│


tmp2:

        ║chrom║start║  end║count║
        ├─────┼─────┼─────┼─────┤
        │chr1 │    0│  100│  444│
        │chr1 │  100│  200│  456│
        │chr2 │  100│  200│  478│


::t0 | transpose({sample_id:&key}, {&chrom, &start, &end, &count}):: will produce
the following table.


        ║sample_id║ chr1_0_100║ chr1_100_200║ chr2_0_100║ chr2_100_200║
        ├─────────┼───────────┼─────────────┼───────────┼─────────────┤
        │120      │   111     │   123       │   234     │    NA       │
        │130      │   444     │   456       │   NA      │   478       │

The _keyexpr_ must produce a struct with >= 1 column(s).

The 2nd arg to transpose must produce a struct with >= 2 columns. The last column is used
as the value, and the other columns used to compute the column name.

`, builtinTranspose,
		func(ast ASTNode, args []AIArg) AIType {
			n := len(args[2].Type.FormalArgs)
			if n != 1 && n != 2 {
				Panicf(ast, "transpose: the col callback must have one or two args")
			}
			return AITableType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}},              // table
		FormalArg{Positional: true, Required: true, Closure: true, ClosureArgs: anonRowFuncArg}, // keys
		FormalArg{Positional: true, Required: true, Types: []ValueType{FuncType}},               // cols
		FormalArg{Name: symbol.Row, Symbol: true, DefaultSymbol: symbol.AnonRow})
}

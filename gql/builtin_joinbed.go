package gql

// This file implements joinbed(), a variation of join optimized for genomic
// range intersections.

import (
	"context"
	"sync"

	"github.com/grailbio/base/intervalmap"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

type bedTargetField struct {
	// Exactly one of field,expr is set.
	field symbol.ID
	expr  *Func
}

func (t bedTargetField) Hash() hash.Hash {
	if t.expr != nil {
		return t.expr.Hash()
	}
	return t.field.Hash()
}

func (t bedTargetField) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	if t.expr != nil {
		enc.PutVarint(1)
		t.expr.Marshal(ctx, enc)
		return
	}
	enc.PutVarint(0)
	t.field.Marshal(enc)
}

func (t *bedTargetField) Unmarshal(ctx UnmarshalContext, dec *marshal.Decoder) {
	typ := dec.Varint()
	if typ == 1 {
		t.expr = unmarshalFunc(ctx, dec)
		return
	}
	if typ != 0 {
		panic(typ)
	}
	t.field.Unmarshal(dec)
}

func (t bedTargetField) Clone() bedTargetField {
	other := bedTargetField{field: t.field}
	if t.expr != nil {
		other.expr = t.expr
	}
	return other
}

func readFeatureMapFromBED(ctx context.Context, ast ASTNode, bedTable Table) map[string]*intervalmap.T {
	sc := bedTable.Scanner(ctx, 0, 1, 1)
	targets := map[string][]intervalmap.Entry{}
	for sc.Scan() {
		val := sc.Value()
		row := val.Struct(ast)
		if row.Field(0).Name != symbol.Chrom ||
			row.Field(1).Name != symbol.Start ||
			row.Field(2).Name != symbol.End {
			Panicf(ast, "Invalid bed row: %v", val)
		}
		chrom := row.Field(0).Value.Str(ast)
		start := row.Field(1).Value.Int(ast)
		limit := row.Field(2).Value.Int(ast)
		targets[chrom] = append(targets[chrom], intervalmap.Entry{
			Interval: intervalmap.Interval{start, limit},
			Data:     val,
		})
	}
	featMap := map[string]*intervalmap.T{}
	for chrom, ents := range targets {
		featMap[chrom] = intervalmap.New(ents)
	}
	return featMap
}

func evalBEDTargetField(ctx context.Context, ast ASTNode, row Struct, field bedTargetField) Value {
	if field.field != symbol.Invalid {
		val, ok := row.Value(field.field)
		if !ok {
			Panicf(ast, "field '%s' not found in row %v", field.field.Str(), NewStruct(row))
		}
		return val
	}
	return field.expr.Eval(ctx, NewStruct(row))
}

func getBEDTargetField(arg ActualArg, name symbol.ID) bedTargetField {
	if arg.Func() == nil {
		// If the arg is not specified, assume that we extract the value of column "name" from each row.
		return bedTargetField{field: name}
	}
	// TODO(saito) detect when fn is of form $field or row.field and short-circuit
	// evaluation.
	return bedTargetField{expr: arg.Func()}
}

type joinBEDTable struct {
	hash     hash.Hash
	ast      ASTNode // location in the source code. Only for error reporting.
	srcTable Table
	bedTable Table
	mapExpr  *Func

	chrom, start, limit, length bedTargetField
	hasLength                   bool

	once sync.Once

	// chromname -> [start,limit) -> chromInterval
	featMap map[string]*intervalmap.T

	exactLenOnce sync.Once
	exactLen     int
}

func (t *joinBEDTable) Hash() hash.Hash { return t.hash }

func (t *joinBEDTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		return t.srcTable.Len(ctx, Approx)
	}
	t.exactLenOnce.Do(func() {
		t.exactLen = DefaultTableLen(ctx, t)
	})
	return t.exactLen
}

func (t *joinBEDTable) Attrs(ctx context.Context) (attrs TableAttrs) {
	attrs.Name = "joinbed"
	attrs.Path = t.srcTable.Attrs(ctx).Path
	return attrs
}

var joinBEDMagic = UnmarshalMagic{0x62, 0xbf}

func (t *joinBEDTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	enc.PutRawBytes(joinBEDMagic[:])
	enc.PutHash(t.hash)
	enc.PutGOB(&t.ast)
	t.srcTable.Marshal(ctx, enc)
	t.bedTable.Marshal(ctx, enc)
	t.mapExpr.Marshal(ctx, enc)
	t.chrom.Marshal(ctx, enc)
	t.start.Marshal(ctx, enc)
	t.limit.Marshal(ctx, enc)
	t.length.Marshal(ctx, enc)
	enc.PutBool(t.hasLength)
}

func unmarshalJoinBEDTable(ctx UnmarshalContext, hash hash.Hash, dec *marshal.Decoder) Table {
	var ast ASTNode
	dec.GOB(&ast)
	t := &joinBEDTable{
		hash:     hash,
		ast:      ast,
		srcTable: unmarshalTable(ctx, dec),
		bedTable: unmarshalTable(ctx, dec),
		mapExpr:  unmarshalFunc(ctx, dec),
	}

	t.chrom.Unmarshal(ctx, dec)
	t.start.Unmarshal(ctx, dec)
	t.limit.Unmarshal(ctx, dec)
	t.length.Unmarshal(ctx, dec)
	t.hasLength = dec.Bool()
	return t
}

func (t *joinBEDTable) Prefetch(ctx context.Context) { go Recover(func() { t.init(ctx) }) }

func (t *joinBEDTable) Scanner(ctx context.Context, start, limit, nshards int) TableScanner {
	t.init(ctx)
	return &joinBEDScanner{
		ctx:       ctx,
		parent:    t,
		mapExpr:   t.mapExpr,
		chrom:     t.chrom,
		start:     t.start,
		limit:     t.limit,
		length:    t.length,
		hasLength: t.hasLength,
		src:       t.srcTable.Scanner(ctx, start, limit, nshards)}
}

func (t *joinBEDTable) init(ctx context.Context) {
	t.once.Do(func() {
		t.featMap = readFeatureMapFromBED(ctx, t.ast, t.bedTable)
	})
}

type joinBEDScanner struct {
	ctx     context.Context
	parent  *joinBEDTable
	mapExpr *Func

	chrom, start, limit, length bedTargetField
	hasLength                   bool
	src                         TableScanner

	row      []Value
	curValue int
}

func (sc *joinBEDScanner) Value() Value { return sc.row[sc.curValue] }

func (sc *joinBEDScanner) Scan() bool {
	sc.curValue++
	if sc.curValue < len(sc.row) {
		return true
	}

	sc.curValue = 0
	matches := []*intervalmap.Entry{}
	for {
		if !sc.src.Scan() {
			return false
		}
		val := sc.src.Value()
		row := val.Struct(sc.parent.ast)

		chrom := evalBEDTargetField(sc.ctx, sc.parent.ast, row, sc.chrom).Str(sc.parent.ast)
		m, ok := sc.parent.featMap[chrom]
		if !ok {
			continue
		}

		start := evalBEDTargetField(sc.ctx, sc.parent.ast, row, sc.start).Int(sc.parent.ast)
		var limit int64
		if sc.hasLength {
			length := evalBEDTargetField(sc.ctx, sc.parent.ast, row, sc.length).Int(sc.parent.ast)
			limit = start + length
		} else {
			limit = evalBEDTargetField(sc.ctx, sc.parent.ast, row, sc.limit).Int(sc.parent.ast)
		}

		m.Get(intervalmap.Interval{Start: start, Limit: limit}, &matches)
		if len(matches) == 0 {
			continue
		}
		sc.row = sc.row[:0]
		if sc.mapExpr == nil {
			sc.row = append(sc.row, val)
			return true
		}
		var dedup map[hash.Hash]bool
		for _, match := range matches {
			bedRow := match.Data.(Value)
			mappedRow := sc.mapExpr.Eval(sc.ctx, val, bedRow)
			mappedRowHash := mappedRow.Hash()
			if len(matches) == 1 { // common case
				sc.row = append(sc.row, mappedRow)
				continue
			}
			if dedup == nil {
				dedup = map[hash.Hash]bool{}
			}
			if _, ok := dedup[mappedRowHash]; !ok {
				sc.row = append(sc.row, mappedRow)
				dedup[mappedRowHash] = true
			}
		}
		return true
	}
}

func builtinJoinBED(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	h := hash.Hash{
		0xca, 0xe6, 0xa5, 0xc2, 0x52, 0xb2, 0xee, 0x30,
		0x4f, 0x06, 0x2f, 0xc0, 0xfe, 0x75, 0x5a, 0x73,
		0x05, 0x0b, 0x2c, 0xcf, 0x81, 0xdd, 0x6a, 0x81,
		0xe5, 0x7e, 0xa9, 0xd2, 0x4f, 0x0a, 0x7c, 0x29}
	srcTable := args[0].Table()
	h = h.Merge(srcTable.Hash())
	bedTable := args[1].Table()
	h = h.Merge(bedTable.Hash())
	mapExpr := args[2].Func()
	if mapExpr != nil {
		h = h.Merge(mapExpr.Hash())
	}
	chrom := getBEDTargetField(args[3], symbol.Chrom)
	h = h.Merge(chrom.Hash())
	start := getBEDTargetField(args[4], symbol.Start)
	h = h.Merge(start.Hash())
	var length, limit bedTargetField
	hasLength := false

	if args[6].Expr != nil { // length set
		length = getBEDTargetField(args[6], symbol.Length)
		hasLength = true
		h = h.Merge(hash.Int(1))
		h = h.Merge(length.Hash())
	} else {
		limit = getBEDTargetField(args[5], symbol.End)
		h = h.Merge(hash.Int(0))
		h = h.Merge(limit.Hash())
	}
	t := &joinBEDTable{
		hash:      h,
		ast:       ast,
		srcTable:  srcTable,
		bedTable:  bedTable,
		mapExpr:   mapExpr,
		chrom:     chrom,
		start:     start,
		length:    length,
		hasLength: hasLength,
		limit:     limit,
	}
	return NewTable(t)
}

func init() {
	RegisterBuiltinFunc("joinbed",
		`
    srctable | joinbed(bedtable [, chrom:=chromexpr]
                                [, start:=startexpr]
                                [, end:=endexpr]
                                [, length:=lengthexpr]
                                [, map:=mapexpr])

Arg types:

- bedtable: table (https://uswest.ensembl.org/info/website/upload/bed.html)
- chromexpr: one-arg function (default: ::|row|row.chrom)
- startexpr: one-arg function (default: ::|row|row.start)
- endexpr: one-arg function (default: ::|row|row.end)
- lengthexpr: one-arg function (default: NA)
- mapexpr: two-arg function (srcrow, bedrow) (default: ::|srcrow,bedrow|srcrow::)

Joinbed is a special kind of join operation that's optimized for intersecting
_srctable_ with genomic intervals listed in _bedtable_.

Example:

     bed := read("test.bed")
     bc := read("test.bincount.tsv")
     out := bc | joininbed(bed, chrom:=$chromo))

Optional args _chromexpr_, _startexpr_, _endexpr_, and _lengthexpr_ specify how to extract the
coordinate values from a _srctable_ row. For example:

     bc := read("test.bincount.tsv")
     bc | joinbed(bed, chrom:=&chromo, start=&S, end=&E)

will use columns "chromo", "S", "E" in table "test.bincount.tsv" to
construct a genomic coordinate, then checks if the coordinate intersects with a
row in the bed table.

At most one of _endexpr_ or _lengthexpr_ arg can be set. If _endexpr_ is set, [_startexpr_, _endexpr_)
defines a zero-based half-open range for the given chromosome. If _lengthexpr_ is
set, [_startexpr_, _startexpr_+_lengthexpr_) defines a zero-based half-open coordinate range.  The

The BED table must contain at least three columns. The chromosome name, start
and end coordinates are extracted from the 1st, 2nd and 3rd columns,
respectively. Each coordinate range is zero-based, half-open.

Two coordinate ranges are considered to intersect if they have nonempty overlap,
that is they overlap at least one base.

_mapexpr_ describes the format of rows produced by joinbed. If _mapexpr_ is
omitted, joinbed simply emits the matched rows in _srctable_.

For example, the below example will produce rows with three columns: name, chrom
and pos.  the "name" column is taken from the "featname" in the BED row, the
"pos" column is taken "start" column of the "bc" table row.

     bc := read("test.bincount.tsv")
     bc | joinbed(bed, chrom:=&chromo, start=&S, end=&E, map:=|bcrow,bedrow|{name:bedrow.featname, pos: bcrow.start})

The below is an example of using the "row" argument. It behaves identically to the above exampel.

     bc := read("test.bincount.tsv")
     bc | joinbed(bed, row:=bcrow, chrom:=bcrow.chromo, start=bcrow.S, end=bcrow.E, map:=|bcrow,bedrow|{name:bedrow.featname, pos: bcrow.start})

`, builtinJoinBED,
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}}, // input table
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}}, // BED table
		FormalArg{Name: symbol.Map, Closure: true,
			ClosureArgs: []ClosureFormalArg{
				{symbol.AnonRow, symbol.Row},
				{symbol.Feat, symbol.Invalid}}, DefaultValue: NewFunc(nil)}, // map:=expr
		FormalArg{Name: symbol.Chrom, Closure: true, ClosureArgs: anonRowFuncArg, DefaultValue: NewFunc(nil)},
		FormalArg{Name: symbol.Start, Closure: true, ClosureArgs: anonRowFuncArg, DefaultValue: NewFunc(nil)},
		FormalArg{Name: symbol.End, Closure: true, ClosureArgs: anonRowFuncArg, DefaultValue: NewFunc(nil)},
		FormalArg{Name: symbol.Length, Closure: true, ClosureArgs: anonRowFuncArg, DefaultValue: NewFunc(nil)},
		FormalArg{Name: symbol.Row, Symbol: true, DefaultSymbol: symbol.AnonRow})
	RegisterTableUnmarshaler(joinBEDMagic, unmarshalJoinBEDTable)
}

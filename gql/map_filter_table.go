package gql

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
)

// mapFilterTable implements map() or filter().
type mapFilterTable struct {
	hashOnce sync.Once
	hash     hash.Hash

	ast ASTNode // source-code location.
	src Table   // The table to read from.
	// filter function, if nonnil, is invoked on each input row.
	// Only the rows that evaluates true will be passed to the mapper.
	filterExpr *Func
	// mapExpr, if nonnil, specifies the transformation of an input row to the
	// output row.  If nil, the input row is yielded as is.
	mapExprs []*Func

	exactLenOnce sync.Once
	exactLen     int
}

func (t *mapFilterTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	sc := &mapFilterTableScanner{
		ctx:         ctx,
		parent:      t,
		src:         t.src.Scanner(ctx, start, limit, total),
		nextMapExpr: len(t.mapExprs),
	}
	if t.filterExpr != nil {
		sc.filterExpr = t.filterExpr
	}
	sc.mapExprs = make([]*Func, len(t.mapExprs))
	for i, e := range t.mapExprs {
		sc.mapExprs[i] = e
	}
	return sc
}

var mapFilterMagic = UnmarshalMagic{0x01, 0x7b}

// marshalMapFilterTable marshals the state of map expression so that it can be
// transported to another machine for use by bigslice.  "ast" is used only when
// reporting errors.
func marshalMapFilterTable(ctx MarshalContext, enc *marshal.Encoder, hash hash.Hash, ast ASTNode, src Table, filterExpr *Func, mapExprs []*Func) {
	enc.PutRawBytes(mapFilterMagic[:])
	enc.PutHash(hash)
	enc.PutGOB(&ast)
	src.Marshal(ctx, enc)
	filterExpr.Marshal(ctx, enc)
	enc.PutVarint(int64(len(mapExprs)))
	for _, e := range mapExprs {
		e.Marshal(ctx, enc)
	}
}

func (t *mapFilterTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	marshalMapFilterTable(ctx, enc, t.Hash(), t.ast, t.src, t.filterExpr, t.mapExprs)
}

func unmarshalMapFilterTable(ctx UnmarshalContext, hash hash.Hash, dec *marshal.Decoder) Table {
	var ast ASTNode
	dec.GOB(&ast)
	t := &mapFilterTable{
		hash: hash,
		ast:  ast,
		src:  unmarshalTable(ctx, dec),
	}
	t.filterExpr = unmarshalFunc(ctx, dec)
	n := int(dec.Varint())
	t.mapExprs = make([]*Func, n)
	for i := 0; i < n; i++ {
		t.mapExprs[i] = unmarshalFunc(ctx, dec)
	}
	return t
}

func (t *mapFilterTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		return t.src.Len(ctx, Approx)
	}
	t.exactLenOnce.Do(func() {
		t.exactLen = DefaultTableLen(ctx, t)
	})
	return t.exactLen
}

func (t *mapFilterTable) Prefetch(ctx context.Context) { t.src.Prefetch(ctx) }

func (t *mapFilterTable) Hash() hash.Hash {
	t.hashOnce.Do(func() {
		if t.hash == hash.Zero { // hash != Zero if it is unmarshalled on a remote machine.
			t.hash = hashMapFilterTable(t.src, t.filterExpr, t.mapExprs)
		}
	})
	return t.hash
}

func (t *mapFilterTable) Attrs(ctx context.Context) TableAttrs {
	srcAttrs := t.src.Attrs(ctx)
	mapDesc := strings.Builder{}
	mapDesc.WriteByte('(')
	for i, e := range t.mapExprs {
		if i > 0 {
			mapDesc.WriteByte(',')
		}
		mapDesc.WriteString(e.String())
	}
	mapDesc.WriteByte(')')
	return TableAttrs{
		Name: "mapfilter",
		Path: srcAttrs.Path,
		Description: fmt.Sprintf("src:=%s, filter:=%v, map:=%s",
			srcAttrs.Description,
			t.filterExpr,
			mapDesc.String()),
	}
}

func (t *mapFilterTable) String() string { return "mapfiltertable" }

type mapFilterTableScanner struct {
	ctx    context.Context
	parent *mapFilterTable

	filterExpr  *Func
	mapExprs    []*Func
	nextMapExpr int
	src         TableScanner
	row         Value
}

func (sc *mapFilterTableScanner) Value() Value { return sc.row }

func (sc *mapFilterTableScanner) Scan() bool {
	if sc.nextMapExpr < len(sc.mapExprs) {
		sc.row = sc.mapExprs[sc.nextMapExpr].Eval(sc.ctx, sc.src.Value())
		sc.nextMapExpr++
		return true
	}
	for {
		if !sc.src.Scan() {
			return false
		}
		row := sc.src.Value()
		if sc.filterExpr != nil {
			if !sc.filterExpr.Eval(sc.ctx, row).Bool(sc.filterExpr.ast) {
				continue
			}
		}
		if len(sc.mapExprs) > 0 {
			sc.row = sc.mapExprs[0].Eval(sc.ctx, row)
			sc.nextMapExpr = 1
		} else {
			sc.row = row
		}
		return true
	}
}

// NewMapFilterTable creates a Table that implements map() or filter().  It
// reads from srcTable. It removes rows for which filterExpr(row)=false. If
// filterExpr=nil, then it is assumed to be just "true". Then it emits expr(row)
// for each expr∈mapExprs. If len(mapExprs)=0, then it emits each matched row as
// is.
//
// if nshards>0, the table runs on multiple machines using bigslice.
func NewMapFilterTable(
	ctx context.Context,
	ast ASTNode,
	srcTable Table,
	filterExpr *Func, /*maybe null, defaults to true */
	mapExprs []*Func, /*maybe nil, defaults to an ID transformation */
	nshards int /*<=0 for sequential execution*/) Value {
	if nshards <= 0 {
		t := &mapFilterTable{
			ast:        ast,
			src:        srcTable,
			filterExpr: filterExpr,
			mapExprs:   mapExprs,
		}
		return NewTable(t)
	}

	var tableBuf marshal.Encoder
	mctx := newMarshalContext(ctx)

	// TODO(saito) don't compute hash here. Do it in marshal...
	hash := hashMapFilterTable(srcTable, filterExpr, mapExprs)
	marshalMapFilterTable(mctx, &tableBuf, hash, ast, srcTable, filterExpr, mapExprs)
	t := &parallelMapFilterTable{
		hash:            hash,
		ast:             ast,
		src:             srcTable,
		filterExpr:      filterExpr,
		mapExprs:        mapExprs,
		nshards:         nshards,
		marshalledEnv:   mctx.marshal(),
		marshalledTable: tableBuf.Bytes(),
	}
	return NewTable(t)
}

func init() {
	RegisterTableUnmarshaler(mapFilterMagic, unmarshalMapFilterTable)
}

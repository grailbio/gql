package gql

import (
	"context"
	"sync"

	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
)

// firstNTable implements a Table that does filter, then map.
type firstNTable struct {
	hashOnce sync.Once
	hash     hash.Hash // hash of the inputs.

	ast  ASTNode // source-code location
	name string  // optional table name; reported in Attr().
	src  Table   // The table to read from.
	n    int     // # of rows to emit.

	lenOnce sync.Once
	len     int
}

// Scanner implements the Table interface.
func (t *firstNTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	if start > 0 {
		return &NullTableScanner{}
	}
	sc := &firstNTableScanner{sc: t.src.Scanner(ctx, 0, 1, 1), remaining: t.n}
	return sc
}

var firstNMagic = UnmarshalMagic{0xa5, 0x4a}

// Marshal implements the Table interface.
func (t *firstNTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	enc.PutRawBytes(firstNMagic[:])
	enc.PutHash(t.Hash())
	enc.PutGOB(&t.ast)
	t.src.Marshal(ctx, enc)
	enc.PutVarint(int64(t.n))
}

// unmarshalFirstNTable reconstructs the table serialized by Marshal.
func unmarshalFirstNTable(ctx UnmarshalContext, hash hash.Hash, dec *marshal.Decoder) Table {
	var ast ASTNode
	dec.GOB(&ast)
	t := &firstNTable{
		hash: hash,
		ast:  ast,
		src:  unmarshalTable(ctx, dec),
	}
	t.n = int(dec.Varint())
	return t
}

// Len implements the Table interface.
func (t *firstNTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		return t.n
	}
	t.lenOnce.Do(func() { t.len = DefaultTableLen(ctx, t) })
	return t.len
}

// Prefetch implements the Table interface.
func (t *firstNTable) Prefetch(ctx context.Context) { t.src.Prefetch(ctx) }

// Hash implements the Table interface.
func (t *firstNTable) Hash() hash.Hash {
	t.hashOnce.Do(func() {
		if t.hash == hash.Zero { // hash != Zero if it is unmarshalled on a remote machine.
			h := hash.Hash{
				0x16, 0x45, 0x7f, 0x88, 0x04, 0xe4, 0x54, 0xb0,
				0x4b, 0x6c, 0x4f, 0xe4, 0xb1, 0x33, 0xe7, 0xbd,
				0x4d, 0x47, 0xf9, 0xe8, 0x21, 0x93, 0x9a, 0x01,
				0x75, 0x9f, 0x5d, 0x1d, 0x4b, 0xec, 0x5a, 0x1a}
			h = h.Merge(t.src.Hash())
			t.hash = h.Merge(hash.Int(int64(t.n)))
		}
	})
	return t.hash
}

// Attrs implements the Table interface.
func (t *firstNTable) Attrs(ctx context.Context) TableAttrs {
	attrs := t.src.Attrs(ctx)
	attrs.Name = t.name
	return attrs
}

// firstNTableScanner is a TableScanner implementation for firstNTable.
type firstNTableScanner struct {
	sc        TableScanner
	remaining int
}

// Scan implements the TableScanner interface.
func (t *firstNTableScanner) Scan() bool {
	if t.remaining <= 0 {
		return false
	}
	t.remaining--
	return t.sc.Scan()
}

// Value implements the TableScanner interface.
func (t *firstNTableScanner) Value() Value { return t.sc.Value() }

func init() {
	RegisterTableUnmarshaler(firstNMagic, unmarshalFirstNTable)
	RegisterBuiltinFunc("firstn",
		`
    tbl | firstn(n)

Arg types:

- _n_: int

Firstn produces a table that contains the first _n_ rows of the input table.
`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			table := args[0].Table()
			n := args[1].Int()
			return NewTable(&firstNTable{
				ast: ast,
				src: table,
				n:   int(n),
			})
		}, func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{TableType}}, // table
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}})   // n
}

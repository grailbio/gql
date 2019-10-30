package gql_test

// Tests for user-defined tables types and values in gql.

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/testutil/h"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/gqltest"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

type udTestTable struct {
	tableHash hash.Hash
	nRow      int

	key0, key1 symbol.ID
}

type udTestTableScanner struct {
	parent *udTestTable
	// The current row index being served
	current int
	// The limit row for this iterator. Exclusive.
	limit int
	value gql.Value
}

func newUDTestTable(tableHash hash.Hash, nRow int) *udTestTable {
	return &udTestTable{
		tableHash: tableHash,
		nRow:      nRow,
		key0:      symbol.Intern("key0"),
		key1:      symbol.Intern("key1"),
	}
}

func (t *udTestTable) Attrs(ctx context.Context) gql.TableAttrs {
	return gql.TableAttrs{Name: "ud"}
}

func (t *udTestTable) Scanner(ctx context.Context, start, limit, total int) gql.TableScanner {
	startRow := start * t.nRow / total
	limitRow := limit * t.nRow / total
	return &udTestTableScanner{parent: t, current: startRow - 1, limit: limitRow}
}

func (t *udTestTable) Len(ctx context.Context, mode gql.CountMode) int { return t.nRow }

func (t *udTestTable) Hash() hash.Hash { return t.tableHash }

func (t *udTestTable) Prefetch(ctx context.Context) {}

func (t *udTestTable) Marshal(ctx gql.MarshalContext, enc *marshal.Encoder) {
	enc.PutRawBytes(udTableMagic[:])
	enc.PutHash(t.tableHash)
	enc.PutVarint(int64(t.nRow))
}

func unmarshalUDTestTable(ctx gql.UnmarshalContext, hash hash.Hash, dec *marshal.Decoder) gql.Table {
	nRow := int(dec.Varint())
	return newUDTestTable(hash, nRow)
}

var udTableMagic = gql.UnmarshalMagic{0x36, 0x8f}

func init() {
	gql.RegisterTableUnmarshaler(udTableMagic, unmarshalUDTestTable)
}

func (s *udTestTableScanner) Scan() bool {
	s.current++
	if s.current >= s.limit {
		return false
	}
	s.value = gql.NewStruct(gql.NewSimpleStruct(
		gql.StructField{Name: s.parent.key0, Value: gql.NewInt(int64(s.current + 1000))},
		gql.StructField{Name: s.parent.key1, Value: gql.NewString(fmt.Sprintf("str%d", s.current+2000))}))
	return true
}

func (s *udTestTableScanner) Value() gql.Value { return s.value }

func TestUserDefinedTable(t *testing.T) {
	table := newUDTestTable(hash.Int(0), 3)
	sess := gqltest.NewSession()
	sess.SetGlobal("udtable", gql.NewTable(table))
	expect.That(t,
		gqltest.ReadTable(gqltest.Eval(t, "udtable", sess)),
		h.ElementsAre(
			"{key0:1000,key1:str2000}",
			"{key0:1001,key1:str2001}",
			"{key0:1002,key1:str2002}"))
}

// Test bigslice-based reduce using a user-defined table
func TestUserDefinedTableReduce(t *testing.T) {
	table := newUDTestTable(hash.Int(0), 100)
	sess := gqltest.NewSession()
	sess.SetGlobal("udtable", gql.NewTable(table))
	expect.That(t,
		// Reduce by the last digit of the string key.
		gqltest.ReadTable(gqltest.Eval(t,
			`udtable | reduce({regexp_replace($key1, "^str...(.).*", "$1")}, _acc+_val, map:=$key0, shards:=1)`, sess)),
		h.WhenSorted(h.ElementsAre(
			"{key:{f0:0},value:10450}",
			"{key:{f0:1},value:10460}",
			"{key:{f0:2},value:10470}",
			"{key:{f0:3},value:10480}",
			"{key:{f0:4},value:10490}",
			"{key:{f0:5},value:10500}",
			"{key:{f0:6},value:10510}",
			"{key:{f0:7},value:10520}",
			"{key:{f0:8},value:10530}",
			"{key:{f0:9},value:10540}")))
}

func TestUserDefinedFunction(t *testing.T) {
	sess := gqltest.NewSession()
	gql.RegisterBuiltinFunc("isprime",
		`Usage: isprime(n)
Example:
    isprime(11) == true
    isprime(12) == false`,
		func(ctx context.Context, ast gql.ASTNode, args []gql.ActualArg) gql.Value {
			val := args[0].Value.Int(nil)
			sqrt := int(math.Sqrt(float64(val)))
			for div := 2; div < sqrt; div++ {
				if val%int64(div) == 0 {
					return gql.False
				}
			}
			return gql.True
		},
		func(ast gql.ASTNode, args []gql.AIArg) gql.AIType {
			return gql.AIIntType
		},
		gql.FormalArg{Positional: true, Required: true, Types: []gql.ValueType{gql.IntType}})

	expect.True(t, gqltest.Eval(t, "isprime(2)", sess).Bool(nil))
	expect.False(t, gqltest.Eval(t, "isprime(10)", sess).Bool(nil))
	expect.True(t, gqltest.Eval(t, "isprime(11)", sess).Bool(nil))
}

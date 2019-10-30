package gql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/symbol"
)

func TestBAMTableScanner(t *testing.T) {
	t.Parallel()
	const path = "./testdata/test.bam"
	ctx := context.Background()
	tab := gql.NewBAMTable(path, &gql.ASTUnknown{}, hash.String(path))
	require.Equal(t, tab.Len(ctx, gql.Exact), 117563)
	scan := tab.Scanner(context.Background(), 0, 1, 1)
	require.True(t, scan.Scan())
	v := scan.Value().Struct(nil)
	fields := map[symbol.ID]gql.Value{
		symbol.Name:         gql.NewString("E00570:81:HH7GMALXX:7:1113:12672:2381:ATCGCC+GTTCCA"),
		symbol.Flags:        gql.NewInt(99),
		symbol.Reference:    gql.NewString("chr1"),
		symbol.Pos:          gql.NewInt(10008),
		symbol.MapQ:         gql.NewChar(60),
		symbol.Cigar:        gql.NewString("4S136M1S"),
		symbol.RNext:        gql.NewString("="),
		symbol.PNext:        gql.NewInt(10106),
		symbol.TLen:         gql.NewInt(138),
		symbol.Seq:          gql.NewString("CCCCAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTACCCCAACCCTAACCCTAACCCTAACCCTAACCCCAACCCC"),
		symbol.Qual:         gql.NewString("77AAF-7AFJFAFA<AJFJFJA7--F<AJJJJJFJ7<JJ<7AJ<<-FFAJJ-A<AJF----FAF-FFJFAJJ<-A-A7AJJ7J7-F7--7-AF<FJ7JJ--77F---7FJ7A<-AF-<-7<---7-FF-7--77)777F7)"),
		symbol.Intern("NM"): gql.NewInt(3),
		symbol.Intern("MD"): gql.NewString("53T43A32T5"),
		symbol.Intern("AS"): gql.NewInt(121),
		symbol.Intern("XS"): gql.NewInt(110),
		symbol.Intern("RG"): gql.NewString("1.5"),
		symbol.Intern("XX"): gql.Null,
	}
	for sym, want := range fields {
		got, ok := v.Value(sym)
		if !ok {
			if want.Type() != gql.NullType {
				t.Errorf("%s: got null, want %v", sym.Str(), want)
			}
			continue
		}
		if gql.Compare(nil, got, want) != 0 {
			t.Errorf("%v: got %v, want %v", sym.Str(), got, want)
		}
	}
}

func TestShardedBAMTableScanner(t *testing.T) {
	t.Parallel()
	oldMaxShards := gql.BAMTableMaxShards
	gql.BAMTableMaxShards = 20
	defer func() { gql.BAMTableMaxShards = oldMaxShards }()
	const path = "./testdata/test.bam"
	tab := gql.NewBAMTable(path, &gql.ASTUnknown{}, hash.String(path))

	whole := appendScanner(nil, tab.Scanner(context.Background(), 0, 1, 1))
	for _, n := range []int{10, 30 /*scaled*/} {
		var sharded []gql.Value
		for i := 0; i < n; i++ {
			sharded = appendScanner(sharded, tab.Scanner(context.Background(), i, i+1, n))
		}
		require.Equal(t, len(whole), len(sharded))
		for i := range whole {
			require.True(t, gql.Compare(nil, whole[i], sharded[i]) == 0)
		}
	}
}

func appendScanner(elems []gql.Value, scan gql.TableScanner) []gql.Value {
	for scan.Scan() {
		elems = append(elems, scan.Value())
	}
	return elems
}

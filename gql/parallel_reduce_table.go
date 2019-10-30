package gql

import (
	"context"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/spaolacci/murmur3"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

var parallelReduceFunc = bigslice.Func(func(
	marshaledConfig []byte,
	tableHash hash.Hash,
	outBTSVPath string,
	marshaledSrcTable []byte,
	nshards int) (slice bigslice.Slice) {
	ctx := newUnmarshalContext(marshaledConfig)
	rtable := unmarshalTable(ctx, marshal.NewDecoder(marshaledSrcTable)).(*parallelReduceTable)
	ast := rtable.ast
	srcTable := rtable.src
	keyExpr := rtable.keyExpr
	reduceExpr := rtable.reduceExpr
	mapExpr := rtable.mapExpr

	type shardState struct {
		scanner          TableScanner
		nrows            int
		keyExpr, mapExpr *Func
	}

	slice = bigslice.ReaderFunc(nshards,
		func(shard int, scanner **shardState, keys, rows []Value) (n int, err error) {
			if *scanner == nil {
				Logf(ast, "start shard %d/%d for table %+v", shard, nshards, srcTable.Attrs(ctx.ctx))
				*scanner = &shardState{
					scanner: srcTable.Scanner(ctx.ctx, shard, shard+1, nshards),
					keyExpr: keyExpr,
				}
				if mapExpr != nil {
					(*scanner).mapExpr = mapExpr
				}
			}
			if len(keys) != len(rows) {
				Panicf(ast, "keys and values don't match %d %d", len(keys), len(rows))
			}
			for i := range rows {
				if !(*scanner).scanner.Scan() {
					return i, sliceio.EOF
				}
				(*scanner).nrows++
				if (*scanner).nrows%10000000 == 0 {
					Logf(ast, "read %+v: shard %d/%d: %d rows", srcTable.Attrs(ctx.ctx), shard, nshards, (*scanner).nrows)
				}
				row := (*scanner).scanner.Value()
				key := (*scanner).keyExpr.Eval(ctx.ctx, row)
				if (*scanner).mapExpr != nil {
					row = (*scanner).mapExpr.Eval(ctx.ctx, row)
				}
				rows[i] = row
				keys[i] = key
			}
			return len(rows), nil
		})
	slice = bigslice.Reduce(slice, func(acc, m Value) Value {
		if !acc.Valid() || !m.Valid() {
			Panicf(ast, "null %v %v", acc, m)
		}
		val := reduceExpr.Eval(ctx.ctx, acc, m)
		return val
	})
	slice = bigslice.Scan(slice, func(shard int, scan *sliceio.Scanner) error {
		Logf(ast, "write shard %d/%d in %v", shard, nshards, outBTSVPath)
		w := NewBTSVShardWriter(ctx.ctx, outBTSVPath, shard, nshards, TableAttrs{})
		var key, acc Value
		nrows := 0
		for scan.Scan(ctx.ctx, &key, &acc) {
			row := NewStruct(NewSimpleStruct(
				StructField{Name: symbol.Key, Value: key},
				StructField{Name: symbol.Value, Value: acc}))
			w.Append(row)
			nrows++
		}
		if err := scan.Err(); err != nil {
			Panicf(ast, "%v: %v", outBTSVPath, err)
		}
		Logf(ast, "wrote %d rows in %d/%d in %v", nrows, shard, nshards, outBTSVPath)
		w.Close(ctx.ctx)
		return nil
	})
	return
})

// parallelReduceTable implements a table that does filter, then map.
type parallelReduceTable struct {
	hash hash.Hash
	ast  ASTNode // source-code location
	// The table to read from.
	src Table
	// Bindings for keyExpr and redcueExpr
	keyExpr    *Func
	reduceExpr *Func
	mapExpr    *Func

	// # of bigslice shards to run.
	nshards int

	marshalledEnv, marshalledTable []byte

	once      sync.Once
	btsvTable Table

	lenOnce sync.Once
	len     int
}

var parallelReduceMagic = UnmarshalMagic{0xff, 0x33}

func marshalParallelReduceTable(ctx MarshalContext, enc *marshal.Encoder, hash hash.Hash, ast ASTNode, src Table, keyExpr, reduceExpr, mapExpr *Func) {
	enc.PutRawBytes(parallelReduceMagic[:])
	enc.PutHash(hash)
	enc.PutGOB(&ast)
	src.Marshal(ctx, enc)
	keyExpr.Marshal(ctx, enc)
	reduceExpr.Marshal(ctx, enc)
	mapExpr.Marshal(ctx, enc)
}

func (t *parallelReduceTable) init(ctx context.Context) {
	t.once.Do(func() {
		cacheName := t.hash.String() + ".btsv"
		btsvPath, found := LookupCache(ctx, cacheName)
		if found {
			Logf(t.ast, "cache hit: %s", btsvPath)
		} else {
			Logf(t.ast, "start bigslice for table %v", t.hash)
			if _, err := bsSession.Run(ctx, parallelReduceFunc, t.marshalledEnv, t.hash, btsvPath, t.marshalledTable, t.nshards); err != nil {
				log.Panic(err)
			}
			ActivateCache(ctx, cacheName, btsvPath)
			Logf(t.ast, "finished bigslice for table %v", t.hash)
		}
		t.btsvTable = NewBTSVTable(btsvPath, t.ast, t.hash)
	})
}

func (t *parallelReduceTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		return 100000
	}
	t.lenOnce.Do(func() { t.len = DefaultTableLen(ctx, t) })
	return t.len
}

func (t *parallelReduceTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	marshalParallelReduceTable(ctx, enc, t.hash, t.ast, t.src, t.keyExpr, t.reduceExpr, t.mapExpr)
}

func (t *parallelReduceTable) Attrs(ctx context.Context) TableAttrs {
	return TableAttrs{Name: "parallelreduce", Path: t.src.Attrs(ctx).Path}
}

func (t *parallelReduceTable) Prefetch(ctx context.Context) { t.init(ctx) }
func (t *parallelReduceTable) Hash() hash.Hash              { return t.hash }

func (t *parallelReduceTable) Scanner(ctx context.Context, start, limit, nshards int) TableScanner {
	t.init(ctx)
	return t.btsvTable.Scanner(ctx, start, limit, nshards)
}

func unmarshalParallelReduceTable(ctx UnmarshalContext, hash hash.Hash, dec *marshal.Decoder) Table {
	var ast ASTNode
	dec.GOB(&ast)
	t := &parallelReduceTable{
		hash: hash,
		ast:  ast,
		src:  unmarshalTable(ctx, dec),
	}
	t.keyExpr = unmarshalFunc(ctx, dec)
	t.reduceExpr = unmarshalFunc(ctx, dec)
	t.mapExpr = unmarshalFunc(ctx, dec)
	return t
}

func init() {
	RegisterTableUnmarshaler(parallelReduceMagic, unmarshalParallelReduceTable)

	frame.RegisterOps(func(vs []Value) frame.Ops {
		return frame.Ops{
			Less: func(i, j int) bool { return Compare(nil, vs[i], vs[j]) < 0 },
			HashWithSeed: func(i int, seed uint32) uint32 {
				// GQL's hash does not appear to provide enough entropy in the
				// low order bits to be used in this context; we re-hash with
				// murmur3 here to mitigate.
				h := vs[i].Hash()
				return murmur3.Sum32WithSeed(h[:], seed)
			},
		}
	})
}

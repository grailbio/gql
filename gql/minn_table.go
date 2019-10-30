package gql

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

var (
	// They are really private consts, but are exposed for testing.

	// MinNMinRowsPerShard is the minimum number of rows per sortshard.
	MinNMinRowsPerShard = 1 << 18
	// MinNMaxRowsPerShard is the maximum number of rows per sortshard.
	MinNMaxRowsPerShard = 1 << 20
	// MinNParallelism is the maximum number of goroutines to create during
	// sorting.
	MinNParallelism = runtime.NumCPU() * 2
)

var parallelMinNFunc = bigslice.Func(func(marshalledEnv []byte, tableHash hash.Hash, marshalledTable []byte,
	minn int64, nshards int) (slice bigslice.Slice) {
	type shardState struct {
		ch      chan string // Emits pathnames of the recordio containing sorted records.
		sortKey *Func    // thread-local copy of the sortkey closure.
	}
	ctx := newUnmarshalContext(marshalledEnv)
	dec := marshal.NewDecoder(marshalledTable)
	var ast ASTNode
	dec.GOB(&ast)
	table := unmarshalTable(ctx, dec)
	sortKey := unmarshalFunc(ctx, dec)
	if l := dec.Len(); l > 0 {
		Panicf(ast, "%d byte junk found in table", l)
	}

	// Read the table shard and dump them in a set of btsv files. Each btsv file
	// contains a sorted list of values in the shard, such that the union of
	// values in the btsv files equal the values in the original shard. The
	// resulting slice stores btsv pathnames.
	slice = bigslice.ReaderFunc(nshards,
		func(shard int, state **shardState, tmpBTSVFiles []string) (n int, err error) {
			if *state == nil { // the first call for this shard
				*state = &shardState{
					ch:      make(chan string, 16),
					sortKey: sortKey,
				}
				go func() {
					for _, path := range sortShard(ctx.ctx, astUnknown, tableHash, table, (*state).sortKey, minn, shard, nshards) {
						(*state).ch <- path
					}
					close((*state).ch)
				}()
			}
			for i := 0; i < len(tmpBTSVFiles); i++ {
				var ok bool
				if tmpBTSVFiles[i], ok = <-(*state).ch; !ok {
					return i, sliceio.EOF
				}
			}
			return len(tmpBTSVFiles), nil
		},
	)
	return
})

// minnTable is a Table implementation that picks N smallest elements from of
// the source table.  It does an external mergesort.
type minnTable struct {
	hash     hash.Hash // hash of inputs to the minntable.
	ast      ASTNode   // source-code location
	attrs    TableAttrs
	srcTable Table    // table to read rows from.
	sortKey  *Func // computes the sort key from each row.
	minn     int64    // # of rows to retain.
	shards   int      // If >0, do distributed mergesort using bigslice.

	marshalledEnv, marshalledTable []byte

	once      sync.Once
	btsvTable Table

	// For computing Len(Exact) value.
	exactLenOnce sync.Once
	exactLen     int
}

type minnElem struct {
	rec     Value // Record read from the source table.
	sortKey Value // Created by invoking the sortkey callback.
}

// This function sorts the shard (out of nshards) of src table.  It invokes
// sortExpr for each input row and uses the result to sort rows. In the end, it
// creates >=1 btsv tables, each containing a sorted list of rows in the
// shard. The union of rows in the btsv tables equals the rows in the srctable
// shard.
//
// It returns a list of btsv files. Rows in each btsv file is sorted.
func sortShard(ctx context.Context, ast ASTNode, hash hash.Hash, src Table, sortExpr *Func, minn int64, shard, nshards int) []string {
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex // guards the next two variables.
		minRows   []minnElem
		btsvPaths []string
	)

	Debugf(ast, "minn: start shard %d/%d", shard, nshards)
	tmpID := int32(0)
	saveRowsToTempFile := func(rows []minnElem) {
		tmpPath := generateUniqueCachePath(
			fmt.Sprintf("%s-minn-tmp-%06d-%06d-%06d.btsv", hash, atomic.AddInt32(&tmpID, 1), shard, nshards))
		Debugf(ast, "minn: shard %d/%d creating %s", shard, nshards, tmpPath)
		w := NewBTSVShardWriter(ctx, tmpPath, 0, 1, TableAttrs{})
		for _, row := range rows {
			w.Append(NewStruct(NewSimpleStruct(
				StructField{Name: symbol.Value, Value: row.rec},
				StructField{Name: symbol.Key, Value: row.sortKey})))
		}
		w.Close(ctx)
		mu.Lock()
		btsvPaths = append(btsvPaths, tmpPath)
		mu.Unlock()
	}

	// Runs in a separate goroutine
	flushTmpRows := func(tmpRows []minnElem) {
		defer wg.Done()
		sort.SliceStable(tmpRows, func(i, j int) bool {
			return Compare(astUnknown /*TODO:fix*/, tmpRows[i].sortKey, tmpRows[j].sortKey) < 0
		})
		if int64(len(tmpRows)) < minn {
			// Likely a full sorting of the srctable is requested. Dump the tmprows to
			// a btsv table.
			saveRowsToTempFile(tmpRows)
			return
		}
		// The minn param is smaller than the input table. Keep the minn elements in
		// memory and read the rest of the srctable.
		tmpRows = tmpRows[:minn]
		mu.Lock()
		minRows = append(minRows, tmpRows...)
		sort.SliceStable(minRows, func(i, j int) bool {
			return Compare(ast, minRows[i].sortKey, minRows[j].sortKey) < 0
		})
		if int64(len(minRows)) > minn {
			minRows = minRows[:minn]
		}
		mu.Unlock()
	}

	sc := src.Scanner(ctx, shard, shard+1, nshards)
	nRows := 0
	tmpRows := []minnElem{}
	for sc.Scan() {
		nRows++
		rec := sc.Value()
		sortKey := sortExpr.Eval(ctx, rec)
		tmpRows = append(tmpRows, minnElem{rec, sortKey})
		if len(tmpRows) >= MinNMaxRowsPerShard {
			Logf(ast, "shard %d/%d, %d rows read", shard, nshards, nRows)
			wg.Add(1)
			go flushTmpRows(tmpRows)
			tmpRows = nil
		}
	}
	if len(tmpRows) > 0 {
		wg.Add(1)
		go flushTmpRows(tmpRows)
	}
	wg.Wait()
	if len(minRows) > 0 {
		saveRowsToTempFile(minRows)
	}
	Logf(ast, "finished shard %d/%d, %d rows, results in %d btsv files", shard, nshards, nRows, len(btsvPaths))
	return btsvPaths
}

// minnInputQueue performs an N-way merge sort.
type minnInputQueue []TableScanner

func (q minnInputQueue) Len() int { return len(q) }
func (q minnInputQueue) Less(i, j int) bool {
	iv := q[i].Value().Struct(astUnknown).Field(1) // field 0 is rec, 1 is sortkey
	jv := q[j].Value().Struct(astUnknown).Field(1)
	c := Compare(nil, iv.Value, jv.Value)
	return c < 0
}

func (q minnInputQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *minnInputQueue) Push(x interface{}) {
	*q = append(*q, x.(TableScanner))
}

func (q *minnInputQueue) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

// Attrs implements the Table interface
func (t *minnTable) Attrs(ctx context.Context) TableAttrs { return t.attrs }

// Hash implements the Table interface
func (t *minnTable) Hash() hash.Hash { return t.hash }

// Len implements the Table interface
func (t *minnTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		return int(t.minn)
	}
	t.exactLenOnce.Do(func() {
		t.exactLen = DefaultTableLen(ctx, t)
	})
	return t.exactLen
}

// Prefetch implements the Table interface
func (t *minnTable) Prefetch(ctx context.Context) { t.init(ctx) }

// MarshalGOB implements the Table interface
func (t *minnTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	t.init(ctx.ctx)
	t.btsvTable.Marshal(ctx, enc)
}

func (t *minnTable) init(ctx context.Context) {
	t.once.Do(func() {
		cacheName := t.hash.String() + ".btsv"
		btsvPath, found := LookupCache(ctx, cacheName)
		if found {
			Logf(t.ast, "cache hit: %s", btsvPath)
			t.btsvTable = NewBTSVTable(btsvPath, t.ast, t.hash)
			return
		}
		var tmpPaths []string
		if t.shards == 0 {
			tmpPaths = t.initLocally(ctx)
		} else {
			tmpPaths = t.initWithBigSlice(ctx)
		}
		sort.Strings(tmpPaths) // make the output as deterministic.
		pq := make(minnInputQueue, len(tmpPaths))
		tmpTables := make([]Table, len(tmpPaths))
		traverse.Parallel.Each(len(tmpPaths), func(i int) error { // nolint: errcheck
			tmpTables[i] = NewBTSVTable(tmpPaths[i], t.ast, t.hash.Merge(hash.String(tmpPaths[i])))
			pq[i] = tmpTables[i].Scanner(ctx, 0, 1, 1)
			if !pq[i].Scan() {
				pq[i] = nil
			}
			return nil
		})
		// Remove the scanners that have already reached EOF.
		j := 0
		for i := range pq {
			if pq[i] != nil {
				pq[j] = pq[i]
				j++
			}
		}
		pq = pq[:j]
		heap.Init(&pq)

		w := NewBTSVShardWriter(ctx, btsvPath, 0, 1, t.attrs)
		nRowsRead := int64(0)
		for nRowsRead < t.minn && len(pq) > 0 {
			row := pq[0].Value().Struct(t.ast).Field(0).Value
			w.Append(row)
			child := heap.Pop(&pq).(TableScanner)
			if child.Scan() {
				heap.Push(&pq, child)
			}
			nRowsRead++
		}
		w.Close(ctx)
		ActivateCache(ctx, cacheName, btsvPath)
		traverse.Each(len(tmpPaths), func(i int) error { // nolint:errcheck
			if err := file.RemoveAll(ctx, tmpPaths[i]); err != nil {
				Errorf(t.ast, "remove %s: %v", tmpPaths[i], err)
			}
			return nil
		})
		t.btsvTable = NewBTSVTable(btsvPath, t.ast, t.hash)
	})
}

// InitLocally creates a set of sorted btsv files. These files are merged during
// scans. It is invoked when shards:=0.
func (t *minnTable) initLocally(ctx context.Context) []string {
	n := t.srcTable.Len(ctx, Approx)/MinNMinRowsPerShard + 1
	if n > MinNParallelism {
		n = MinNParallelism
	}
	var (
		mu       sync.Mutex
		tmpPaths []string
	)
	traverse.Each(n, func(shard int) error { // nolint:errcheck
		paths := sortShard(ctx, t.ast, t.hash, t.srcTable, t.sortKey, t.minn, shard, n)
		mu.Lock()
		tmpPaths = append(tmpPaths, paths...)
		mu.Unlock()
		return nil
	})
	return tmpPaths
}

// initWithBigSlice creates a set of sorted btsv files. These files are merged during
// scans. It is invoked when shards>0.
func (t *minnTable) initWithBigSlice(ctx context.Context) []string {
	// Run bigslice.
	result, err := bsSession.Run(ctx, parallelMinNFunc, t.marshalledEnv, t.hash, t.marshalledTable, t.minn, t.shards)
	if err != nil {
		log.Panic(err)
	}

	// parallelMinNFunc produces recordio files that lists btsv pathnames.  read
	// them into tmpPaths.
	var (
		tmpPaths []string
		tmpPath  string
	)
	sc := result.Scan(ctx)
	for sc.Scan(ctx, &tmpPath) {
		tmpPaths = append(tmpPaths, tmpPath)
	}
	if err := sc.Err(); err != nil {
		Panicf(t.ast, "minn: %v", err)
	}
	Logf(t.ast, "tables %v", tmpPaths)
	return tmpPaths
}

// Scanner implements the TableScanner interface.
func (t *minnTable) Scanner(ctx context.Context, start, limit, nshards int) TableScanner {
	t.init(ctx)
	return t.btsvTable.Scanner(ctx, start, limit, nshards)
}

// NewMinNTable creates a table that yields the smallest minn rows in
// srcTable. If minn<0, it is treated as ∞. The row order is determined by
// applying sortKey to each row, then comparing the results lexicographically.
func NewMinNTable(ctx context.Context, ast ASTNode, attrs TableAttrs, srcTable Table, sortKey *Func, minn int64, shards int) Table {
	h := hash.Hash{
		0x77, 0x27, 0x9e, 0x46, 0x7d, 0xc0, 0x27, 0x1c,
		0x0f, 0x20, 0xff, 0x5d, 0xd7, 0x0d, 0x96, 0xb4,
		0x02, 0x22, 0x94, 0x53, 0xa6, 0x98, 0xd5, 0x9a,
		0x48, 0x9b, 0x40, 0xfd, 0x98, 0x9d, 0xf1, 0x71}
	h = h.Merge(srcTable.Hash())
	h = h.Merge(hash.Int(minn))
	h = h.Merge(sortKey.Hash())
	h = h.Merge(hash.Int(int64(shards)))
	if minn < 0 {
		minn = math.MaxInt64
	}

	var marshalledEnv, marshalledTable []byte

	if shards > 0 {
		ctx := newMarshalContext(ctx)
		enc := marshal.NewEncoder(nil)
		enc.PutGOB(&ast)
		srcTable.Marshal(ctx, enc)
		sortKey.Marshal(ctx, enc)
		marshalledTable = marshal.ReleaseEncoder(enc)
		marshalledEnv = ctx.marshal()
	}
	return &minnTable{hash: h, ast: ast, attrs: attrs, srcTable: srcTable, sortKey: sortKey, minn: minn, marshalledEnv: marshalledEnv, marshalledTable: marshalledTable, shards: shards}
}

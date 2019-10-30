package gql

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"runtime"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/simd"
	gunsafe "github.com/grailbio/base/unsafe"
	gbam "github.com/grailbio/bio/encoding/bam"
	"github.com/grailbio/bio/encoding/bamprovider"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/hts/sam"
)

// OnceTask manages a computation that must be run at most once.
// It's similar to sync.Once, except it also handles and returns errors.
type onceTask struct {
	once sync.Once
	err  error
}

// Do run the function do at most once. The returned error is
// the result of this invocation.
func (o *onceTask) Do(do func() error) error {
	o.once.Do(func() { o.err = do() })
	return o.err
}

// TaskOnce coordinates actions that must happen exactly once.
type taskOnce sync.Map

// Perform the provided action named by a key. Do invokes the action
// exactly once for each key, and returns any errors produced by the
// provided action.
func (t *taskOnce) Do(key interface{}, do func() error) error {
	taskv, _ := (*sync.Map)(t).LoadOrStore(key, new(onceTask))
	task := taskv.(*onceTask)
	return task.Do(do)
}

// BAMTable implements a table backed by a BAM or PAM file. It
// exports all of the standard BAM fields as well as auxiliary tag.
// An additional field, "length" is synthesized from the BAM since this
// useful but not easily computed within GQL.
type bamTable struct {
	hashOnce sync.Once
	hash     hash.Hash
	ast      ASTNode // source-code location
	path     string

	exactLenOnce sync.Once
	exactLen     int

	shardsOnce taskOnce
	mu         sync.Mutex
	shards     map[int][]gbam.Shard
}

// NewBAMTable returns a new BAM table backed by the BAM or PAM
// file at the provided path.
//
// TODO(marius): support selecting out fields from reading so that these
// can be pushed down to the underlying iterators.
func NewBAMTable(path string, ast ASTNode, hash hash.Hash) Table {
	t := &bamTable{
		hash:   hash,
		ast:    ast,
		path:   path,
		shards: make(map[int][]gbam.Shard),
	}
	return t
}

func (t *bamTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	MarshalTablePath(enc, t.path, singletonBAMFileHandler, t.Hash())
}

func (t *bamTable) Hash() hash.Hash {
	t.hashOnce.Do(func() {
		if t.hash == hash.Zero || VerifyFileHash {
			var h hash.Hash
			if IsFileImmutable(t.path) {
				h = hash.String(t.path)
			} else {
				provider := bamprovider.NewProvider(t.path, bamprovider.ProviderOpts{})
				stat, err := provider.FileInfo()
				if err != nil {
					Panicf(t.ast, "fileinfo %s: %v", t.path, err)
				}
				h = hash.String(t.path).Merge(hash.Time(stat.ModTime)).Merge(hash.Int(stat.Size))
				if t.hash != hash.Zero && t.hash != h {
					Panicf(t.ast, "mismatched hash for '%s' (file changed in the background?)", t.path)
				}
			}
			t.hash = h
		}
	})
	return t.hash
}

func (t *bamTable) Attrs(ctx context.Context) TableAttrs {
	return TableAttrs{Name: "bam", Path: t.path}
}

func (t *bamTable) Prefetch(ctx context.Context) {}

func (t *bamTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		return 5e8 // Seems like a not-unreasonable length for a BAM file.
	}
	t.exactLenOnce.Do(func() {
		t.exactLen = DefaultTableLen(ctx, t)
	})
	return t.exactLen
}

func (t *bamTable) getShards(total int) (bamprovider.Provider, []gbam.Shard, error) {
	provider := bamprovider.NewProvider(t.path, bamprovider.ProviderOpts{})
	err := t.shardsOnce.Do(total, func() error {
		shards, err := provider.GenerateShards(bamprovider.GenerateShardsOpts{
			Strategy:          bamprovider.ByteBased,
			SplitMappedCoords: true,
			IncludeUnmapped:   true,
			NumShards:         total,
		})
		if err != nil {
			return err
		}
		t.mu.Lock()
		t.shards[total] = shards
		t.mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return provider, t.shards[total], nil
}

type bamTableScanner struct {
	ctx      context.Context
	parent   *bamTable
	provider bamprovider.Provider
	shards   []gbam.Shard
	iter     bamprovider.Iterator
	row      bamTableRow
	nrow     int

	start, limit, total int
}

func (s *bamTableScanner) Scan() bool {
	CheckCancellation(s.ctx)
	for {
		if s.iter == nil {
			if len(s.shards) == 0 {
				Logf(s.parent.ast, "[%d,%d)/%d finished, read %d rows", s.start, s.limit, s.total, s.nrow)
				return false
			}
			s.iter = s.provider.NewIterator(s.shards[0])
			s.shards = s.shards[1:]
		}
		if !s.iter.Scan() {
			if err := s.iter.Close(); err != nil {
				Panicf(s.parent.ast, "Scan: %v", err)
			}
			s.iter = nil
			continue
		}
		s.row.record = s.iter.Record()
		InitStruct(&s.row)
		s.nrow++
		if s.nrow%1e7 == 0 {
			Logf(s.parent.ast, "[%d,%d)/%d: read %d rows", s.start, s.limit, s.total, s.nrow)
		}
		return true
	}
}

func (s *bamTableScanner) Value() Value {
	return NewStruct(&s.row)
}

// BAMTableMaxShards sets the maximum physical shards created for a BAM
// scanner. Exposed only for testing.
//
// TODO(marius): it might make more sense to compute a dynamic max
// shard by having a minimum shard size.
var BAMTableMaxShards = 512

func (t *bamTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	if total > BAMTableMaxShards {
		shardPerTotal := float64(BAMTableMaxShards) / float64(total)
		total = BAMTableMaxShards
		start = int(math.Ceil(float64(start) * shardPerTotal))
		limit = int(math.Ceil(float64(limit) * shardPerTotal))
		Logf(t.ast, "open %v [%d,%d)/%d [scaled]", t.path, start, limit, total)
	} else {
		Logf(t.ast, "open %v [%d,%d)/%d", t.path, start, limit, total)
	}
	// TODO(marius): it would be useful if GQL could provide scanners of
	// tables-of-records hints about which fields are used, so that
	// unused fields can be dropped.
	provider, shards, err := t.getShards(total)
	if err != nil {
		Panicf(t.ast, "%s: GenerateShards: %v", t.path, err)
	}
	var (
		nshards        = len(shards)
		shardsPerTotal = (nshards + total - 1) / total
		beg            = start * shardsPerTotal
		end            = limit * shardsPerTotal
	)
	if beg > nshards {
		beg = nshards
	}
	if end > nshards {
		end = nshards
	}
	s := &bamTableScanner{
		ctx:      ctx,
		parent:   t,
		provider: provider,
		shards:   shards[beg:end],
		start:    start,
		limit:    limit,
		total:    total,
	}
	runtime.SetFinalizer(s, func(s *bamTableScanner) {
		if s.iter != nil {
			closeAndLogError(s.iter, t.path)
		}
		closeAndLogError(provider, t.path)
	})
	return s
}

type bamTableRow struct {
	StructImpl
	record *sam.Record
}

// Len implements Struct
func (s *bamTableRow) Len() int {
	return 11 + len(s.record.AuxFields)
}

// Field implements Struct
func (s *bamTableRow) Field(idx int) StructField {
	r := s.record
	switch idx {
	case 0:
		return StructField{symbol.Name, NewString(r.Name)}
	case 1:
		return StructField{symbol.Reference, NewString(r.Ref.Name())}
	case 2:
		return StructField{symbol.Pos, NewInt(int64(r.Pos) + 1)}
	case 3:
		return StructField{symbol.MapQ, NewChar(rune(r.MapQ))}
	case 4:
		return StructField{symbol.Cigar, NewString(r.Cigar.String())}
	case 5:
		return StructField{symbol.Flags, NewInt(int64(r.Flags))}
	case 6:
		return StructField{symbol.RNext, NewString(formatMate(r.Ref, r.MateRef))}
	case 7:
		return StructField{symbol.PNext, NewInt(int64(r.MatePos) + 1)}
	case 8:
		return StructField{symbol.TLen, NewInt(int64(r.TempLen))}
	case 9:
		return StructField{symbol.Seq, NewString(FormatSeq(r.Seq))}
	case 10:
		return StructField{symbol.Qual, NewString(formatQual(r.Qual))}
	default:
		aux := r.AuxFields[idx-11]
		id, ok := symbol.AuxTags[aux.Tag()]
		if !ok {
			id = symbol.Intern(aux.Tag().String())
		}
		return StructField{id, auxToValue(aux)}
	}
}

// Value implements Struct
func (s *bamTableRow) Value(colName symbol.ID) (Value, bool) {
	r := s.record
	switch colName {
	case symbol.Name:
		return NewString(r.Name), true
	case symbol.Reference:
		return NewString(r.Ref.Name()), true
	case symbol.Pos:
		return NewInt(int64(r.Pos) + 1), true
	case symbol.MapQ:
		return NewChar(rune(r.MapQ)), true
	case symbol.Cigar:
		return NewString(r.Cigar.String()), true
	case symbol.Flags:
		return NewInt(int64(r.Flags)), true
	case symbol.RNext:
		return NewString(formatMate(r.Ref, r.MateRef)), true
	case symbol.PNext:
		return NewInt(int64(r.MatePos) + 1), true
	case symbol.TLen:
		return NewInt(int64(r.TempLen)), true
	case symbol.Seq:
		return NewString(FormatSeq(r.Seq)), true
	case symbol.Qual:
		return NewString(formatQual(r.Qual)), true
	default:
		tag, ok := symbol.AuxTagsIndex[colName]
		if !ok {
			tag = sam.NewTag(colName.Str())
		}
		aux, ok := r.Tag(tag[:])
		if !ok {
			return Value{}, false
		}
		return auxToValue(aux), true
	}
}

func auxToValue(aux sam.Aux) Value {
	switch aux[2] {
	case 'A':
		return NewChar(rune(aux[3]))
	case 'c', 'C':
		return NewInt(int64(aux[3]))
	case 's', 'S':
		return NewInt(int64(binary.LittleEndian.Uint16(aux[3:5])))
	case 'i', 'I':
		return NewInt(int64(binary.LittleEndian.Uint32(aux[3:7])))
	case 'f':
		return NewFloat(float64(math.Float32frombits(binary.LittleEndian.Uint32(aux[3:7]))))
	case 'Z', 'H': // 'H' is technically a byte array
		return NewString(string(aux[3:]))
	case 'B':
		// TODO(marius): support array types
		return NewString(aux.String())
	default:
		return NewString(fmt.Sprintf("%%!(UNKNOWN type=%c)", aux[2]))
	}
}

func closeAndLogError(closer io.Closer, path string) {
	if err := closer.Close(); err != nil {
		log.Error.Printf("%s: Closer: %v", path, err)
	}
}

func formatMate(ref, mate *sam.Reference) string {
	if mate != nil && ref == mate {
		return "="
	}
	return mate.Name()
}

// FormatSeq reports a human-readable string for the give DNA sequence.
func FormatSeq(s sam.Seq) string {
	if s.Length == 0 {
		return "*"
	}
	return gunsafe.BytesToString(s.Expand())
}

func formatQual(q []byte) string {
	for _, v := range q {
		if v != 0xff {
			a := make([]byte, len(q))
			simd.AddConst8(a, q, 33)
			return gunsafe.BytesToString(a)
		}
	}
	return "*"
}

// BAMFileHandler is a FileHandler implementation for BAM/PAM files.
type bamFileHandler struct{}

var singletonBAMFileHandler = &bamFileHandler{}

// Name implements FileHandler.
func (*bamFileHandler) Name() string { return "bam" }

// Open implements FileHandler.
func (*bamFileHandler) Open(ctx context.Context, path string, ast ASTNode, hash hash.Hash) Table {
	return NewBAMTable(path, ast, hash)
}

// Write implements FileHandler.
func (*bamFileHandler) Write(ctx context.Context, path string, ast ASTNode, table Table, nShard int, overwrite bool) {
	Panicf(ast, "write %s: write to BAM/PAM not supported", path)
}

func init() {
	RegisterFileHandler(singletonBAMFileHandler, `\.bam$`, `\.pam$`)
}

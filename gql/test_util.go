package gql

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/termutil"
	"github.com/grailbio/testutil"
)

func TestPrintValueLong(v Value) string {
	out := termutil.NewBufferPrinter()
	args := PrintArgs{
		Out:                out,
		Mode:               PrintCompact,
		MaxInlinedTableLen: math.MaxInt64,
	}
	v.Print(context.Background(), args)
	return out.String()
}

var testOnce sync.Once

func TestNewSession() *Session {
	testOnce.Do(func() {
		if BackgroundContext != nil {
			// gol_test already initialized the thing
			return
		}
		Init(Opts{CacheDir: testutil.GetTmpDir()})
	})
	return NewSession()
}

func TestMarshalValue(t *testing.T, val Value) (ctxData, valData []byte) {
	ctx := newMarshalContext(context.Background())
	enc := marshal.NewEncoder(nil)
	val.Marshal(ctx, enc)
	return ctx.marshal(), enc.Bytes()
}

func TestUnmarshalValue(t *testing.T, ctxData, valData []byte) Value {
	ctx := newUnmarshalContext(ctxData)
	dec := marshal.NewDecoder(valData)
	var val Value
	val.Unmarshal(ctx, dec)
	marshal.ReleaseDecoder(dec)
	return val
}

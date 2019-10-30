package gql

import (
	"context"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
)

// Types MarshalContext and UnmarshalContext store state needed while marshaling
// or unmarshaling a table. They keep track of call frames referenced by
// closures, so that reference cycles involving closure -> frame -> a closure
// stored in the frame -> frame ...  can be terminated.
//
// Map keys are the set of frames referenced transitively by closures. Keys are
// callFrame.hash().
type MarshalContext struct {
	ctx    context.Context
	frames map[hash.Hash]*callFrame
}

// UnmarshalContext stores state needed while unmarshaling a table.  The map
// stores the set of frames referenced transitively by closures. Keys are
// callFrame.hash().
type UnmarshalContext struct {
	ctx    context.Context
	frames map[hash.Hash]*callFrame
}

// NewMarshalContext creates an empty context. A table MarshalGOB function
// should create a new context at its start.
func newMarshalContext(ctx context.Context) MarshalContext {
	return MarshalContext{ctx: ctx, frames: map[hash.Hash]*callFrame{}}
}

// PutFrame adds the given frame to the cache. It is used when transferring
// frames across machine.
func (ctx MarshalContext) putFrame(frame *callFrame) {
	hash := frame.hash()
	if _, ok := ctx.frames[hash]; !ok {
		ctx.frames[hash] = frame.clone()
	}
}

// marshalBindings should be called for every Closure. b is the variable
// bindings, and fv should be the free variables used by the closure.
//
// fv isn't used now; in a near future we'll use it to subset the variables
// serialized.
func marshalBindings(ctx MarshalContext, enc *marshal.Encoder, b *bindings) {
	enc.PutVarint(int64(len(b.frames) - 1))
	for _, frame := range b.frames[1:] {
		ctx.putFrame(frame)
		hash := frame.hash()
		enc.PutHash(hash)
	}
}

// The caller should run marshal() after adding all the frames to the
// context. The marshal function serializes the registered call frames. The
// result can be passed to newUnmarshalContext, possibly on a different machine,
// to recreate the frames.
func (ctx MarshalContext) marshal() []byte {
	enc := marshal.NewEncoder(nil)
	n := 0
	done := map[hash.Hash]bool{}
	for {
		var (
			todo   []*callFrame
			hashes []hash.Hash
		)
		for h, frame := range ctx.frames {
			if !done[h] {
				done[h] = true
				todo = append(todo, frame)
				hashes = append(hashes, h)
			}
		}
		if len(todo) == 0 {
			break
		}
		for i, frame := range todo {
			if frame.hash() != hashes[i] {
				log.Panicf("Ignore %v %v", frame.describe(), hashes[i].String())
				continue
			}
			// The frame is fully instantiated already.
			n++
			enc.PutHash(hashes[i])
			frame.marshal(ctx, enc)
		}
	}
	enc.PutHash(hash.Zero)
	log.Debug.Printf("MarshalContext.marshal: wrote %d frames, %d bytes", n, enc.Len())
	return marshal.ReleaseEncoder(enc)
}

// NewUnmarshalContext creates a new context. Arg "data" should be produced by
// MarshalContext.marshal.
func newUnmarshalContext(data []byte) UnmarshalContext {
	dec := marshal.NewDecoder(data)
	ctx := UnmarshalContext{
		ctx:    BackgroundContext,
		frames: map[hash.Hash]*callFrame{},
	}
	for {
		var h hash.Hash
		dec.RawBytes(h[:])
		if h == hash.Zero {
			break
		}
		nf := unmarshalCallFrame(ctx, dec)
		f, ok := ctx.frames[h]
		if !ok {
			ctx.frames[h] = nf
		} else {
			// The frame was already registered in internFrame. This happens when
			// object created in unmarshalCallFrame refers to a frame not yet
			// registered.
			syms, vals := nf.list()
			for i, sym := range syms {
				if _, ok := f.lookup(sym); !ok {
					f.set(sym, vals[i])
				}
			}
		}
	}
	marshal.ReleaseDecoder(dec)
	return ctx
}

// UnmarshalBindings unserializes a binding.
func unmarshalBindings(ctx UnmarshalContext, dec *marshal.Decoder) *bindings {
	n := dec.Varint()
	b := &bindings{
		frames: make([]*callFrame, n+1),
	}
	b.frames[0] = globalConsts
	for i := 0; i < int(n); i++ {
		var h hash.Hash
		dec.RawBytes(h[:])
		b.frames[i+1] = ctx.internFrame(h)
	}
	return b
}

// InternFrame finds or creates a frame with the given hash. It is used when
// transferring frames across machine.
func (ctx UnmarshalContext) internFrame(hash hash.Hash) *callFrame {
	f, ok := ctx.frames[hash]
	if !ok {
		f = &callFrame{}
		ctx.frames[hash] = f
	}
	return f
}

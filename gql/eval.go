package gql

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

//go:generate ../../../../github.com/grailbio/base/gtl/generate.py --PREFIX=callFrame --package=gql --output=call_frame_pool.go -DELEM=*callFrame -DMAXSIZE=1024 ../../../../github.com/grailbio/base/gtl/freepool.go.tpl

// Bindings stores variable -> value mappings. It is a stack of callframes. The
// topmost frame is for global constants (builtin functions), the next frame is
// for gloval variables shared by all the computations in the session.  The rest
// of the frames are pushed and poped for each function call.
//
// Bindings is thread compatible. It is owned by one goroutine. To share a
// binding with multiple threads, it must be cloned beforehand.
type bindings struct {
	// frames[0] is the global immutable frame containing builtin functions. It is
	// shared by all the bindings created from one Session. frames[1] is the
	// global (mutable) frame.  Each Bindings owns its copy of global mutable
	// frame. frames[2:] are created during function calls to store local
	// variables.
	frames []*callFrame
	pool   callFramePool
}

// callFrame stores a set of variables for one function-call frame.
type callFrame struct {
	// The first two variables are stored inplace for efficiency.
	sym0, sym1 symbol.ID
	val0, val1 Value
	vars       map[symbol.ID]Value
}

func (b *bindings) newFrame() *callFrame {
	if b.pool.New == nil {
		b.pool.New = func() *callFrame { return &callFrame{} }
	}
	f := b.pool.Get()
	return f
}

// pushFrame0 pushes an empty frame
func (b *bindings) pushFrame0() {
	f := b.newFrame()
	b.frames = append(b.frames, f)
}

// pushFrame1 pushes a frame that contains one variable binding.
func (b *bindings) pushFrame1(sym symbol.ID, v Value) {
	f := b.newFrame()
	f.sym0, f.val0 = sym, v
	b.frames = append(b.frames, f)
}

// pushFrame2 pushes a frame that contains two variable bindings.
func (b *bindings) pushFrame2(sym0 symbol.ID, v0 Value, sym1 symbol.ID, v1 Value) {
	f := b.newFrame()
	f.sym0, f.val0 = sym0, v0
	f.sym1, f.val1 = sym1, v1
	b.frames = append(b.frames, f)
}

// pushFrameN pushes a frame with the given variable-to-value bindings.
//
// REQUIRES: len(syms)==len(values).
func (b *bindings) pushFrameN(syms []symbol.ID, values []Value) {
	if len(syms) != len(values) {
		log.Panic("newFrameN: length mismatch")
	}
	frame := b.newFrame()
	for i := range syms {
		frame.set(syms[i], values[i])
	}
	b.frames = append(b.frames, frame)
}

// popFrame removes the bottommost frame added by a prior pushFrame* call.
func (b *bindings) popFrame() {
	f := b.frames[len(b.frames)-1]
	b.frames = b.frames[:len(b.frames)-1]
	f.sym0 = symbol.Invalid
	f.sym1 = symbol.Invalid
	for k := range f.vars {
		delete(f.vars, k)
	}
	b.pool.Put(f)
}

// Clone creates a deep copy of the frame.
func (f *callFrame) clone() *callFrame {
	n := &callFrame{}
	*n = *f
	if f.vars != nil {
		n.vars = map[symbol.ID]Value{}
		for k, v := range f.vars {
			n.vars[k] = v
		}
	}
	return n
}

// List lists variables and the corresponding values in the frame.  It is slow
// and not for general use.
func (f *callFrame) list() (syms []symbol.ID, vals []Value) {
	if f.sym0 != symbol.Invalid {
		syms = append(syms, f.sym0)
		vals = append(vals, f.val0)
	}
	if f.sym1 != symbol.Invalid {
		syms = append(syms, f.sym1)
		vals = append(vals, f.val1)
	}
	for sym, val := range f.vars {
		syms = append(syms, sym)
		vals = append(vals, val)
	}
	return
}

// Set adds a new binding. It crashes if the symbol already exists.
func (f *callFrame) set(sym symbol.ID, v Value) {
	if f.sym1 != symbol.Invalid {
		if f.vars == nil {
			f.vars = map[symbol.ID]Value{}
		} else {
			if len(f.vars) != 0 {
				panic(f)
			}
		}
		f.vars[f.sym1] = f.val1
		f.vars[f.sym0] = f.val0
		f.sym1 = symbol.Invalid
		f.sym0 = symbol.Invalid
		// fallthrough
	} else if f.sym0 != symbol.Invalid {
		f.sym1 = sym
		f.val1 = v
		return
	}
	if f.vars == nil {
		f.vars = map[symbol.ID]Value{}
	} else {
		if _, ok := f.vars[sym]; ok {
			log.Panicf("variable '%s' already exists in the frame", sym.Str())
		}
	}
	f.vars[sym] = v
}

// MarshalBinary implements the GOB interface.
func (f *callFrame) MarshalBinary() ([]byte, error) {
	panic("Call Marshal instead")
}

// UnmarshalBinary implements the GOB interface.
func (f *callFrame) UnmarshalBinary(data []byte) error {
	panic("Call Unmarshal instead")
}

func (f *callFrame) numVars() int {
	n := 0
	if f.sym0 != symbol.Invalid {
		n++
	}
	if f.sym1 != symbol.Invalid {
		n++
	}
	return n + len(f.vars)
}

// marshal serializes this object.
func (f *callFrame) marshal(ctx MarshalContext, enc *marshal.Encoder) {
	nVar := f.numVars()
	enc.PutVarint(int64(nVar))
	if f.sym0 != symbol.Invalid {
		f.sym0.Marshal(enc)
		f.val0.Marshal(ctx, enc)
		nVar--
	}
	if f.sym1 != symbol.Invalid {
		f.sym1.Marshal(enc)
		f.val1.Marshal(ctx, enc)
		nVar--
	}
	for sym, val := range f.vars {
		sym.Marshal(enc)
		val.Marshal(ctx, enc)
		nVar--
	}
	if nVar != 0 {
		log.Panicf("frame: %+v", *f)
	}
}

// unmarshalCallFrame deserializes a callFrame.
func unmarshalCallFrame(ctx UnmarshalContext, dec *marshal.Decoder) *callFrame {
	f := &callFrame{}
	nVar := dec.Varint()
	if nVar > 0 {
		f.sym0.Unmarshal(dec)
		f.val0.Unmarshal(ctx, dec)
		nVar--
	}
	if nVar > 0 {
		f.sym1.Unmarshal(dec)
		f.val1.Unmarshal(ctx, dec)
		nVar--
	}
	if nVar > 0 {
		f.vars = map[symbol.ID]Value{}
		for nVar > 0 {
			var (
				sym symbol.ID
				val Value
			)
			sym.Unmarshal(dec)
			val.Unmarshal(ctx, dec)
			f.vars[sym] = val
			nVar--
		}
	}
	return f
}

// Describe lists names of variables in the frame.
func (f *callFrame) describe() string {
	syms, _ := f.list()
	var vars []string
	for _, sym := range syms {
		vars = append(vars, sym.Str())
	}
	sort.Strings(vars)
	return fmt.Sprintf("frame: %v (hash: %v)", vars, f.hash())
}

func (f *callFrame) hash() hash.Hash {
	h := hash.Hash{
		0xb7, 0x29, 0x28, 0x26, 0x68, 0xe5, 0x28, 0xf7,
		0x04, 0xfa, 0x97, 0xd2, 0x95, 0xad, 0x18, 0xd2,
		0x75, 0xcb, 0xab, 0xae, 0x3f, 0xd1, 0x4b, 0xf3,
		0x9c, 0xff, 0x71, 0xbd, 0x14, 0x2f, 0x82, 0x11}
	if f.sym0 != symbol.Invalid {
		h = h.Add(f.sym0.Hash().Merge(f.val0.Hash()))
	}
	if f.sym1 != symbol.Invalid {
		h = h.Add(f.sym1.Hash().Merge(f.val1.Hash()))
	}
	for k, v := range f.vars {
		hh := k.Hash().Merge(v.Hash())
		h = h.Add(hh)
	}
	return h
}

func (f *callFrame) lookup(name symbol.ID) (Value, bool) {
	if name == symbol.Invalid {
		panic(name)
	}
	if name == f.sym0 {
		return f.val0, true
	}
	if name == f.sym1 {
		return f.val1, true
	}
	if f.vars != nil {
		val, ok := f.vars[name]
		return val, ok
	}
	return Value{}, false
}

// Clone creates a copy of the binding.
func (b *bindings) clone() *bindings {
	b2 := &bindings{}
	b2.frames = make([]*callFrame, len(b.frames), len(b.frames)+4 /*give space for future push*/)
	for i := range b.frames {
		if i == 0 { // global const frame is immutable
			b2.frames[i] = b.frames[i]
			continue
		}
		b2.frames[i] = b.frames[i].clone()
	}
	return b2
}

// globalConsts stores built-in functions and consts.
var (
	globalConsts     = &callFrame{}
	globalConstsHash = globalConsts.hash()
)

func registerGlobalConstInternal(name symbol.ID, val Value, aiType AIType) {
	globalConsts.set(name, val)
	globalConstsHash = globalConsts.hash()
	aiGlobalConsts[name] = aiType
}

// RegisterGlobalConst adds name->value binding in the global constant table.
// It panics if the name already exists. This function may be called after
// gql.Init(), but before evaluating any user expression.
func RegisterGlobalConst(name string, val Value) {
	if val.Type() == FuncType {
		log.Panicf("RegisterGlobalConst %v: builtin function must be registered using RegisterBuiltinFunc", name)
	}
	registerGlobalConstInternal(symbol.Intern(name), val, AIType{Type: val.Type(), Literal: &val})
}

// Describe dumps the binding contents in a human-readable fashion.
func (b *bindings) Describe() string {
	buf := bytes.NewBuffer(nil)
	for i := len(b.frames) - 1; i >= 1; /*exclude global*/ i-- {
		f := b.frames[i]
		buf.WriteString(fmt.Sprintf("Frame %d: %s\n", i, f.describe()))
	}
	return buf.String()
}

// GlobalVars returns the names of global consts and variables.  Names are
// returned in no particular order.
func (b *bindings) GlobalVars() (vars []symbol.ID) {
	for _, frame := range b.frames[0:2] {
		syms, _ := frame.list()
		for _, sym := range syms {
			vars = append(vars, sym)
		}
	}
	return vars
}

// setGlobal sets a variable in the global mutable frame (frames[1]).
//
// REQUIRES: sym is not a builtin function or value.
func (b *bindings) setGlobal(sym symbol.ID, val Value) {
	b.frames[1].set(sym, val)
}

// Lookup finds the value bound to the given symbol.
func (b *bindings) Lookup(name symbol.ID) (Value, bool) {
	for i := len(b.frames) - 1; i >= 0; i-- {
		val, ok := b.frames[i].lookup(name)
		if ok {
			switch {
			case i == 0:
				return val, true
			case i == 1:
				return val, true
			default:
				return val, true
			}
		}
	}
	return Value{}, false
}

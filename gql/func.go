package gql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

// FuncCallback is the function body. ast is syntax the tree node of the call,
// used only to report the source-code location on error.  args is the arguments
// to the function.
type FuncCallback func(ctx context.Context, ast ASTNode, args []ActualArg) Value

// TypeCallback is called when a script is parsed. ast is the syntax tree node
// of the call.  It should check if the args[] conforms to the the function's
// input spec. It returns (function return type, free variables read by lazy
// args passed to the function>
type TypeCallback func(ast ASTNode, args []AIArg) AIType

// ClosureFormalArg is the list of formal args for a closure.
// It is part of FormalArg
type ClosureFormalArg struct {
	// Name is the default name of the arg
	Name symbol.ID
	// Override specifies the name of the argument to the (outer) function that
	// overrides the Name. For example, the builtin map() function has an optional
	// argument "row" that overrides the name of the arguments to filter and map
	// callbacks. For these callbacks, Name=symbol.Intern("_"),
	// Override=symbol.Intern("row").
	Override symbol.ID
}

// FormalArg is a description of an argument to a function.
type FormalArg struct {
	// Name must be set for a named argument. For positional args, this field is
	// only informational.
	Name symbol.ID
	// Positional is true if the caller need not name the arg in form
	// "arg:=value".
	Positional bool
	// Required is true if the arg must be provided by the caller.
	Required bool
	// Variadic causes this arg to consume all the remaining, unnamed actual args
	Variadic bool

	// Closure is true if the argument is treated as a lambda body with the args
	// specified in ClosureArgs. A Closure argument is translated into a
	// user-defined function during the analysis phase.
	Closure     bool
	ClosureArgs []ClosureFormalArg

	// Joinclosure is a special kind of closure that's used to specify the join
	// condition and map arg for the join function. The formal args to these
	// callbacks need to be extraced from the 1st arg to join ({t0:table0, ...,
	// tN:tableN}).
	JoinClosure bool

	// If Symbol=true, the actual arg must be a naked symbol that specifies a
	// variable name. It is used, e.g., as the "row:=foobar" arg in map() and
	// filter().
	Symbol bool
	// DefaultSymbol is set as the value of the symbol arg if it is not provided
	// by the caller.
	//
	// INVARIANT: If DefaultSymbol!=symbol.Invalid then Symbol==true.
	DefaultSymbol symbol.ID

	// DefaultValue is set as the arg value if it is not provided by the caller.
	// It is meaningful only for non-lazy, non-symbol args.
	DefaultValue Value

	// The list of types allowed as the arg. Meaningful only if !Symbol. This
	// field is optional. The parser will check for type conformance if len(Types)
	// len(Types)>0. This field may be nil, in which case it is up to the
	// typecheck callback to validate arg types.
	Types []ValueType
}

var anonRowFuncArg = []ClosureFormalArg{{symbol.AnonRow, symbol.Row}}

func (f FormalArg) String() string {
	return "arg:" + f.Name.Str()
}

func validateFormalArgs(name string, args []FormalArg) {
	hasVariadic := false
	for i := range args {
		arg := &args[i]
		if !arg.Required {
			switch {
			case arg.Symbol:
				if arg.DefaultSymbol == symbol.Invalid {
					log.Panicf("%s (arg#%d) : default symbol not set for arg %s", name, i, arg.Name.Str())
				}
			default:
				if !arg.DefaultValue.Valid() {
					log.Panicf("%s (arg#%d): default value not set for arg %s", name, i, arg.Name.Str())
				}
			}
		}
		if !arg.DefaultValue.Valid() {
			// gob.isZero() inspects the Value fields (specifically unsafe.Pointer) even if
			// we set the Value.MarshalBinary handler, so set a non-zero field value.
			arg.DefaultValue = Null
		}
		if !arg.Positional && arg.Name == symbol.Invalid {
			log.Panicf("%s (arg#%d): Non-positional arg must have a name", name, i)
		}
		if hasVariadic {
			if arg.Positional {
				log.Panicf("%s (arg#%d): only named args can follow variadic arg", name, i)
			}
		}
		if arg.Variadic {
			if !arg.Positional {
				log.Panicf("Variadic arg must be positional")
			}
			hasVariadic = true
		}
	}
}

// Func represents a function closure. It is stored in Value.
type Func struct {
	// name is the name of the function. For builtin functions, name also is used
	// as the function ID. Otherwise for logging&debugging only.
	name        symbol.ID
	ast         ASTNode // for reporting source-code locations on error
	builtin     bool    // true for builtin, false for user-defined lambdas.
	funcCB      FuncCallback
	typeCB      TypeCallback
	formalArgs  []FormalArg
	hash        hash.Hash
	description string

	// The following fields are set only for user-defined functions.  They
	// represent the body of the function and its execution env.
	env  *bindings
	body ASTNode
}

// RegisterBuiltinFunc registers a builtin function. It should be called inside
// init().
// Note when specifying the list of FormatlArgs certain restrictions are
// imposed by the parser. In particular, all positional arguments must be
// listed before named ones.
func RegisterBuiltinFunc(name, desc string,
	funcCB FuncCallback,
	typeCB TypeCallback,
	formalArgs ...FormalArg) Value {
	symID := symbol.Intern(name)
	validateFormalArgs(name, formalArgs)
	f := &Func{
		name:        symID,
		ast:         &ASTUnknown{},
		builtin:     true,
		funcCB:      funcCB,
		typeCB:      typeCB,
		formalArgs:  formalArgs,
		description: desc,
		hash:        hash.String(name)}
	val := NewFunc(f)
	registerGlobalConstInternal(symID, val, AIType{Type: FuncType, Literal: &val, FormalArgs: formalArgs, TypeCB: typeCB})
	return val
}

func evalUserDefinedFunc(ctx context.Context, ast ASTNode, args []ActualArg, formalArgs []FormalArg, body ASTNode, envPool *sync.Pool) Value {
	if len(args) != len(formalArgs) {
		Panicf(ast, "wrong# of args: expect %v, got %+v", formalArgs, args)
	}
	newEnv := envPool.Get().(*bindings)
	switch len(args) {
	case 0:
		newEnv.pushFrame0()
	case 1:
		newEnv.pushFrame1(formalArgs[0].Name, args[0].Value)
	case 2:
		newEnv.pushFrame2(
			formalArgs[0].Name, args[0].Value,
			formalArgs[1].Name, args[1].Value)
	default:
		vars := make([]symbol.ID, len(args))
		values := make([]Value, len(args))
		for i := range args {
			vars[i] = formalArgs[i].Name
			values[i] = args[i].Value
		}
		newEnv.pushFrameN(vars, values)
	}
	val := body.eval(ctx, newEnv)
	newEnv.popFrame()
	envPool.Put(newEnv)
	return val
}

// NewUserDefinedFunc creates a new user-defined function.  "ast" is used only
// to show debug messages on error.
func NewUserDefinedFunc(ast ASTNode, orgEnv *bindings, formalArgs []FormalArg, body ASTNode) *Func {
	env := orgEnv.clone()
	h := hash.Hash{
		0x72, 0xd1, 0xc3, 0xdc, 0xbc, 0xdb, 0x27, 0x72,
		0xe0, 0xa8, 0xbb, 0x75, 0x63, 0x45, 0xe0, 0xb6,
		0x22, 0xe1, 0x3a, 0x8d, 0x5e, 0x0f, 0xb4, 0x55,
		0x00, 0x3b, 0xc3, 0x3a, 0xac, 0x4d, 0x24, 0xdd}
	var (
		syms   []symbol.ID
		values []Value
	)
	for _, arg := range formalArgs {
		if arg.Name == symbol.Invalid {
			panic(arg)
		}
		syms = append(syms, arg.Name)
		values = append(values, Null)
	}
	env.pushFrameN(syms, values)
	h = h.Merge(body.hash(env))
	h = h.Merge(globalConstsHash)
	env.popFrame()

	envPool := sync.Pool{New: func() interface{} { return env.clone() }}
	validateFormalArgs("func{"+ast.String()+"}", formalArgs)
	f := &Func{
		name:    symbol.Intern("lambda"), // TODO(saito) pick a better name
		ast:     ast,
		builtin: false,
		funcCB: func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			if len(args) != len(formalArgs) {
				Panicf(ast, "wrong# of args: expect %v, got %+v", formalArgs, args)
			}
			newEnv := envPool.Get().(*bindings)
			switch len(args) {
			case 0:
				newEnv.pushFrame0()
			case 1:
				newEnv.pushFrame1(formalArgs[0].Name, args[0].Value)
			case 2:
				newEnv.pushFrame2(
					formalArgs[0].Name, args[0].Value,
					formalArgs[1].Name, args[1].Value)
			default:
				vars := make([]symbol.ID, len(args))
				values := make([]Value, len(args))
				for i := range args {
					vars[i] = formalArgs[i].Name
					values[i] = args[i].Value
				}
				newEnv.pushFrameN(vars, values)
			}
			val := body.eval(ctx, newEnv)
			newEnv.popFrame()
			envPool.Put(newEnv)
			return val
		},
		typeCB: func(ast ASTNode, args []AIArg) AIType {
			panic("lambda typecb") // typeCB is never invoked; the lambda's Analyze is invoked instead.
		},
		formalArgs: formalArgs,
		hash:       h,
		env:        env,
		body:       body,
	}
	return f
}

// Marshal marshals given function.
func (f *Func) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	if f == nil {
		enc.PutByte(0)
		return
	}
	if f.builtin {
		enc.PutByte(1)
		f.name.Marshal(enc)
		return
	}
	enc.PutByte(2)
	enc.PutHash(f.hash)
	marshalBindings(ctx, enc, f.env)
	enc.PutGOB(f.formalArgs)
	enc.PutGOB(&f.body)
}

// unmarshalFunc unmarshals a function from bytes.
func unmarshalFunc(ctx UnmarshalContext, dec *marshal.Decoder) *Func {
	typ := dec.Byte()
	if typ == 0 {
		return nil
	}
	if typ == 1 { // builtin
		var name symbol.ID
		name.Unmarshal(dec)
		c, ok := globalConsts.lookup(name)
		if !ok {
			log.Panicf("unmarshal: function %v not found", name)
		}
		return c.Func(astUnknown)
	}
	if typ != 2 {
		panic(typ)
	}
	h := dec.Hash()
	env := unmarshalBindings(ctx, dec)
	var (
		formalArgs []FormalArg
		body       ASTNode
	)
	dec.GOB(&formalArgs)
	dec.GOB(&body)
	envPool := sync.Pool{New: func() interface{} { return env.clone() }}

	f := &Func{
		name:    symbol.Intern("lambda"), // TODO(saito) pick a better name
		ast:     astUnknown,
		builtin: false,
		funcCB: func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			return evalUserDefinedFunc(ctx, ast, args, formalArgs, body, &envPool)
		},
		typeCB: func(ast ASTNode, args []AIArg) AIType {
			panic("lambda typecb") // typeCB is never invoked; the lambda's Analyze is invoked instead.
		},
		formalArgs: formalArgs,
		hash:       h,
		env:        env,
		body:       body,
	}
	return f
}

// NewFunc creates a new  function value.
func NewFunc(f *Func) Value {
	return Value{typ: FuncType, p: unsafe.Pointer(f)}
}

// Func extracts the function value
//
// REQUIRES: v.Type()==FuncType.
func (v Value) Func(ast ASTNode) *Func {
	if v.typ != FuncType {
		Panicf(ast, "Value '%v' (type %v) is not a function", v, v.typ)
	}
	return (*Func)(v.p)
}

// String generates a human-readable description.
func (f *Func) String() string {
	if f == nil {
		return "nil"
	}
	if f.builtin {
		return f.name.Str()
	}
	var argNames []string
	for _, arg := range f.formalArgs {
		argNames = append(argNames, arg.Name.Str())
	}
	return fmt.Sprintf("λ(%s)%s/env:%s",
		strings.Join(argNames, ","),
		f.body.String(),
		f.env.Describe())
}

// Builtin returns true if the function is built into GQL.
func (f *Func) Builtin() bool { return f.builtin }

// Hash computes the hash of the function, including its closure environment.
func (f *Func) Hash() hash.Hash {
	return f.hash
}

// Eval evaluates the closure
func (f *Func) Eval(ctx context.Context, args ...Value) Value {
	actualArgs := actualArgPool.Get()
	for i, arg := range args {
		actualArgs = append(actualArgs,
			ActualArg{Name: f.formalArgs[i].Name, Value: arg})
	}
	val := f.funcCB(ctx, f.ast, actualArgs)
	actualArgs = actualArgs[:0]
	actualArgPool.Put(actualArgs)
	return val
}

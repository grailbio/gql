package gql

// Abstract interpretation (aka typecheck) utilities

import (
	"fmt"
	"strings"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/symbol"
)

// AIType is similar to ValueType, but it contains more detailed information,
// such as function signatures. It is used only during pre-execution program
// analysis, while ASTNode.Analyze is running. It is never persisted.
type AIType struct {
	// ValueType is the type of the node.
	Type ValueType

	// Literal is non-nil if the type refers to a compile-time constant.
	Literal *Value

	// Any is set when the type is unknown. An any type matches anything.
	Any bool

	// The following fields are set iff Type==FuncType

	// Formal args to the function.
	FormalArgs []FormalArg
	// TypeCB is called via ASTNode.Analyze to run a
	// function-implementation-specific typecheck.
	//
	// Note: the types of non-lazy args in []FormalArgs are checked by
	// ASTFunctionCall.Analeze, so TypeCB implementations don't need to check
	// them.
	TypeCB TypeCallback
}

var (
	// Shorthand for trivial types.
	AIAnyType    = AIType{Any: true}
	AIBoolType   = AIType{Type: BoolType}
	AIIntType    = AIType{Type: IntType}
	AIFloatType  = AIType{Type: FloatType}
	AIStringType = AIType{Type: StringType}
	AIStructType = AIType{Type: StructType}
	AITableType  = AIType{Type: TableType}
)

// Is checks if t is of the given type. It always returns true if t.Any==true.
// It also treats string-like types as the same, and date-like types as the
// same.
func (t AIType) Is(typ ValueType) bool {
	if t.Any || t.Type == typ {
		return true
	}
	if t.Type.LikeString() && typ.LikeString() {
		return true
	}
	if t.Type.LikeDate() && typ.LikeDate() {
		return true
	}
	return false
}

// IsType checks if "other" can be interpreted as "t".
func (t AIType) IsType(other AIType) bool {
	if other.Any {
		return true
	}
	return t.Is(other.Type)
}

// FuncReturnType is the type of return values of the function.  If t.Any, it
// returns AIAnyType.
//
// REQUIRES: t.Is(FuncType).
func (t AIType) FuncReturnType(ast ASTNode) AIType {
	if t.Any {
		return AIAnyType
	}
	if !t.Is(FuncType) {
		Panicf(ast, "value is not a function.")
	}
	var dummyArgs []AIArg
	for _, arg := range t.FormalArgs {
		dummyArgs = append(dummyArgs, AIArg{Name: arg.Name, Type: AIAnyType})
	}
	return t.TypeCB(ast, dummyArgs)
}

// AIArg is the result of parsing & typechecking FormalFuncArg.
type AIArg struct {
	Name         symbol.ID  // =symbol.Invalid iff the arg is positional.
	Type         AIType     // Set when the value is eagerly evaluated
	Env          aiBindings // Env and Expr form a closure
	Expr         ASTNode    // Syntactic node. Nil iff the arg is optional and is omitted by the caller.
	Symbol       symbol.ID  // Set iff FormalArg.Symbol=true.
	DefaultValue Value      // Copied from FormalArg.DefaultValue
}

// aiFrame is holds variable -> type bindings for args for a function call
// during static analysis.
type aiFrame map[symbol.ID]AIType

// String produces a human-readable description.
func (frame aiFrame) String() string {
	buf := strings.Builder{}
	buf.WriteByte('{')
	n := 0
	for sym := range frame {
		if n > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(sym.Str())
		n++
	}
	buf.WriteByte('}')
	return buf.String()
}

var aiGlobalConsts = aiFrame{}

// aiBindings is similar to Bindings, but only stores formal type info and used
// during static type checking.
type aiBindings struct {
	// Frames stores types of variables in one function-call frame. It is exported
	// because it is accessed by GOB.
	Frames []aiFrame
}

// String produces a human-readable description.
func (b aiBindings) String() string {
	buf := strings.Builder{}
	for i := len(b.Frames) - 1; i >= 0; i-- {
		f := b.Frames[i]
		fmt.Fprintf(&buf, "Frame %d: %v\n", i, f)
	}
	return buf.String()
}

// PushFrame pushes the given frame.
func (b aiBindings) PushFrame(frame aiFrame) aiBindings {
	for sym := range frame {
		if sym == symbol.Invalid {
			panic(frame)
		}
	}
	n := aiBindings{
		Frames: make([]aiFrame, len(b.Frames)+1),
	}
	copy(n.Frames, b.Frames)
	n.Frames[len(n.Frames)-1] = frame
	return n
}

func (b aiBindings) Lookup(sym symbol.ID) (AIType, bool) {
	for i := len(b.Frames) - 1; i >= 0; i-- {
		if val, ok := b.Frames[i][sym]; ok {
			return val, true
		}
	}
	return AIType{}, false
}

// setGlobal sets variable type in the global mutable frame (frames[1]).
//
// REQUIRES: sym is not a builtin function or value.
func (b *aiBindings) setGlobal(sym symbol.ID, typ AIType) {
	if _, ok := b.Frames[0][sym]; ok {
		log.Panicf("SetGlobal: cannot overwrite global constant '%s'", sym.Str())
	}
	b.Frames[1][sym] = typ
}

// combineTypes merges multiple types into one by computing the most specific
// type of them. It panics if the input types are not compatible with each
// other.
//
// "ast" is used only to report source-code location on error.
func combineTypes(ast ASTNode, types []AIType) AIType {
	// Return the more specific of the two types.
	//
	// REQIRES: v0 and v1 are compatible
	top := func(v0, v1 ValueType) ValueType {
		if v0.LikeString() && v0 != v1 {
			if v0 == StringType {
				return v1
			}
			return v0
		}
		// TODO(saito): pick a proper top type for date-like types.
		return v0
	}

	t0 := types[0]
	for _, other := range types[1:] {
		if !t0.IsType(other) {
			buf := strings.Builder{}
			fmt.Fprintf(&buf, "%v:%v: invalid arg types:", ast.pos(), ast)
			for i, typ := range types {
				if i > 0 {
					buf.WriteString(", ")
				}
				if typ.Any {
					fmt.Fprintf(&buf, "#%d: type any", i)
				} else {
					fmt.Fprintf(&buf, "#%d: type %v", i, typ)
				}
			}
			log.Panic(buf.String())
		}
		// Pick the most specific type
		if t0.Any && !other.Any {
			t0 = other
		} else if !t0.Any && !other.Any {
			t0.Type = top(t0.Type, other.Type)
		}
	}

	t0.Literal = nil
	return t0
}

// combineArgTypes calls combineTypes for each type in args[].
func combineArgTypes(ast ASTNode, args []AIArg) AIType {
	types := make([]AIType, len(args))
	for i, arg := range args {
		types[i] = arg.Type
	}
	return combineTypes(ast, types)
}

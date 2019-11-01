package gql

//go:generate goyacc -l syntax.y

// Types and functions related to parsing.

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"regexp"
	"runtime/debug"
	"strings"
	"text/scanner"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/gql/termutil"
)

// EncodeGOB is a convenience function for encoding a value using gob.  It
// crashes the process on error.
func encodeGOB(ast ASTNode, enc *gob.Encoder, val interface{}) {
	if err := enc.Encode(val); err != nil {
		Panicf(ast, "gob: failed to encode %v: %v", val, err)
	}
}

// DecodeGOB is a convenience function for decoding a value using gob.  It
// crashes the process on error.
func decodeGOB(ast ASTNode, dec *gob.Decoder, val interface{}) {
	if err := dec.Decode(val); err != nil {
		Panicf(ast, "gob: failed to decode %v: %v: %v", val, err, string(debug.Stack()))
	}
}

// ASTNode represents an abstract syntax tree node. One ASTnode is created for a
// syntactic element found in the source script.  Implementations of ASTNode
// must be threadsafe and GOB-encodable.
type ASTNode interface {
	// Eval evaluates the node. The bindings stores local variables in the call
	// frames for this node.
	eval(ctx context.Context, env *bindings) Value

	// String produces a human-readable description of the expression node.  The
	// resulting string is only for logging; it may not be a valid GQL expression.
	String() string

	// Hash computes a hash of this node (and descendants). The bindings stores
	// local variables in the call frames for this node.
	hash(b *bindings) hash.Hash

	// pos reports the location of this node in the source file.
	pos() scanner.Position
}

// ASTStatement represents an assignment 'var:=expr' or an expression.
type ASTStatement struct {
	// Pos is the location of this node in the source file.
	Pos scanner.Position

	// LHS is set when the statement is of form "var := expr".
	LHS symbol.ID

	// Expr is the right-hand side of "var := expr". It is also set when the
	// statement is a naked expression.
	Expr ASTNode
}

// ASTStatementOrLoad is a toplevel gql construct.
type ASTStatementOrLoad struct {
	ASTStatement
	// Load is set when the statement is of form "load `path`". The value is the
	// pathname. Other fields are unset.
	LoadPath string
}

// String returns a human-readable string.
func (s ASTStatementOrLoad) String() string {
	if s.LoadPath != "" {
		return fmt.Sprintf("load `%s`", s.LoadPath)
	}
	return s.ASTStatement.String()
}

func (s ASTStatement) String() string {
	if s.LHS == symbol.Invalid {
		return s.Expr.String()
	}
	return fmt.Sprintf("%s:=%s", s.LHS.Str(), s.Expr)
}

// NewASTStatement creates a new ASTStatement.
func NewASTStatement(pos scanner.Position, name string, expr ASTNode) ASTStatement {
	return ASTStatement{Pos: pos, LHS: symbol.Intern(name), Expr: expr}
}

// ASTBlock represents an expression of form "{ assignments; expr }"
type ASTBlock struct {
	Pos        scanner.Position
	Statements []ASTStatement
}

var _ ASTNode = &ASTBlock{}

func (n *ASTBlock) hash(b *bindings) hash.Hash {
	h := hash.Hash{
		0x87, 0xb1, 0x7a, 0x43, 0x27, 0xb8, 0x4f, 0x7b,
		0xc1, 0x06, 0x13, 0xc4, 0x6b, 0x90, 0xc6, 0x87,
		0x03, 0xb9, 0xab, 0xae, 0xb0, 0xa0, 0xea, 0x86,
		0x32, 0xdf, 0x66, 0x2b, 0xd3, 0x5c, 0x55, 0x12}
	b.pushFrame0()
	for _, s := range n.Statements {
		if s.LHS != symbol.Invalid {
			h = h.Merge(s.LHS.Hash())
			b.frames[len(b.frames)-1].set(s.LHS, Null)
		}
		h = h.Merge(s.Expr.hash(b))
	}
	b.popFrame()
	return h
}

func (n *ASTBlock) eval(ctx context.Context, b *bindings) Value {
	b.pushFrame0()
	var val Value
	for _, s := range n.Statements {
		val = s.Expr.eval(ctx, b)
		if s.LHS != symbol.Invalid {
			b.frames[len(b.frames)-1].set(s.LHS, val)
		}
	}
	b.popFrame()
	return val
}

func (n *ASTBlock) pos() scanner.Position { return n.Pos }

func (n *ASTBlock) String() string {
	buf := strings.Builder{}
	buf.WriteByte('{')
	for i, s := range n.Statements {
		if i > 0 {
			buf.WriteByte(';')
		}
		buf.WriteString(s.String())
	}
	buf.WriteByte('}')
	return buf.String()
}

// ASTUnknown implements ASTNode. It is a placeholder whose only purpose to
// report a source code location.
type ASTUnknown struct{}

// astUnknown can be used when no source-code location is known.
var astUnknown ASTNode = &ASTUnknown{}

func (n *ASTUnknown) hash(_ *bindings) hash.Hash                    { panic("hash") }
func (n *ASTUnknown) eval(ctx context.Context, env *bindings) Value { panic("eval") }
func (n *ASTUnknown) pos() scanner.Position                         { return scanner.Position{} }
func (n *ASTUnknown) String() string                                { return "(unknown)" }

// ASTLiteral is an ASTNode for a literal value, such as 123, `foobar`.
type ASTLiteral struct {
	// Pos is the source-code location of this node.
	Pos scanner.Position
	// Literal stores the const value.
	Literal Value
	// Org stores the original expression, if this node was created as a result of
	// constant-propagation optimization. Otherwise Org is nil.
	Org ASTNode
}

var _ ASTNode = &ASTLiteral{}

// String implements the ASTNode interface.
func (n *ASTLiteral) String() string {
	if n.Org != nil {
		return n.Org.String()
	}
	out := termutil.NewBufferPrinter()
	args := PrintArgs{
		Out:  out,
		Mode: PrintValues,
	}
	n.Literal.printRec(BackgroundContext, args, 0)
	return out.String()
}

func (n *ASTLiteral) hash(_ *bindings) hash.Hash {
	return n.Literal.Hash()
}

func (n *ASTLiteral) eval(ctx context.Context, env *bindings) Value {
	return n.Literal
}

func (n *ASTLiteral) pos() scanner.Position { return n.Pos }

// ASTLambda is an ASTNode for a user-defined function, "func(params...) {statements...}".
type ASTLambda struct {
	Pos  scanner.Position
	Args []FormalArg
	// Body is the function body.
	Body ASTNode
}

var _ ASTNode = &ASTLambda{}

// NewASTLambda creates a new ASTLambda node.
func NewASTLambda(pos scanner.Position, params []string, body ASTNode) *ASTLambda {
	args := make([]FormalArg, len(params))
	for i := range params {
		args[i].Name = symbol.Intern(params[i])
		args[i].Positional = true
		args[i].Required = true
		args[i].DefaultValue = Null
	}
	return &ASTLambda{Pos: pos, Args: args, Body: body}
}

func (n *ASTLambda) eval(ctx context.Context, env *bindings) Value {
	f := NewUserDefinedFunc(n, env, n.Args, n.Body)
	return NewFunc(f)
}

func (n ASTLambda) pos() scanner.Position { return n.Pos }

func (n *ASTLambda) hash(env *bindings) hash.Hash {
	f := NewUserDefinedFunc(n, env, n.Args, n.Body)
	return f.hash
}

// String implements the ASTNode interface.
func (n *ASTLambda) String() string {
	buf := strings.Builder{}
	buf.WriteString("func(")
	for i, arg := range n.Args {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(arg.Name.Str())
	}
	buf.WriteString(")")
	buf.WriteString(n.Body.String())
	return buf.String()
}

// ASTStructLiteralField represents a column within a struct literal expression
// '{f0:=val0,f1:=val1,...}.
type ASTStructLiteralField struct {
	Pos  scanner.Position
	Name symbol.ID // column name. may be symbol.Invalid.
	Expr ASTNode
}

// NewASTStructLiteralField creates a new ASTStructLiteralField. name is the the
// column (field) name, and expr is an expression that refers to the struct.
func NewASTStructLiteralField(pos scanner.Position, name string, expr ASTNode) ASTStructLiteralField {
	symID := symbol.Invalid
	if name != "" {
		symID = symbol.Intern(name)
	}
	return ASTStructLiteralField{pos, symID, expr}
}

// ASTStructLiteral is an ASTNode that represents a struct literal
// '{f0:=val0,f1:=val1,...}.
type ASTStructLiteral struct {
	Pos    scanner.Position
	Fields []ASTStructLiteralField
}

var _ ASTNode = &ASTStructLiteral{}

// NewASTStructLiteral creates a new ASTStructLiteral.
func NewASTStructLiteral(pos scanner.Position, fields []ASTStructLiteralField) *ASTStructLiteral {
	st := &ASTStructLiteral{Pos: pos, Fields: make([]ASTStructLiteralField, len(fields))}
	copy(st.Fields, fields)
	for fi := range st.Fields {
		if st.Fields[fi].Name != symbol.Invalid {
			continue
		}
		// Pick a default field name if not given by the user.
		var fieldName symbol.ID
		switch ft := st.Fields[fi].Expr.(type) {
		case *ASTColumnRef:
			fieldName = ft.Col
		case *ASTImplicitColumnRef:
			fieldName = ft.Col
		case *ASTStructFieldRef:
			fieldName = ft.Field
		case *ASTVarRef:
			fieldName = ft.Var
		default:
			fieldName = symbol.Intern(fmt.Sprintf("f%d", fi))
		}
		st.Fields[fi].Name = fieldName
	}
	return st
}

func (n *ASTStructLiteral) pos() scanner.Position { return n.Pos }

func (n *ASTStructLiteral) hash(env *bindings) hash.Hash {
	h := hash.Hash{
		0x63, 0x00, 0x25, 0xb0, 0x29, 0x04, 0x32, 0x9c,
		0x77, 0x5c, 0x0e, 0x8c, 0xea, 0x20, 0xfc, 0x87,
		0x22, 0xfd, 0x49, 0xac, 0x8f, 0x9a, 0x9a, 0xb1,
		0xd8, 0xdb, 0x4b, 0xae, 0x9e, 0x11, 0x82, 0xc3}
	for _, f := range n.Fields {
		h = h.Merge(f.Name.Hash())
		h = h.Merge(f.Expr.hash(env))
	}
	return h
}

//go:generate ../../../../github.com/grailbio/base/gtl/generate_randomized_freepool.py --prefix=structField --PREFIX=structField --package=gql --output=struct_field_pool -DELEM=[]StructField
var structFieldPool = NewstructFieldFreePool(func() []StructField { return nil }, 1024)

func (n *ASTStructLiteral) eval(ctx context.Context, env *bindings) Value {
	tmp := structFieldPool.Get()
	tmp = tmp[:0]
	for _, f := range n.Fields {
		val := f.Expr.eval(ctx, env)
		switch val.Type() {
		case StructFragmentType:
			tmp = append(tmp, val.StructFragment()...)
		default:
			tmp = append(tmp, StructField{f.Name, val})
		}
	}
	st := NewStruct(NewSimpleStruct(tmp...))
	tmp = tmp[:0]
	structFieldPool.Put(tmp)
	return st
}

// String implements the ASTNode interface.
func (n *ASTStructLiteral) String() string {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte('{')
	for fi, field := range n.Fields {
		if fi > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(field.Name.Str())
		buf.WriteByte(':')
		buf.WriteString(field.Expr.String())
	}
	buf.WriteByte('}')
	return buf.String()
}

// ASTCondOp is an ASTNode implementation for "cond(if,then,else)".
type ASTCondOp struct {
	// Pos is the position of the start of the expression.
	Pos scanner.Position
	// Cond is the conditional.
	Cond ASTNode
	// Then is the positive clause.
	Then ASTNode
	// Else is the negative clause.
	Else ASTNode
}

var _ ASTNode = &ASTCondOp{}

// String returns a human-readable description.
func (n *ASTCondOp) String() string {
	s := "cond(" + n.Cond.String() + "," + n.Then.String()
	if n.Else != nil {
		s += "," + n.Else.String()
	}
	return s + ")"
}

// pos implements the ASTNode interface.
func (n *ASTCondOp) pos() scanner.Position { return n.Pos }

func (n *ASTCondOp) hash(b *bindings) hash.Hash {
	h := hash.Hash{
		0xce, 0x6a, 0xf8, 0x76, 0x5c, 0xcf, 0xae, 0x5c,
		0xd2, 0x32, 0x48, 0xc9, 0xd4, 0x74, 0xec, 0x51,
		0x21, 0x58, 0x4d, 0xfe, 0x23, 0x26, 0x7b, 0x09,
		0xb1, 0x48, 0x93, 0xed, 0xfc, 0xa3, 0x7c, 0x23}
	h = h.Merge(n.Cond.hash(b)).Merge(n.Then.hash(b))
	if n.Else != nil {
		h = h.Merge(n.Else.hash(b))
	}
	return h
}

func (n *ASTCondOp) eval(ctx context.Context, b *bindings) Value {
	v := n.Cond.eval(ctx, b).Bool(n)
	if v {
		return n.Then.eval(ctx, b)
	}
	if n.Else == nil {
		return Null
	}
	return n.Else.eval(ctx, b)
}

// ASTLogicalOp is an ASTNode implementation for "||" and "&&" operators.
type ASTLogicalOp struct {
	// AndAnd is true for "&&", false for "||"
	AndAnd   bool
	LHS, RHS ASTNode
}

var _ ASTNode = &ASTLogicalOp{}

// String returns a human-readable description.
func (n *ASTLogicalOp) String() string {
	op := "&&"
	if !n.AndAnd {
		op = "||"
	}
	return n.LHS.String() + op + n.RHS.String()
}

// pos implements the ASTNode interface.
func (n *ASTLogicalOp) pos() scanner.Position { return n.LHS.pos() }

func (n *ASTLogicalOp) hash(b *bindings) hash.Hash {
	if n.AndAnd {
		h := hash.Hash{
			0x4a, 0x8a, 0xad, 0xbe, 0xc4, 0x1f, 0x3b, 0x86,
			0x9b, 0x1c, 0xf1, 0xd8, 0x3f, 0xaf, 0x73, 0x09,
			0xa2, 0x34, 0x3f, 0x86, 0xe5, 0x5e, 0xb8, 0x9c,
			0xc3, 0xcb, 0xfd, 0xad, 0x00, 0x33, 0xfa, 0x79}
		return h.Merge(n.LHS.hash(b)).Merge(n.RHS.hash(b))
	}
	h := hash.Hash{
		0x1d, 0x29, 0x42, 0x86, 0x76, 0x27, 0x07, 0x7f,
		0xc6, 0xbd, 0x61, 0xb9, 0xdb, 0x8e, 0x17, 0xf1,
		0x33, 0xf1, 0x17, 0xbf, 0xc0, 0xd3, 0xeb, 0xfd,
		0x0c, 0x02, 0x25, 0x52, 0x94, 0x69, 0x06, 0x3d}
	return h.Merge(n.LHS.hash(b)).Merge(n.RHS.hash(b))
}

func (n *ASTLogicalOp) eval(ctx context.Context, b *bindings) Value {
	v := n.LHS.eval(ctx, b).Bool(n)
	if n.AndAnd {
		if !v {
			return False
		}
	} else {
		if v {
			return True
		}
	}
	vR := n.RHS.eval(ctx, b)
	_ = vR.Bool(n)
	return vR
}

// ASTParamVal represents a function-call parameter that appears in the source
// code.
type ASTParamVal struct {
	Pos scanner.Position
	// Name is set to symbol.Intern("foo") if the actual arg is 'foo:=expr'. It is
	// symbol.Invalid for a positional arg.
	Name symbol.ID
	// Expr is the actual arg expression.
	Expr ASTNode
	// PipeSource is true if if this arg is a LHS of a pipe operator.  E.g., take
	// expression `read("foo.tsv") | map($col==0)`. It is syntactically translated
	// into `map(read("foo.tsv"), $col==0)`. Its first arg, `read("foo.tsv")` is
	// marked as PipeSource. A pipesource argument is exempted from '&' expansion.
	PipeSource bool
}

// NewASTParamVal creates a new ASTParamVal.
func NewASTParamVal(pos scanner.Position, name string, expr ASTNode) ASTParamVal {
	symID := symbol.Invalid
	if name != "" {
		symID = symbol.Intern(name)
	}
	return ASTParamVal{Pos: pos, Name: symID, Expr: expr}
}

// ASTFuncall is an ASTNode for function call 'func(arg0,arg1,...)'
type ASTFuncall struct {
	Pos      scanner.Position
	Function ASTNode       // function body.
	Raw      []ASTParamVal // set by yacc.
	Args     []AIArg       // set in analyze
	Analyzed bool          // true after analyze() runs.
}

var _ ASTNode = &ASTFuncall{}

// NewASTFuncall creates a new ASTFuncall.
func NewASTFuncall(fun ASTNode, args []ASTParamVal) *ASTFuncall {
	if len(args) >= 64 { // we use a 64bit bitmap in analyze
		Panicf(fun, "Too many function args (max 64): %+v", args)
	}
	return &ASTFuncall{Pos: fun.pos(), Function: fun, Raw: args}
}

// NewASTPipe creates a syntax node for construct "left | fun(args)". It is
// translated into "fun(left, args)".
func NewASTPipe(left ASTNode, right ASTNode) *ASTFuncall {
	fc, ok := right.(*ASTFuncall)
	if !ok {
		log.Panicf("%v: %v | %v: the right side must be a function call", left.pos(), left, right)
	}
	newfc := &ASTFuncall{
		Pos:      fc.Function.pos(),
		Function: fc.Function,
		Raw:      make([]ASTParamVal, len(fc.Raw)+1),
	}
	newfc.Raw[0] = ASTParamVal{
		Pos:        left.pos(),
		Expr:       left,
		PipeSource: true,
	}
	copy(newfc.Raw[1:], fc.Raw)
	return newfc
}

// ActualArg represents an function-call argument.
//
// 1. If FormalArg.Symbol==true, only the Symbol field will be set.
//
// 2. Else Value and Expr are set. Value is the actual
// fully-evaluated arg value. Expr is useful primarily to show a informative
// error message.
type ActualArg struct {
	Name   symbol.ID // Formal arg name. Copied from FormalArg.Name
	Value  Value     // Set for eagerly evaluated arg.
	Expr   ASTNode   // Points to the source code element. Set unless the arg is optional and is not provided by the caller.
	Symbol symbol.ID // Set only for symbol arg.
}

// Int retrieves the int64 value from arg.Value. A shorthand for arg.Value.Int(arg.Expr).
func (arg *ActualArg) Int() int64 { return arg.Value.Int(arg.Expr) }

// Table retrieves the table64 value from arg.Value. A shorthand for arg.Value.Table(arg.Expr).
func (arg *ActualArg) Table() Table { return arg.Value.Table(arg.Expr) }

// Float retrieves the float64 value from arg.Value. A shorthand for arg.Value.Float(arg.Expr).
func (arg *ActualArg) Float() float64 { return arg.Value.Float(arg.Expr) }

// Str retrieves the string value from arg.Value. A shorthand for arg.Value.Str(arg.Expr).
func (arg *ActualArg) Str() string { return arg.Value.Str(arg.Expr) }

// Bool retrieves the bool value from arg.Value. A shorthand for arg.Value.Bool(arg.Expr).
func (arg *ActualArg) Bool() bool { return arg.Value.Bool(arg.Expr) }

// Struct retrieves the struct value from arg.Value. A shorthand for arg.Value.Struct(arg.Expr).
func (arg *ActualArg) Struct() Struct { return arg.Value.Struct(arg.Expr) }

// Char retrieves the char value from arg.Value. A shorthand for arg.Value.Char(arg.Expr).
func (arg *ActualArg) Char() rune { return arg.Value.Char(arg.Expr) }

// DateTime retrieves the char value from arg.Value. A shorthand for arg.Value.DateTime(arg.Expr).
func (arg *ActualArg) DateTime() time.Time { return arg.Value.DateTime(arg.Expr) }

// Duration retrieves the duration value from arg.Value. A shorthand for arg.Value.Duration(arg.Expr).
func (arg *ActualArg) Duration() time.Duration { return arg.Value.Duration(arg.Expr) }

// Func retrieves the function closure from arg.Value. A shorthand for arg.Value.Func(arg.Expr).
func (arg *ActualArg) Func() *Func { return arg.Value.Func(arg.Expr) }

//go:generate ../../../../github.com/grailbio/base/gtl/generate_randomized_freepool.py --prefix=actualArg --PREFIX=actualArg --package=gql --output=actual_arg_pool -DELEM=[]ActualArg
var actualArgPool = NewactualArgFreePool(func() []ActualArg { return make([]ActualArg, 0, 8) }, 1024)

func (n *ASTFuncall) eval(ctx context.Context, env *bindings) Value {
	if !n.Analyzed {
		Panicf(n, "analyze not called")
	}
	f := n.Function.eval(ctx, env).Func(n)
	actualArgs := actualArgPool.Get()
	for _, at := range n.Args {
		switch {
		case at.Symbol != symbol.Invalid:
			actualArgs = append(actualArgs, ActualArg{Name: at.Name, Symbol: at.Symbol, Expr: at.Expr})
		case at.Expr != nil: // eager arg
			actualArgs = append(actualArgs, ActualArg{Name: at.Name, Value: at.Expr.eval(ctx, env), Expr: at.Expr})
		default: // missing optional arg
			aarg := ActualArg{Name: at.Name, Value: at.DefaultValue, Expr: at.Expr}
			actualArgs = append(actualArgs, aarg)
		}
	}
	val := f.funcCB(ctx, n, actualArgs)
	actualArgs = actualArgs[:0]
	actualArgPool.Put(actualArgs)
	return val
}

// pos implements ASTNode.
func (n *ASTFuncall) pos() scanner.Position { return n.Pos }

func (n *ASTFuncall) hash(b *bindings) hash.Hash {
	if !n.Analyzed {
		Panicf(n, "analyze not called")
	}
	h := hash.Hash{
		0x0f, 0x39, 0xee, 0x65, 0x9a, 0x28, 0xbf, 0x3b,
		0xa8, 0xb5, 0x27, 0x92, 0x2d, 0x5e, 0x86, 0x32,
		0x7b, 0x41, 0x16, 0x89, 0xb8, 0x57, 0x58, 0xf8,
		0xfc, 0x37, 0xe8, 0x99, 0xda, 0xb9, 0x9b, 0x87}
	h = h.Merge(n.Function.hash(b))
	for i, arg := range n.Args {
		h = h.Merge(hash.Int(int64(i)))
		if arg.Name != symbol.Invalid {
			h = h.Merge(arg.Name.Hash())
		}
		if arg.Expr != nil { // omitted optional args have arg.Expr==nil
			if arg.Symbol != symbol.Invalid {
				h = h.Merge(arg.Symbol.Hash())
			} else {
				h = h.Merge(arg.Expr.hash(b))
			}
		}
	}
	return h
}

// String implements the ASTNode interface.
func (n *ASTFuncall) String() string {
	var args []string
	if !n.Analyzed {
		for _, arg := range n.Raw {
			var s string
			if arg.Name != symbol.Invalid {
				s = fmt.Sprintf("%s:=%+v", arg.Name.Str(), arg.Expr.String())
			} else {
				s = fmt.Sprintf("%+v", arg.Expr.String())
			}
			args = append(args, s)
		}
	} else {
		for _, arg := range n.Args {
			var s strings.Builder
			if arg.Name != symbol.Invalid {
				s.WriteString(arg.Name.Str())
				s.WriteString(":=")
			}
			if arg.Expr != nil {
				s.WriteString(arg.Expr.String())
			} else {
				s.WriteString("[default]")
			}
			args = append(args, s.String())
		}
	}
	funcName := n.Function.String()
	switch {
	case strings.HasPrefix(funcName, "infix:") && len(args) == 2:
		return fmt.Sprintf("(%s%s%s)", args[0], funcName[6:], args[1])
	case strings.HasPrefix(funcName, "prefix:") && len(args) == 1:
		return fmt.Sprintf("%s%s", funcName[7:], args[0])
	default:
		return fmt.Sprintf("%s(%s)", funcName, strings.Join(args, ","))
	}
}

// ASTVarRef represents a symbol reference.
type ASTVarRef struct {
	Pos scanner.Position
	Var symbol.ID // variable name
}

var _ ASTNode = &ASTVarRef{}

// pos implements ASTNode.
func (n *ASTVarRef) pos() scanner.Position { return n.Pos }

func (n *ASTVarRef) hash(b *bindings) hash.Hash {
	h := hash.Hash{
		0xf9, 0xcc, 0x53, 0x17, 0x4a, 0x97, 0x13, 0xc8,
		0xe2, 0x46, 0xf3, 0x59, 0x83, 0x7d, 0x74, 0xd8,
		0xce, 0xab, 0x23, 0x6c, 0x53, 0x63, 0x34, 0x95,
		0x47, 0x97, 0xbf, 0xbc, 0x09, 0x9f, 0x3b, 0x36}
	val, ok := b.Lookup(n.Var)
	if !ok {
		Panicf(n, "variable not found, bindings are: %+v", b.Describe())
	}
	return h.Merge(n.Var.Hash()).Merge(val.Hash())
}

func (n *ASTVarRef) eval(ctx context.Context, env *bindings) Value {
	if val, ok := env.Lookup(n.Var); ok {
		return val
	}
	Panicf(n, "variable not found; registered variables are:\n%v", env.Describe())
	return Value{}
}

// String implements the ASTNode interface.
func (n *ASTVarRef) String() string { return n.Var.Str() }

// ASTColumnRef represents a "_.column" expression. It is produced as a result
// of rewriting &column.
type ASTColumnRef struct {
	// Pos is the location of this node in the source file.
	Pos scanner.Position
	// Col is the column name
	Col symbol.ID

	// Deprecated is true if this syntax node was created by a "$column"
	// expression, as opposed to "&column".
	Deprecated bool
}

var _ ASTNode = &ASTColumnRef{}

// pos implements ASTNode.
func (n *ASTColumnRef) pos() scanner.Position { return n.Pos }

func (n *ASTColumnRef) hash(_ *bindings) hash.Hash {
	h := hash.Hash{
		0x00, 0x55, 0xd3, 0x91, 0x7e, 0xd0, 0x4c, 0x2b,
		0xad, 0x34, 0x17, 0x37, 0x1e, 0xd4, 0x19, 0x86,
		0xe2, 0x3f, 0x1e, 0x87, 0xa6, 0xad, 0x94, 0x88,
		0x7b, 0x27, 0x31, 0x21, 0xa5, 0x92, 0xc3, 0x60}
	return h.Merge(n.Col.Hash())
}

func (n *ASTColumnRef) eval(ctx context.Context, env *bindings) Value {
	row, ok := env.Lookup(symbol.AnonRow)
	if !ok {
		Panicf(n, "variable '_' not set in the current context; registered variables are:\n%v", env.Describe())
	}
	if row.Type() != StructType {
		Panicf(n, "row not a struct type, but %v", row)
	}
	if val, ok := row.Struct(astUnknown).Value(n.Col); ok {
		return val
	}
	Panicf(n, "column not found. Row contents are: %v", row)
	return Value{}
}

// String implements the ASTNode interface.
func (n *ASTColumnRef) String() string { return "$" + n.Col.Str() }

// ASTImplicitColumnRef represents a "&column" expression. It can appear only in
// function-call arguments. "&column" is rewritten into "|_|{_.column}" by
// replaceImplicitColumnRef.
type ASTImplicitColumnRef struct {
	// Pos is the location of this node in the source file.
	Pos scanner.Position
	// Col is the column name.
	Col symbol.ID
}

var _ ASTNode = &ASTImplicitColumnRef{}

// pos implements ASTNode.
func (n *ASTImplicitColumnRef) pos() scanner.Position { return n.Pos }

func (n *ASTImplicitColumnRef) hash(_ *bindings) hash.Hash { panic("hash") }

func (n *ASTImplicitColumnRef) eval(ctx context.Context, env *bindings) Value { panic("eval") }

// String implements the ASTNode interface.
func (n *ASTImplicitColumnRef) String() string { return "&" + n.Col.Str() }

// ASTStructFieldRef represents an expression  "struct.field".
type ASTStructFieldRef struct {
	Parent ASTNode   // Refers to the struct object.
	Field  symbol.ID // Field in the struct.
}

var _ ASTNode = &ASTStructFieldRef{}

// NewASTStructFieldRef creates a new ASTStructFieldRef.
func NewASTStructFieldRef(parent ASTNode, name string) *ASTStructFieldRef {
	return &ASTStructFieldRef{Parent: parent, Field: symbol.Intern(name)}
}

// String implements the ASTNode interface.
func (n *ASTStructFieldRef) String() string {
	return fmt.Sprintf("%s.%s", n.Parent.String(), n.Field.Str())
}

func (n *ASTStructFieldRef) eval(ctx context.Context, env *bindings) Value {
	p := n.Parent.eval(ctx, env).Struct(n)
	val, ok := p.Value(n.Field)
	if !ok {
		return Null
	}
	return val
}

// pos implements ASTNode.
func (n *ASTStructFieldRef) pos() scanner.Position { return n.Parent.pos() }

func (n *ASTStructFieldRef) hash(b *bindings) hash.Hash {
	h := hash.Hash{
		0x6e, 0x41, 0xd2, 0x25, 0xf7, 0x39, 0x59, 0x35,
		0xcd, 0x4e, 0xcc, 0x4a, 0x7e, 0x30, 0x91, 0x0f,
		0xde, 0x5b, 0x63, 0xe7, 0xb6, 0xbe, 0x19, 0x01,
		0x2a, 0xa7, 0xb6, 0x9e, 0x09, 0xc3, 0xbd, 0x79}
	h = h.Merge(n.Parent.hash(b))
	return h.Merge(n.Field.Hash())
}

// ASTStructFieldRegex is for evaluating expressions of form "struct./regex/".
// It evaluates into a value containing []StructField.
type ASTStructFieldRegex struct {
	parent ASTNode        // struct
	re     *regexp.Regexp // regexp of field names in the parent struct.
	Pos    scanner.Position
}

var _ ASTNode = &ASTStructFieldRegex{}

// MarshalBinary implements the GOB interface.
func (n *ASTStructFieldRegex) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	encodeGOB(n, enc, &n.parent)
	encodeGOB(n, enc, n.re.String())
	encodeGOB(n, enc, &n.Pos)
	return buf.Bytes(), nil
}

// UnmarshalBinary implements the GOB interface.
func (n *ASTStructFieldRegex) UnmarshalBinary(data []byte) error {
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	decodeGOB(n, dec, &n.parent)
	var reStr string
	decodeGOB(n, dec, &reStr)
	n.re = regexp.MustCompile(reStr)
	decodeGOB(n, dec, &n.Pos)
	return nil
}

// NewASTStructFieldRegex creates a new ASTStructFieldRegex.
func NewASTStructFieldRegex(pos scanner.Position, parent ASTNode, re string) *ASTStructFieldRegex {
	if parent == nil {
		parent = &ASTVarRef{Pos: pos, Var: symbol.AnonRow}
	}
	return &ASTStructFieldRegex{Pos: pos, parent: parent, re: regexp.MustCompile(re)}
}

// String implements the ASTNode interface.
func (n *ASTStructFieldRegex) String() string {
	return fmt.Sprintf("%s./%v/", n.parent.String(), n.re.String())
}

func (n *ASTStructFieldRegex) eval(ctx context.Context, env *bindings) Value {
	p := n.parent.eval(ctx, env).Struct(n)

	cols := []StructField{}
	nFields := p.Len()
	for fi := 0; fi < nFields; fi++ {
		col := p.Field(fi)
		if n.re.FindString(col.Name.Str()) != "" {
			cols = append(cols, col)
		}
	}
	if len(cols) == 0 {
		Panicf(n, "No key matched")
	}
	return NewStructFragment(cols)
}

func (n *ASTStructFieldRegex) pos() scanner.Position { return n.Pos }

func (n *ASTStructFieldRegex) hash(b *bindings) hash.Hash {
	h := hash.Hash{
		0xa8, 0x8e, 0x9b, 0x27, 0x2e, 0xf5, 0x0b, 0xe0,
		0x77, 0x3a, 0xa6, 0x4a, 0x74, 0x23, 0x3e, 0x87,
		0x00, 0xe1, 0xfe, 0x45, 0xd8, 0x90, 0xe1, 0x72,
		0x41, 0x1b, 0x46, 0xc8, 0x7d, 0xc6, 0x7c, 0x5b}
	h = h.Merge(n.parent.hash(b))
	return h.Merge(hash.String(n.re.String()))
}

// NewASTBuiltinFuncall creates a new ASTFuncall object. Is is used for infix or
// prefix operators.
func NewASTBuiltinFuncall(pos scanner.Position, f Value, args ...ASTNode) *ASTFuncall {
	if f.Type() != FuncType {
		log.Panicf("%v: not a function: %v", pos, f)
	}
	n := &ASTFuncall{
		Pos:      pos,
		Function: &ASTLiteral{Literal: f},
		Raw:      make([]ASTParamVal, len(args)),
	}
	for i, arg := range args {
		n.Raw[i].Expr = arg
	}
	return n
}

func init() {
	gob.Register(&ASTLiteral{})
	gob.Register(&ASTVarRef{})
	gob.Register(&ASTColumnRef{})
	gob.Register(&ASTImplicitColumnRef{})
	gob.Register(&ASTCondOp{})
	gob.Register(&ASTLogicalOp{})
	gob.Register(&ASTFuncall{})
	gob.Register(&ASTLambda{})
	gob.Register(&ASTStructLiteral{})
	gob.Register(&ASTStructFieldRef{})
	gob.Register(&ASTStructFieldRegex{})
	gob.Register(&ASTBlock{})
}

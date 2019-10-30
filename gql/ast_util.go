package gql

// Static typechecking and optimization utilities.

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/grailbio/gql/symbol"
)

// AstTypes is the central repository of the type information of all the AST nodes
// in the program. Use add to register nodes in a tree. Thread compatible.
type astTypes struct {
	types map[ASTNode]AIType
}

// NewASTTypes creates an empty astTypes.
func newASTTypes() *astTypes {
	return &astTypes{types: map[ASTNode]AIType{}}
}

// Add analyzes the types of the given node and its subtree.
func (t *astTypes) add(n ASTNode, env *aiBindings) (typ AIType) {
	switch n := n.(type) {
	case *ASTLiteral:
		typ = t.addLiteral(n)
	case *ASTLambda:
		typ = t.addLambda(n, env)
	case *ASTBlock:
		typ = t.addBlock(n, env)
	case *ASTStructLiteral:
		for _, f := range n.Fields {
			t.add(f.Expr, env)
		}
		typ = AIStructType
	case *ASTCondOp:
		tvCond := t.add(n.Cond, env)
		if !tvCond.Is(BoolType) {
			Panicf(n, "conditional expression is not bool (is %v)", tvCond)
		}
		tvThen := t.add(n.Then, env)
		tvElse := t.add(n.Else, env)
		if tvCond.Literal != nil {
			if tvCond.Literal.Bool(n) {
				typ = tvThen
			} else {
				typ = tvElse
			}
		} else {
			typ = combineTypes(n, []AIType{tvThen, tvElse})
		}
	case *ASTLogicalOp:
		tvLHS := t.add(n.LHS, env)
		tvRHS := t.add(n.RHS, env)
		if !tvLHS.Is(BoolType) {
			Panicf(n, "LHS is not bool (is %v)", tvLHS)
		}
		if !tvRHS.Is(BoolType) {
			Panicf(n, "RHS is not bool (is %v)", tvRHS)
		}
		typ = AIBoolType
		if tvLHS.Literal != nil {
			lhs := tvLHS.Literal.Bool(n)
			switch {
			case n.AndAnd:
				if lhs {
					typ = tvRHS
				} else {
					typ = AIType{Type: BoolType, Literal: &False}
				}
			default: // ||
				if lhs {
					typ = AIType{Type: BoolType, Literal: &True}
				} else {
					typ = tvRHS
				}
			}
		}
	case *ASTFuncall:
		typ = t.addFuncall(n, env)
	case *ASTVarRef:
		var ok bool
		typ, ok = env.Lookup(n.Var)
		if !ok {
			Panicf(n, "analyze: variable not found, bindings are: %+v", env)
		}
	case *ASTColumnRef:
		name := symbol.AnonRow
		var ok bool
		typ, ok = env.Lookup(name)
		if !ok {
			Panicf(n, "Variable '%s' not found in: %v", name.Str(), env)
		}
		if !typ.Is(StructType) {
			Panicf(n, "Variable '%s' not a struct", name.Str())
		}
		typ = AIAnyType
	case *ASTStructFieldRef:
		typParent := t.add(n.Parent, env)
		if !typParent.Is(StructType) {
			Panicf(n, "reading field of a non-struct type (%+v)", typParent)
		}
		typ = AIAnyType
	case *ASTStructFieldRegex:
		typ = t.add(n.parent, env)
		if !typ.Is(StructType) {
			Panicf(n, "regex: not a struct (is %+v)", typ)
		}
	default:
		Panicf(n, "Unknown AST type")
	}
	if typ.Type == InvalidType && !typ.Any {
		Panicf(n, "invalid type: %+v", typ)
	}
	t.addType(n, typ)
	return typ
}

func (t *astTypes) addType(n ASTNode, typ AIType) {
	if _, ok := t.types[n]; ok {
		panic(n)
	}
	t.types[n] = typ
}

func (t *astTypes) getType(n ASTNode) AIType {
	typ, ok := t.types[n]
	if !ok {
		Panicf(n, "type unknown")
	}
	return typ
}

// addLiteral is called by add() to analyze a literal.
func (t *astTypes) addLiteral(n *ASTLiteral) AIType {
	typ := AIType{
		Type:    n.Literal.Type(),
		Literal: &n.Literal,
	}
	if typ.Type == FuncType {
		f := n.Literal.Func(n)
		// This path is only for builtin functions.
		typ.FormalArgs = f.formalArgs
		typ.TypeCB = f.typeCB
	}
	return typ
}

// addLambda is called by add() to analyze a function literal.
func (t *astTypes) addLambda(n *ASTLambda, env *aiBindings) AIType {
	newFrame := aiFrame{}
	for _, arg := range n.Args {
		if !arg.Required { // we don't support exotic args in lambdas yet.
			panic(arg)
		}
		newFrame[arg.Name] = AIAnyType
	}
	newEnv := env.PushFrame(newFrame)
	typ := t.add(n.Body, &newEnv)
	return AIType{
		Type:       FuncType,
		FormalArgs: n.Args,
		TypeCB:     func(ast ASTNode, args []AIArg) AIType { return typ },
	}
}

// addLambda is called by add() to analyze '{ exprs... }'
func (t *astTypes) addBlock(n *ASTBlock, env *aiBindings) AIType {
	newFrame := aiFrame{}
	newEnv := env.PushFrame(newFrame)
	var typ AIType
	if len(n.Statements) == 0 {
		Panicf(n, "empty statement")
	}
	for i := range n.Statements {
		s := &n.Statements[i]
		typ = t.add(s.Expr, &newEnv)
		if s.LHS != symbol.Invalid {
			newFrame[s.LHS] = typ
		}
	}
	return typ
}

// addFuncall is called by add() to analyze a function call.
func (t *astTypes) addFuncall(n *ASTFuncall, env *aiBindings) AIType {
	funcType := t.add(n.Function, env)
	if funcType.Type != FuncType {
		Panicf(n, "non-function call node of type %v", funcType.Type)
	}
	if funcType.TypeCB == nil {
		Panicf(n, "nil function typecheck callback")
	}

	args := []AIArg{}

	// getNamedArg finds a named arg (name:=val) from n.Raw. It returns the index
	// of n.Raw, or -1.
	getNamedArg := func(name symbol.ID) int {
		for i, aarg := range n.Raw {
			if aarg.Name == name {
				return i
			}
		}
		return -1
	}

	// getArg finds the actual argument that matches the given formal arg.  It
	// returns nil if not found. "pos" is the index (0,1,...) of the farg.
	getArg := func(pos int, farg FormalArg, remainingBitmap *bitmap64) *ASTParamVal {
		if farg.Positional {
			if len(n.Raw) <= pos || n.Raw[pos].Name != symbol.Invalid {
				return nil
			}
			if !remainingBitmap.tryClear(pos) {
				Panicf(n, "arg #%d already set", pos)
			}
			return &n.Raw[pos]
		}
		if farg.Name == symbol.Invalid {
			panic(farg)
		}
		idx := getNamedArg(farg.Name)
		if idx < 0 {
			return nil
		}
		if !remainingBitmap.tryClear(idx) {
			Panicf(n, "arg #%d ('%s') appears twice", pos, farg.Name.Str())
		}
		return &n.Raw[idx]

	}

	addAIArg := func(farg FormalArg, arg ASTParamVal) {
		expr := arg.Expr
		// Replace '&arg' to a function.  Replacement is applied only to arguments
		// that aren't pipe sources. Consider:
		//
		//   table | filter(&x==10) | sort(&y)
		//
		// It is translated into sort(filter(table, &x==10), &y).  The
		// 'filter(table, &x==10)' arg to sort is marked as source, and 'table' arg
		// to filter is also marked as such. The pipesource arg is not subject to
		// recursive &-expansion. This heuristics allows the above example to expand into:
		//
		//   table | filter(|_|_.x==10) | sort(|_|_.y)
		if !arg.PipeSource {
			replaceImplicitColumnRef(&expr)
		}
		warnDeprecatedColumnRef(expr)
		_, argIsLambda := expr.(*ASTLambda)
		switch {
		case farg.Closure && !argIsLambda:
			if len(farg.ClosureArgs) == 0 {
				Panicf(n, "No closure arg supplied")
			}
			// Translate the "expr" to "func(args..) { expr }", where args... are
			// names listed in farg.ClosureArgs.
			var cargNames []string
			for _, carg := range farg.ClosureArgs {
				name := carg.Name
				if carg.Override != symbol.Invalid {
					// See if have a "row:=name" style of argument. It overrides the name
					// of the func arg.
					if idx := getNamedArg(carg.Override); idx >= 0 {
						symArg := n.Raw[idx].Expr
						switch t := symArg.(type) {
						case *ASTVarRef:
							name = t.Var
						default:
							Panicf(symArg, "arg must be a symbol")
						}
					}
				}
				cargNames = append(cargNames, name.Str())
			}
			expr = NewASTLambda(expr.pos(), cargNames, expr)
			t.add(expr, env)
			args = append(args, AIArg{Name: farg.Name, Type: t.getType(expr), Expr: expr, DefaultValue: Null})
		case farg.JoinClosure && !argIsLambda:
			// Translate the "expr" to "func(args..) { expr }", where args... are
			// names listed in farg.ClosureArgs.
			var cargNames []string
			tablesArg := n.Raw[0].Expr.(*ASTStructLiteral)
			for _, tableEntry := range tablesArg.Fields {
				cargNames = append(cargNames, tableEntry.Name.Str())
			}
			expr = NewASTLambda(expr.pos(), cargNames, expr)
			t.add(expr, env)
			args = append(args, AIArg{Name: farg.Name, Type: t.getType(expr), Expr: expr, DefaultValue: Null})
		case farg.Symbol:
			switch t := expr.(type) {
			case *ASTVarRef:
				args = append(args, AIArg{Name: farg.Name, Symbol: t.Var, Expr: expr, DefaultValue: Null})
				return
			default:
				Panicf(n, "Expect a symbol for argument %s, but found %s", farg.Name.Str(), expr.String())
			}
		default:
			t.add(expr, env)
			argType := t.getType(expr)
			args = append(args, AIArg{Name: farg.Name, Type: argType, Expr: expr, DefaultValue: Null})
			if len(farg.Types) > 0 {
				ok := false
				for _, expected := range farg.Types {
					if argType.Is(expected) {
						ok = true
						break
					}
				}
				if !ok {
					Panicf(n, "wrong argument type: %s (of type %v, but expect %v)", expr.String(), argType.Type, farg.Types)
				}
			}
		}
	}

	// Match formal args and actual args provided the caller.
	remaining := newbitmap64(len(n.Raw))
	i := 0
	for _, farg := range funcType.FormalArgs {
		if !farg.DefaultValue.Valid() {
			// gob.isZero() inspects the Value fields (specifically unsafe.Pointer) even if
			// we set the Value.MarshalBinary handler, so set a non-zero field value.
			Panicf(n, "no default value set for arg %s", farg.Name.Str())
		}
		aarg := getArg(i, farg, &remaining)
		if aarg == nil {
			if farg.Required {
				Panicf(n, "arg #%d is missing in call to function", i)
			}
			if farg.Variadic {
				continue
			}
			if farg.Symbol {
				args = append(args, AIArg{Name: farg.Name, Symbol: farg.DefaultSymbol, DefaultValue: Null})
			} else {
				args = append(args, AIArg{Name: farg.Name, DefaultValue: farg.DefaultValue})
			}
			i++
			continue
		}
		addAIArg(farg, *aarg)
		i++
		if farg.Variadic {
			for {
				aarg := getArg(i, farg, &remaining)
				if aarg == nil {
					break
				}
				addAIArg(farg, *aarg)
				i++
			}
		}
	}
	if remaining != 0 {
		msg := strings.Builder{}
		for i, raw := range n.Raw {
			if remaining.test(i) {
				fmt.Fprintf(&msg, "arg #%d", i)
				if raw.Name != symbol.Invalid {
					fmt.Fprintf(&msg, "['%s']", raw.Name.Str())
				}
				fmt.Fprintf(&msg, ", %v", raw.Expr)
			}
		}
		Panicf(n, "too many arguments to function: %s", msg.String())
	}

	// If funcType and args are all compile-time constants, this function runs the
	// function body eagerly and returns the (result, true).
	evalWithCompileTimeConstants := func(ctx context.Context, funcType AIType, args []AIArg) (Value, bool) {
		if funcType.Literal == nil {
			return Value{}, false
		}
		aargs := make([]ActualArg, len(args))
		for i, arg := range args {
			switch {
			case arg.Symbol != symbol.Invalid:
				aargs[i] = ActualArg{Expr: arg.Expr, Symbol: arg.Symbol}
			case arg.Type.Literal != nil:
				aargs[i] = ActualArg{Expr: arg.Expr, Value: *arg.Type.Literal}
			default:
				return Value{}, false
			}
		}
		return funcType.Literal.Func(n).funcCB(ctx, n, aargs), true
	}
	resultType := funcType.TypeCB(n, args)
	if resultType.Literal == nil {
		val, ok := evalWithCompileTimeConstants(BackgroundContext, funcType, args)
		if ok {
			resultType.Literal = &val
		}
	}
	n.Args = args
	n.Analyzed = true
	return resultType
}

// astChildren lists the direct children of the given node.  It returns a list
// of *ASTNodes, so that the caller can update the fields in the root if needed.
func astChildren(root ASTNode) (c []*ASTNode) {
	switch n := root.(type) {
	case *ASTLiteral:
		if n.Literal.Type() == FuncType {
			f := n.Literal.Func(root)
			if f.body != nil { // user-defined function
				c = append(c, &f.body)
			}
		}
	case *ASTLambda:
		c = append(c, &n.Body)
	case *ASTBlock:
		for i := range n.Statements {
			c = append(c, &n.Statements[i].Expr)
		}
	case *ASTStructLiteral:
		for i := range n.Fields {
			c = append(c, &n.Fields[i].Expr)
		}
	case *ASTCondOp:
		c = append(c, &n.Cond, &n.Then, &n.Else)
	case *ASTLogicalOp:
		c = append(c, &n.LHS, &n.RHS)
	case *ASTFuncall:
		if !n.Analyzed {
			panic(n)
		}
		c = append(c, &n.Function)
		for i := range n.Args {
			arg := &n.Args[i]
			if arg.Symbol == symbol.Invalid && arg.Expr != nil {
				c = append(c, &arg.Expr)
			}
		}
	case *ASTVarRef:
	case *ASTColumnRef:
	case *ASTStructFieldRef:
		c = append(c, &n.Parent)
	case *ASTStructFieldRegex:
		c = append(c, &n.parent)
	default:
		Panicf(n, "Invalid node type")
	}
	return
}

// visitRawASTTree invokes the given function on every node under the root. It
// is supposed to be invoked before function-call args are fully parsed. So for
// ASTFuncall nodes, it invkoes cb on the raw, unparsed args.
func visitRawASTTree(root *ASTNode, cb func(n *ASTNode) bool) bool {
	if !cb(root) {
		return false
	}
	switch v := (*root).(type) {
	case *ASTImplicitColumnRef:
	case *ASTFuncall:
		visitRawASTTree(&v.Function, cb)
		for i := range v.Raw {
			visitRawASTTree(&v.Raw[i].Expr, cb)
		}
	default:
		for _, child := range astChildren(*root) {
			visitRawASTTree(child, cb)
		}
	}
	return true
}

// Check if the expression contains '&var'.
func hasColumnVarRef(root *ASTNode) bool {
	hasRef := false
	visitRawASTTree(root, func(nptr *ASTNode) bool {
		if _, ok := (*nptr).(*ASTImplicitColumnRef); ok {
			hasRef = true
			return true
		}
		return true
	})
	return hasRef
}

var warnDeprecatedColumnRefOnce sync.Once

func warnDeprecatedColumnRef(root ASTNode) {
	visitRawASTTree(&root,
		func(nptr *ASTNode) bool {
			if n, ok := (*nptr).(*ASTColumnRef); ok && n.Deprecated {
				warnDeprecatedColumnRefOnce.Do(func() {
					Errorf(n, "$-syntax is deprecated. Use & instead. See https://phabricator.grailbio.com/w/docs/gql/#8-expansion for more details.")
				})
			}
			return true
		})
}

// replaceImplicitColumnRef rewrites an expression containing &xxx into
// func(_){_.xxx}.
func replaceImplicitColumnRef(nptr *ASTNode) bool {
	if !hasColumnVarRef(nptr) {
		return false
	}
	pos := (*nptr).pos()
	visitRawASTTree(nptr, func(n *ASTNode) bool {
		if v, ok := (*n).(*ASTImplicitColumnRef); ok {
			*n = &ASTColumnRef{
				Pos: v.Pos,
				Col: v.Col,
			}
		}
		return true
	})
	*nptr = &ASTLambda{
		Pos: pos,
		Args: []FormalArg{{
			Name:       symbol.AnonRow,
			Positional: true,
			Required:   true,
		}},
		Body: *nptr}
	return true
}

// replaceConstExprWithLiteral replaces an AST node that refers to a
// compile-time constant with the constant itself.
func replaceConstExprWithLiteral(t *astTypes, nptr *ASTNode) {
	n := *nptr
	if _, ok := n.(*ASTLiteral); !ok {
		typ := t.getType(n)
		if typ.Literal != nil {
			*nptr = &ASTLiteral{Pos: n.pos(), Literal: *typ.Literal, Org: n}
			t.addType(*nptr, typ)
		}
	}
	for _, child := range astChildren(*nptr) {
		replaceConstExprWithLiteral(t, child)
	}
}

// transformAST optimizes the given expression. It updates *nptr in place.
//
// REQUIRES: the type info of all the descendant nodes have been added to *t.
func transformAST(t *astTypes, nptr *ASTNode) {
	replaceConstExprWithLiteral(t, nptr)
}

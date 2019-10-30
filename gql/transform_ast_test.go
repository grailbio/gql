package gql

import (
	"context"
	"testing"

	"github.com/grailbio/testutil/expect"
)

// Clear ASTLiteral.Org field so that ASTLiteral.String() will print the
// constant value, not the original syntactic element.
func clearOrg(nptr *ASTNode) {
	n := *nptr
	if lit, ok := n.(*ASTLiteral); ok {
		lit.Org = nil
	}
	for _, child := range astChildren(*nptr) {
		clearOrg(child)
	}
}

func TestTransformAST(t *testing.T) {
	sess := newSession()
	ctx := context.Background()

	eval := func(str string) Value {
		statements, err := sess.Parse("(input)", []byte(str))
		if err != nil {
			t.Fatal(err)
		}
		return sess.EvalStatements(ctx, statements)
	}

	transform := func(str string) string {
		statements, err := sess.Parse("(input)", []byte(str))
		if err != nil {
			t.Fatal(err)
		}
		ast := statements[0].Expr
		env := sess.aiBindings()
		sess.types.add(ast, &env)
		transformAST(sess.types, &ast)
		clearOrg(&ast)
		return ast.String()
	}

	// Constant propagation.
	eval("foo := 20")
	expect.EQ(t, transform("foo+1"), "21")
	expect.EQ(t, transform("foo"), "20")
	expect.EQ(t, transform("foo * 3 + 5"), "65")
	expect.EQ(t, transform("foo * 3 % 7"), "4")
	expect.EQ(t, transform("if foo==20 55 else 66"), "55")
	expect.EQ(t, transform("if foo!=20 55 else 66"), "66")
	expect.EQ(t, transform("func(a) {foo+a}"), "func(a){(20+a)}")
	expect.EQ(t, transform("|a| foo+a"), "func(a)(20+a)")
}

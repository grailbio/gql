package gql

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"testing"

	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/assert"
)

func doParse(expr string, sess *Session) []ASTStatementOrLoad {
	p, err := sess.Parse("test", []byte(expr))
	if err != nil {
		panic(fmt.Sprintf("parse %s: %v", expr, err))
	}
	return p
}

// Eval parses and evaluates a given expression or statement.
func doEval(t testing.TB, str string, sess *Session) Value {
	statements := doParse(str, sess)
	return sess.EvalStatements(context.Background(), statements)
}

var once sync.Once

func newSession() *Session {
	once.Do(func() {
		if BackgroundContext != nil {
			// gol_test already initialized the thing
			return
		}
		Init(Opts{CacheDir: testutil.GetTmpDir()})
	})
	return NewSession()
}

func TestTypeCheck(t *testing.T) {
	sess := newSession()
	env := sess.aiBindings()

	typeCheck := func(ast ASTNode) AIType {
		sess.types.add(ast, &env)
		return sess.types.getType(ast)
	}

	p := doParse("10", sess)
	typ := typeCheck(p[0].Expr)
	assert.Equal(t, IntType, typ.Type)

	p = doParse("read(`foo.tsv`)", sess)
	typ = typeCheck(p[0].Expr)
	assert.Equal(t, TableType, typ.Type)

	doEval(t, "xxx := `blah.btsv`", sess)
	p = doParse("read(xxx)", sess)
	typ = typeCheck(p[0].Expr)
	assert.Equal(t, TableType, typ.Type)

	p = doParse("table(10,20) | map(xxx != `` || $x>10)", sess)
	typ = typeCheck(p[0].Expr)
	assert.Equal(t, TableType, typ.Type)

	p = doParse("table(10,20) | map(_.yy>10)", sess)
	typ = typeCheck(p[0].Expr)
	assert.Equal(t, TableType, typ.Type)

	p = doParse("table(10,20) | map(blah.yy>10, row:=blah)", sess)
	typ = typeCheck(p[0].Expr)
	assert.Equal(t, TableType, typ.Type)

	p = doParse("flatten(table(10,20), table(20,30))", sess)
	typ = typeCheck(p[0].Expr)
	assert.Equal(t, TableType, typ.Type)

	p = doParse("flatten(table(10,20), table(20,30))", sess)
	typ = typeCheck(p[0].Expr)
	assert.Equal(t, TableType, typ.Type)
}

func TestTypeCheckNested(t *testing.T) {
	sess := newSession()
	env := sess.aiBindings()
	typeCheck := func(ast ASTNode) AIType {
		sess.types.add(ast, &env)
		return sess.types.getType(ast)
	}
	p := doParse("table(10,20) | map(filter(blah, $xx>10), row:=blah)", sess)
	typ := typeCheck(p[0].Expr)
	assert.Equal(t, TableType, typ.Type)

	mapExpr := p[0].Expr.(*ASTFuncall).Raw[1].Expr
	assert.Equal(t, "filter(blah,func(_)($xx>10),map:=[default],shards:=[default],row:=[default])", mapExpr.String())
}

func testTypeCheckError(t *testing.T, expr, msgRe string) {
	defer func() {
		if p := recover(); p != nil {
			ps := fmt.Sprintf("%v", p)
			m, err := regexp.MatchString(msgRe, ps)
			if err != nil {
				panic("invalid regexp: " + msgRe)
			}
			if m {
				return
			}
			t.Errorf("Expect panic for '%s' with msg '%s', but found '%s'",
				expr, msgRe, ps)
		} else {
			t.Errorf("Expect panic for '%s', but it succeeded", expr)
		}
	}()

	sess := newSession()
	env := sess.aiBindings()
	p := doParse(expr, sess)
	sess.types.add(p[0].Expr, &env)
}

func testTypeCheckOk(t *testing.T, expr string) ValueType {
	sess := newSession()
	env := sess.aiBindings()
	p := doParse(expr, sess)
	sess.types.add(p[0].Expr, &env)
	return sess.types.getType(p[0].Expr).Type
}

func TestTypeCheckErrors(t *testing.T) {
	testTypeCheckError(t, "read(10)", "wrong argument type")
	testTypeCheckError(t, "read(blahblah)", "variable not found")
	testTypeCheckError(t, "string_len(10)", "wrong argument type")
	testTypeCheckError(t, "-{a:1}", "wrong argument type")
	testTypeCheckError(t, "flatten(10)", "wrong argument type")
	testTypeCheckError(t, "table(1,2) | pick(10)", "arg#1 .* is not bool")
	testTypeCheckError(t, "table(1,2) | map(_+10, filter:=10)", "filter .* is not bool")
	assert.Equal(t, testTypeCheckOk(t, "table(1,2) | map(_+10)"), TableType)
}

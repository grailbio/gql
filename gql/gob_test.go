package gql

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math"
	"testing"

	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/gql/termutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gobEncode(t *testing.T, v interface{}) []byte {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	require.NoError(t, enc.Encode(v))
	return buf.Bytes()
}

func gobDecode(t *testing.T, v interface{}, data []byte) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	require.NoError(t, dec.Decode(v))
}

func doReadTable(t Value) []string {
	s := []string{}
	scanner := t.Table(nil).Scanner(context.Background(), 0, 1, 1)
	for scanner.Scan() {
		s = append(s, scanner.Value().String())
	}
	return s
}

func TestGobLiteral(t *testing.T) {
	values := []Value{
		NewNull(PosNull),
		NewNull(NegNull),
		NewInt(10),
		NewFloat(12.5),
		NewString("blah"),
		NewFileName("/tmp/blah"),
		NewEnum("xxx"),
		NewChar('龍'),
		ParseDateTime("2017-12-11T13:14:15Z"),
		ParseDateTime("2017-12-11T13:14:15-0700"),
		ParseDateTime("2017-01-02"),
	}
	for _, v := range values {
		var lit ASTNode = &ASTLiteral{Literal: v}
		data := gobEncode(t, &lit)
		var lit2 ASTNode
		gobDecode(t, &lit2, data)
		assert.Equal(t,
			lit.(*ASTLiteral).Literal.String(),
			lit2.(*ASTLiteral).Literal.String())
	}
}

type TestInterface interface {
	Foo() string
}

type TestType struct {
	X int
}

type TestType2 struct {
	xx int
}

func (t TestType) Foo() string { return "blah" }

func (t TestType2) Foo() string { return "blah2" }

func (t TestType2) MarshalBinary() ([]byte, error) {
	return []byte("foohah"), nil
}

func (t *TestType2) UnmarshalBinary(data []byte) error {
	s := string(data)
	if s != "foohah" {
		panic("testtype2")
	}
	t.xx = 11
	return nil
}

func TestGobTest(t *testing.T) {
	gob.Register(TestType{})
	gob.Register(TestType2{})
	var tt TestInterface
	{
		tt = TestType{10}
		data := gobEncode(t, &tt)
		var tt2 TestInterface
		gobDecode(t, &tt2, data)
		assert.Equal(t, tt, tt2)
	}
	{
		tt = TestType2{11}
		data := gobEncode(t, &tt)
		var tt2 TestInterface
		gobDecode(t, &tt2, data)
		assert.Equal(t, tt, tt2)
	}
	{
		var tt TestInterface
		data := gobEncode(t, &tt)
		var tt2 TestInterface
		gobDecode(t, &tt2, data)
		assert.Nil(t, tt2)
	}
}

func TestMarshalTable(t *testing.T) {
	sess := newSession()
	path := "./testdata/data.tsv"
	path2 := "./testdata/data2.tsv"

	for _, testExpr := range []string{
		`table(10,20,30,"abc")`,
		fmt.Sprintf("read(`%s`)", path),
		fmt.Sprintf("read(`%s`) | firstn(10)", path),
		fmt.Sprintf("read(`%s`) | map({$A,$B}, filter:=$A==2)", path),
		fmt.Sprintf("flatten(table(read(`%s`), read(`%s`)))", path, path2),
	} {
		t.Logf("GobTable: test %s", testExpr)
		v := doEval(t, testExpr, sess)

		ctx, data := TestMarshalValue(t, v)
		v2 := TestUnmarshalValue(t, ctx, data)
		assert.Equal(t, doReadTable(v), doReadTable(v2))
	}
}

func printValueLong(v Value) string {
	out := termutil.NewBufferPrinter()
	args := PrintArgs{
		Out:                out,
		Mode:               PrintCompact,
		MaxInlinedTableLen: math.MaxInt64,
	}
	v.Print(context.Background(), args)
	return out.String()
}

func TestGobStruct(t *testing.T) {
	sess := newSession()
	v := doEval(t, "{1,2.0,`abc`}", sess)
	ctx, data := TestMarshalValue(t, v)
	v2 := TestUnmarshalValue(t, ctx, data)
	assert.Equal(t, printValueLong(v), printValueLong(v2))
}

func TestGobFormalArg(t *testing.T) {
	arg := FormalArg{Name: symbol.Intern("blah"), DefaultValue: NewString("xxx")}
	data := gobEncode(t, &arg)
	var arg2 FormalArg
	gobDecode(t, &arg2, data)
	assert.Equal(t, arg.Name, arg2.Name)
	assert.Equal(t, arg.DefaultValue.String(), arg2.DefaultValue.String())
}

func TestGobEnv(t *testing.T) {
	rt := NewSession().Bindings()

	sym0 := symbol.Intern("foo")
	sym1 := symbol.Intern("bar")
	rt.pushFrame2(sym0, NewInt(10), sym1, NewString("blah"))
	buf := marshal.NewEncoder(nil)
	ctx := newMarshalContext(context.Background())
	marshalBindings(ctx, buf, rt)

	uctx := newUnmarshalContext(ctx.marshal())
	r := marshal.NewDecoder(buf.Bytes())
	rt2 := unmarshalBindings(uctx, r)
	assert.Equal(t, 0, r.Len())
	v, _ := rt2.Lookup(symbol.Intern("foo"))
	assert.Equal(t, int64(10), v.Int(nil))
	v, _ = rt2.Lookup(symbol.Intern("bar"))
	assert.Equal(t, "blah", v.Str(nil))
}

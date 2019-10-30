package gql

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexOps(t *testing.T) {
	for _, testCase := range lexOpDefs {
		l := newLexer("test", bytes.NewReader([]byte(testCase.str)))
		var sym yySymType
		require.Equalf(t, testCase.tok, l.next(&sym), "test: %+v", testCase)
		require.Equalf(t, 0, l.next(&sym), "test: %+v", testCase)
	}
}

func TestLexDates(t *testing.T) {
	l := newLexer("test", bytes.NewReader([]byte("2018-04-05")))
	var sym yySymType
	require.Equal(t, tokDateTime, l.next(&sym))
	require.Equal(t, sym.expr.String(), "2018-04-05")

	l = newLexer("test", bytes.NewReader([]byte("2018 -04-05")))
	require.Equal(t, tokInt, l.next(&sym))
	require.Equal(t, sym.expr.String(), "2018")
	require.Equal(t, int('-'), l.next(&sym))
	require.Equal(t, tokInt, l.next(&sym))
	require.Equal(t, sym.expr.String(), "4")
	require.Equal(t, int('-'), l.next(&sym))
	require.Equal(t, tokInt, l.next(&sym))
	require.Equal(t, sym.expr.String(), "5")
}

func TestLexDuration(t *testing.T) {
	l := newLexer("test", bytes.NewReader([]byte("3h + 1m3s * 100")))
	var sym yySymType
	require.Equal(t, tokDuration, l.next(&sym))
	require.Equal(t, sym.expr.String(), "3h0m0s")

	require.Equal(t, int('+'), l.next(&sym))
	require.Equal(t, tokDuration, l.next(&sym))
	require.Equal(t, sym.expr.String(), "1m3s")

	require.Equal(t, int('*'), l.next(&sym))
	require.Equal(t, tokInt, l.next(&sym))
	require.Equal(t, sym.expr.String(), "100")
}

func TestRegex(t *testing.T) {
	l := newLexer("test", bytes.NewReader([]byte("/.foo/")))
	var sym yySymType
	require.Equal(t, tokRegex, l.next(&sym))
	require.Equal(t, sym.stringNode.str, ".foo")

	l = newLexer("test", bytes.NewReader([]byte("/foo/")))
	require.Equal(t, tokRegex, l.next(&sym))
	require.Equal(t, sym.stringNode.str, "foo")

	l = newLexer("test", bytes.NewReader([]byte("/ foo")))
	require.Equal(t, int('/'), l.next(&sym))
	require.Equal(t, tokIdent, l.next(&sym))
	require.Equal(t, sym.stringNode.str, "foo")
}

func TestSpaceBetweenOp(t *testing.T) {
	var sym yySymType
	l := newLexer("test", bytes.NewReader([]byte("! ==")))
	require.Equal(t, int('!'), l.next(&sym))
	require.Equal(t, tokEQEQ, l.next(&sym))
}

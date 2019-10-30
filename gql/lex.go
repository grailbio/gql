package gql

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"text/scanner"
	"unicode"
	"unicode/utf8"

	"github.com/grailbio/base/log"
)

// lexer is the state kept during lexical scanning of the source code.
type lexer struct {
	sc     scanner.Scanner
	curPos scanner.Position // position of the last token read by next().

	eof        bool
	opPrefixes map[string][]int
	ops        map[string]int
	opChars    [256]bool
}

func isSpace(tok rune) bool {
	if tok > ' ' {
		return false
	}
	return (scanner.GoWhitespace & (1 << uint32(tok))) != 0
}

func isDatePrefix(str string) bool {
	return len(str) == 4 && strings.HasPrefix(str, "20") || strings.HasPrefix(str, "21") ||
		strings.HasPrefix(str, "19") || strings.HasPrefix(str, "00")
}

func isDurationSuffix(ch rune) bool {
	return ch == 'h' || ch == 's' || ch == 'm' || ch == 'u' || ch == 'µ' || ch == 'n'
}

func isRegexpChar(ch rune) bool {
	return ch == '.' || ch == '*' || ch == '+' || ch == '[' || ch == '{' || ch == '^'
}

func (lex *lexer) numPossibleOpsWithPrefix(prefix string) int {
	return len(lex.opPrefixes[prefix])
}

func (lex *lexer) registerOp(op string, tok int) {
	for _, ch := range op {
		lex.opChars[ch] = true
	}

	lex.ops[op] = tok
	for i := 0; i < len(op); i++ {
		prefix := op[0 : i+1]
		lex.opPrefixes[prefix] = append(lex.opPrefixes[prefix], tok)
	}
}

// pos returns the position of the last token read by next().
func (lex *lexer) pos() string { return lex.curPos.String() }

// next reads a token from the source. It returns the character or token (one of
// tokXXX) read, or zero on EOF.
func (lex *lexer) next(sym *yySymType) int {
	*sym = yySymType{}
	lex.curPos = lex.sc.Pos()
	tok := lex.sc.Scan()
	switch tok {
	case scanner.EOF:
		lex.eof = true
		return 0
	case scanner.Ident:
		str := lex.sc.TokenText()
		switch str {
		case "NA":
			sym.expr = &ASTLiteral{Pos: lex.curPos, Literal: Null}
			return tokNull
		case "func":
			sym.pos = lex.curPos
			return tokFunc
		case "load":
			sym.pos = lex.curPos
			return tokLoad
		case "false":
			sym.expr = &ASTLiteral{Pos: lex.curPos, Literal: False}
			return tokBool
		case "true":
			sym.expr = &ASTLiteral{Pos: lex.curPos, Literal: True}
			return tokBool
		case "cond":
			sym.pos = lex.curPos
			return tokCond
		case "if":
			sym.pos = lex.curPos
			return tokIf
		case "else":
			sym.pos = lex.curPos
			return tokElse
		default:
			sym.stringNode = stringNode{
				pos: lex.curPos,
				str: lex.sc.TokenText(),
			}
		}
		return tokIdent
	case scanner.String, scanner.RawString:
		str := lex.sc.TokenText()
		str = str[1 : len(str)-1]
		sym.expr = &ASTLiteral{Pos: lex.curPos, Literal: NewString(str)}
		return tokString
	case scanner.Int:
		if isDurationSuffix(lex.sc.Peek()) {
			buf := strings.Builder{}
			// Consume all text of form (NNN{h,m,s,ms,us,ns})+
			for tok == scanner.Int && isDurationSuffix(lex.sc.Peek()) {
				buf.WriteString(lex.sc.TokenText())
				_ = lex.sc.Scan()
				buf.WriteString(lex.sc.TokenText())
			}
			sym.expr = &ASTLiteral{Pos: lex.curPos, Literal: ParseDuration(buf.String())}
			return tokDuration
		}
		str := lex.sc.TokenText()
		if isDatePrefix(str) && lex.sc.Peek() == '-' {
			buf := strings.Builder{}
			buf.WriteString(str)
			for {
				ch := lex.sc.Peek()
				if ch == scanner.EOF {
					break
				}
				if !unicode.IsDigit(ch) && ch != '-' && ch != '+' && ch != ':' && ch != 'T' && ch != 'Z' {
					break
				}
				buf.WriteByte(byte(ch))
				lex.sc.Next()
			}
			sym.expr = &ASTLiteral{Pos: lex.curPos, Literal: ParseDateTime(buf.String())}
			return tokDateTime
		}
		var i int64
		n, err := fmt.Sscanf(str, "%v", &i)
		if n != 1 || err != nil {
			log.Panicf("%s: Failed to parse int: %v (n=%d, err=%v)", lex.sc.Pos(), str, n, err)
		}
		sym.expr = &ASTLiteral{Pos: lex.curPos, Literal: NewInt(i)}
		return tokInt
	case scanner.Float:
		str := lex.sc.TokenText()
		var f float64
		n, err := fmt.Sscanf(str, "%v", &f)
		if n != 1 || err != nil {
			log.Panicf("%s: Failed to parse float: %v (n=%d, err=%v)", lex.sc.Pos(), str, n, err)
		}
		sym.expr = &ASTLiteral{Pos: lex.curPos, Literal: NewFloat(f)}
		return tokFloat
	case scanner.Char:
		str := lex.sc.TokenText()
		str = str[1 : len(str)-1] // Remove the single quotes.
		r, n := utf8.DecodeRuneInString(str)
		if r == utf8.RuneError {
			log.Panicf("%s: Failed to parse '%s' as a character", lex.sc.Pos(), str)
		}
		if n != len(str) {
			log.Panicf("%s: Invalid character literal '%s'", lex.sc.Pos(), str)
		}
		sym.expr = &ASTLiteral{Pos: lex.curPos, Literal: NewChar(r)}
		return tokChar
	default:
		if tok <= 0 || tok > 128 {
			log.Panicf("%s: invalid token: %s", lex.sc.Pos(), scanner.TokenString(tok))
		}
		buf := bytes.Buffer{}
		if tok == '/' && (unicode.IsLetter(lex.sc.Peek()) || isRegexpChar(lex.sc.Peek())) {
			// regexp struct field
			//
			// TODO(saito): this code misparses '1024/denominator'. Maybe we should remove
			// the regex feature.  It's not all that useful.
			for {
				ch := lex.sc.Next()
				if ch == '/' {
					break
				}
				if ch == scanner.EOF || isSpace(ch) {
					log.Panicf("%s: nonterminated regexp", lex.sc.Pos())
				}
				buf.WriteByte(uint8(ch))
			}
			sym.stringNode = stringNode{
				pos: lex.curPos,
				str: buf.String(),
			}
			return tokRegex
		}
		buf.WriteByte(uint8(tok))
		if lex.numPossibleOpsWithPrefix(buf.String()) <= 1 {
			s := buf.String()
			if op, ok := lex.ops[s]; ok {
				sym.pos = lex.curPos
				return op
			}
		}
		for {
			ch := lex.sc.Peek()
			if ch <= 0 || ch >= 256 || !lex.opChars[ch] {
				break
			}
			buf.WriteByte(uint8(ch))
			switch lex.numPossibleOpsWithPrefix(buf.String()) {
			case 0:
				buf.Truncate(buf.Len() - 1)
				goto end
			case 1:
				lex.sc.Next()
				s := buf.String()
				if op, ok := lex.ops[s]; ok {
					return op
				}
			default:
				lex.sc.Next()
			}
		}
	end:
		op, ok := lex.ops[buf.String()]
		if !ok {
			log.Panicf("%s: Unknown op: %v", lex.sc.Pos(), buf.String())
		}
		sym.pos = lex.curPos
		return op
	}
}

type lexOpDef struct {
	str string
	tok int
}

var lexOpDefs = []lexOpDef{
	{":=", tokAssign},
	{"&&", tokAndAnd},
	{"|", '|'},
	{"||", tokOrOr},
	{"$", '$'},
	{"&", '&'},
	{"]", ']'},
	{"[", '['},
	{"(", '('},
	{")", ')'},
	{",", ','},
	{"{", '{'},
	{"}", '}'},
	{"==?", tokEQOrRhsNull},
	{"?==", tokEQOrLhsNull},
	{"?==?", tokEQOrBothNull},
	{"==", tokEQEQ},
	{"!", '!'},
	{"!=", tokNE},
	{">=", tokGEQ},
	{"<=", tokLEQ},
	{">", '>'},
	{"<", '<'},
	{"+", '+'},
	{"-", '-'},
	{"*", '*'},
	{"%", '%'},
	{".", '.'},
	{":", ':'},
	{";", ';'},
	{"/", '/'},
}

func newLexer(filename string, in io.Reader) *lexer {
	lex := &lexer{}
	lex.sc.Init(in)
	lex.sc.Mode = scanner.GoTokens
	lex.sc.Position.Filename = filename
	lex.sc.IsIdentRune = func(ch rune, i int) bool {
		if ch == '_' || unicode.IsLetter(ch) || (unicode.IsDigit(ch) && i > 0) {
			return true
		}
		return false
	}
	lex.opPrefixes = map[string][]int{}
	lex.ops = map[string]int{}
	for _, d := range lexOpDefs {
		lex.registerOp(d.str, d.tok)
	}
	return lex
}

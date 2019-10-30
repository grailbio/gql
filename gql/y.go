package gql

import __yyfmt__ "fmt"

import (
	"text/scanner"

	"github.com/grailbio/gql/symbol"
)

type stringNode struct {
	pos scanner.Position
	str string
}

type stringListNode struct {
	pos scanner.Position
	str []string
}

type yySymType struct {
	yys               int
	pos               scanner.Position
	statement         ASTStatement
	statements        []ASTStatement
	statementOrLoad   ASTStatementOrLoad
	statementsOrLoads []ASTStatementOrLoad
	expr              ASTNode
	// elseifExpr ASTNode
	stringNode     stringNode
	stringListNode stringListNode
	structField    ASTStructLiteralField
	structFields   []ASTStructLiteralField
	paramVals      []ASTParamVal
}

const tokIdent = 57346
const tokRegex = 57347
const tokInt = 57348
const tokString = 57349
const tokBool = 57350
const tokFloat = 57351
const tokChar = 57352
const tokDateTime = 57353
const tokDuration = 57354
const tokNull = 57355
const tokOrOr = 57356
const tokAndAnd = 57357
const tokAssign = 57358
const tokEQEQ = 57359
const tokEQOrRhsNull = 57360
const tokEQOrLhsNull = 57361
const tokEQOrBothNull = 57362
const tokNE = 57363
const tokLEQ = 57364
const tokGEQ = 57365
const tokFunc = 57366
const tokLoad = 57367
const tokCond = 57368
const tokIf = 57369
const tokElse = 57370
const unary = 57371
const deref = 57372

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"tokIdent",
	"tokRegex",
	"tokInt",
	"tokString",
	"tokBool",
	"tokFloat",
	"tokChar",
	"tokDateTime",
	"tokDuration",
	"tokNull",
	"tokOrOr",
	"tokAndAnd",
	"tokAssign",
	"tokEQEQ",
	"tokEQOrRhsNull",
	"tokEQOrLhsNull",
	"tokEQOrBothNull",
	"tokNE",
	"tokLEQ",
	"tokGEQ",
	"'>'",
	"'<'",
	"'|'",
	"'{'",
	"'$'",
	"'&'",
	"tokFunc",
	"tokLoad",
	"tokCond",
	"tokIf",
	"tokElse",
	"'+'",
	"'-'",
	"'^'",
	"'*'",
	"'/'",
	"'%'",
	"unary",
	"'.'",
	"'['",
	"']'",
	"'('",
	"')'",
	"'}'",
	"deref",
	"';'",
	"'!'",
	"','",
	"':'",
}
var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 131,
	51, 77,
	-2, 57,
	-1, 132,
	34, 41,
	38, 41,
	39, 41,
	40, 41,
	-2, 28,
}

const yyPrivate = 57344

const yyLast = 745

var yyAct = [...]int{

	9, 84, 117, 30, 70, 7, 17, 32, 69, 125,
	159, 141, 106, 57, 60, 107, 107, 64, 122, 124,
	56, 114, 61, 107, 33, 115, 113, 31, 164, 74,
	76, 158, 55, 123, 71, 37, 138, 107, 85, 87,
	88, 89, 90, 91, 92, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 117, 105, 116, 81,
	35, 36, 80, 63, 108, 112, 5, 39, 40, 144,
	46, 47, 48, 49, 50, 54, 52, 51, 53, 38,
	156, 120, 126, 43, 44, 45, 56, 55, 41, 42,
	37, 43, 44, 45, 34, 55, 3, 62, 37, 36,
	79, 36, 147, 121, 157, 1, 128, 127, 104, 140,
	130, 87, 132, 104, 134, 4, 74, 66, 139, 71,
	136, 65, 135, 83, 82, 146, 145, 148, 77, 143,
	149, 68, 11, 67, 150, 142, 41, 42, 151, 43,
	44, 45, 154, 55, 2, 155, 37, 78, 0, 0,
	71, 0, 0, 0, 0, 0, 0, 161, 162, 160,
	163, 153, 59, 0, 18, 21, 24, 19, 25, 22,
	23, 20, 39, 40, 0, 46, 47, 48, 49, 50,
	54, 52, 51, 53, 110, 28, 26, 27, 58, 0,
	15, 16, 0, 41, 111, 0, 43, 44, 45, 0,
	55, 0, 0, 109, 0, 0, 39, 40, 13, 46,
	47, 48, 49, 50, 54, 52, 51, 53, 38, 0,
	0, 0, 0, 0, 0, 0, 0, 41, 42, 0,
	43, 44, 45, 0, 55, 0, 0, 37, 0, 0,
	0, 0, 0, 129, 137, 75, 18, 21, 24, 19,
	25, 22, 23, 20, 0, 10, 0, 18, 21, 24,
	19, 25, 22, 23, 20, 0, 14, 28, 26, 27,
	58, 0, 15, 16, 0, 0, 12, 14, 28, 26,
	27, 8, 6, 15, 16, 29, 0, 12, 0, 0,
	13, 0, 0, 0, 0, 0, 29, 0, 0, 0,
	0, 13, 73, 75, 18, 21, 24, 19, 25, 22,
	23, 20, 0, 59, 0, 18, 21, 24, 19, 25,
	22, 23, 20, 0, 14, 28, 26, 27, 72, 0,
	15, 16, 0, 0, 12, 14, 28, 26, 27, 58,
	0, 15, 16, 29, 0, 12, 0, 0, 13, 0,
	0, 0, 0, 0, 29, 0, 0, 39, 40, 13,
	46, 47, 48, 49, 50, 54, 52, 51, 53, 38,
	0, 0, 0, 0, 0, 0, 0, 0, 41, 42,
	0, 43, 44, 45, 0, 55, 0, 0, 37, 0,
	0, 10, 152, 18, 21, 24, 19, 25, 22, 23,
	20, 0, 86, 0, 18, 21, 24, 19, 25, 22,
	23, 20, 0, 14, 28, 26, 27, 72, 0, 15,
	16, 0, 0, 12, 14, 28, 26, 27, 58, 0,
	15, 16, 29, 0, 12, 0, 0, 13, 0, 0,
	0, 0, 0, 29, 0, 0, 0, 131, 13, 18,
	21, 24, 19, 25, 22, 23, 20, 0, 10, 0,
	18, 21, 24, 19, 25, 22, 23, 20, 0, 14,
	28, 26, 27, 58, 0, 15, 16, 0, 0, 12,
	14, 28, 26, 27, 8, 0, 15, 16, 29, 0,
	12, 0, 0, 13, 0, 0, 0, 0, 0, 29,
	0, 0, 39, 40, 13, 46, 47, 48, 49, 50,
	54, 52, 51, 53, 38, 0, 0, 0, 0, 0,
	0, 0, 0, 41, 42, 0, 43, 44, 45, 0,
	55, 0, 0, 37, 165, 39, 40, 0, 46, 47,
	48, 49, 50, 54, 52, 51, 53, 38, 0, 0,
	0, 0, 0, 0, 0, 0, 41, 42, 0, 43,
	44, 45, 0, 55, 0, 0, 37, 119, 39, 40,
	0, 46, 47, 48, 49, 50, 54, 52, 51, 53,
	38, 0, 0, 0, 0, 0, 0, 0, 133, 41,
	42, 0, 43, 44, 45, 0, 55, 39, 40, 37,
	46, 47, 48, 49, 50, 54, 52, 51, 53, 38,
	0, 0, 0, 0, 0, 0, 0, 0, 41, 42,
	0, 43, 44, 45, 0, 55, 39, 40, 37, 46,
	47, 48, 49, 50, 54, 52, 51, 53, 38, 0,
	0, 0, 0, 0, 0, 0, 0, 41, 42, 0,
	43, 44, 45, 0, 118, 0, 40, 37, 46, 47,
	48, 49, 50, 54, 52, 51, 53, 38, 0, 0,
	0, 0, 0, 0, 0, 0, 41, 42, 0, 43,
	44, 45, 0, 55, 0, 0, 37, 46, 47, 48,
	49, 50, 54, 52, 51, 53, 38, 0, 0, 0,
	0, 0, 0, 0, 0, 41, 42, 0, 43, 44,
	45, 0, 55, 0, 0, 37, 46, 47, 48, 49,
	50, 54, 52, 51, 53, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 41, 42, 0, 43, 44, 45,
	0, 55, 0, 0, 37,
}
var yyPact = [...]int{

	251, -1000, -22, -25, -1000, -1000, 87, -1000, 56, 583,
	70, -1000, 309, 309, 93, 18, 309, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 117, 113, 298, 309,
	-1000, 251, -1000, 454, -1000, 17, 93, 398, 309, 309,
	309, 309, 309, 309, 309, 309, 309, 309, 309, 309,
	309, 309, 309, 309, 309, 109, 309, -10, 16, -1000,
	-10, -14, -1000, 309, 158, -1000, -1000, -23, -26, -1000,
	-1000, -1000, 54, 4, 612, -1000, 521, -25, -1000, -1000,
	93, -28, -13, -32, -42, 583, 66, 699, 641, 670,
	45, 45, -10, -10, -10, 101, 101, 101, 101, 101,
	101, 101, 101, 101, -1000, 583, 309, 102, 192, 398,
	443, 309, 554, 387, -1000, 240, -9, 309, 104, -1000,
	-1000, -35, 42, -1000, 398, 98, 309, 699, -1000, 309,
	521, -1000, -10, 309, 343, -1000, -1000, -50, 93, 583,
	-1000, 309, -1000, -1000, 387, -42, 583, 64, 583, 53,
	583, -16, -1000, -36, 583, 343, 309, 309, -1000, 309,
	-19, 583, 488, 583, -1000, -1000,
}
var yyPgo = [...]int{

	0, 115, 144, 135, 5, 66, 8, 96, 133, 0,
	132, 6, 4, 131, 22, 124, 123, 1, 105, 3,
}
var yyR1 = [...]int{

	0, 18, 18, 18, 7, 7, 5, 5, 5, 19,
	19, 2, 2, 1, 4, 11, 8, 8, 6, 6,
	3, 3, 9, 9, 9, 9, 9, 9, 9, 9,
	9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
	9, 9, 9, 9, 9, 9, 9, 9, 9, 10,
	10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
	10, 10, 15, 15, 15, 15, 16, 16, 17, 17,
	12, 12, 12, 12, 13, 13, 14, 14, 14,
}
var yyR2 = [...]int{

	0, 2, 4, 2, 1, 3, 1, 6, 1, 0,
	1, 1, 3, 2, 3, 6, 1, 3, 1, 6,
	1, 4, 1, 4, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 2, 2, 3, 5, 4, 8, 5, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 2, 2,
	3, 3, 0, 1, 3, 1, 1, 3, 3, 5,
	3, 1, 3, 1, 1, 3, 0, 1, 3,
}
var yyChk = [...]int{

	-1000, -18, -2, -7, -1, -5, 31, -4, 30, -9,
	4, -10, 36, 50, 26, 32, 33, -11, 6, 9,
	13, 7, 11, 12, 8, 10, 28, 29, 27, 45,
	-19, 49, -19, 49, 7, 4, 45, 45, 26, 14,
	15, 35, 36, 38, 39, 40, 17, 18, 19, 20,
	21, 24, 23, 25, 22, 42, 16, -9, 30, 4,
	-9, -14, 4, 45, -9, 4, 4, -8, -13, -6,
	-12, -4, 30, 4, -9, 5, -9, -7, -1, -5,
	45, -14, -15, -16, -17, -9, 4, -9, -9, -9,
	-9, -9, -9, -9, -9, -9, -9, -9, -9, -9,
	-9, -9, -9, -9, 4, -9, 26, 51, -9, 45,
	26, 36, -9, 49, 47, 51, 4, 52, 42, 46,
	-19, -14, 46, 46, 51, 51, 16, -9, 4, 51,
	-9, 4, -9, 34, -9, -6, -12, 4, 45, -9,
	5, 46, -3, -11, 27, -17, -9, 4, -9, -9,
	-9, -19, 49, -14, -9, -9, 16, 51, 47, 46,
	-19, -9, -9, -9, 47, 46,
}
var yyDef = [...]int{

	0, -2, 9, 9, 11, 4, 0, 6, 0, 8,
	57, 22, 0, 0, 76, 0, 0, 48, 49, 50,
	51, 52, 53, 54, 55, 56, 0, 0, 0, 0,
	1, 10, 3, 10, 13, 0, 76, 62, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 41, 0, 57,
	42, 0, 77, 0, 0, 58, 59, 0, 0, 16,
	74, 18, 0, 57, 71, 73, 0, 9, 12, 5,
	76, 0, 0, 63, 65, 66, 57, 24, 25, 26,
	27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
	37, 38, 39, 40, 43, 14, 0, 0, 0, 62,
	76, 0, 0, 0, 60, 0, 0, 0, 0, 61,
	2, 0, 0, 23, 0, 0, 0, 45, 78, 0,
	66, -2, -2, 0, 9, 17, 75, 57, 76, 70,
	72, 0, 44, 20, 0, 64, 67, 0, 68, 0,
	47, 0, 10, 0, 7, 9, 0, 0, 15, 0,
	0, 69, 0, 19, 21, 46,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 50, 3, 3, 28, 40, 29, 3,
	45, 46, 38, 35, 51, 36, 42, 39, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 52, 49,
	25, 3, 24, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 43, 3, 44, 37, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 27, 26, 47,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 30, 31, 32, 33, 34, 41, 48,
}
var yyTok3 = [...]int{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := yyPact[state]
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && yyChk[yyAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || yyExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := yyExca[i]
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		token = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = yyTok3[i+0]
		if token == char {
			token = yyTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yyrcvr.char = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yylex.(*parserState).statements = yyDollar[1].statementsOrLoads
		}
	case 2:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			sptr := &yylex.(*parserState).statements
			*sptr = yyDollar[1].statementsOrLoads
			for _, s := range yyDollar[3].statements {
				(*sptr) = append((*sptr), ASTStatementOrLoad{ASTStatement: s})
			}
		}
	case 3:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			sptr := &yylex.(*parserState).statements
			(*sptr) = nil
			for _, s := range yyDollar[1].statements {
				(*sptr) = append((*sptr), ASTStatementOrLoad{ASTStatement: s})
			}
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.statements = []ASTStatement{yyDollar[1].statement}
		}
	case 5:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.statements = append(yyDollar[1].statements, yyDollar[3].statement)
		}
	case 7:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.statement = NewASTStatement(yyDollar[1].pos, yyDollar[2].stringNode.str, NewASTLambda(yyDollar[1].pos, yyDollar[4].stringListNode.str, yyDollar[6].expr))
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.statement = ASTStatement{Expr: yyDollar[1].expr}
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.statementsOrLoads = []ASTStatementOrLoad{yyDollar[1].statementOrLoad}
		}
	case 12:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.statementsOrLoads = append(yyDollar[1].statementsOrLoads, yyDollar[3].statementOrLoad)
		}
	case 13:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.statementOrLoad = ASTStatementOrLoad{LoadPath: yyDollar[2].expr.(*ASTLiteral).Literal.Str(nil)}
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.statement = ASTStatement{Pos: yyDollar[1].stringNode.pos, LHS: symbol.Intern(yyDollar[1].stringNode.str), Expr: yyDollar[3].expr}
		}
	case 15:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.expr = &ASTBlock{Pos: yyDollar[1].pos, Statements: append(yyDollar[2].statements, ASTStatement{Pos: yyDollar[4].expr.pos(), Expr: yyDollar[4].expr})}
		}
	case 16:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.statements = []ASTStatement{yyDollar[1].statement}
		}
	case 17:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.statements = append(yyDollar[1].statements, yyDollar[3].statement)
		}
	case 19:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.statement = NewASTStatement(yyDollar[1].pos, yyDollar[2].stringNode.str, NewASTLambda(yyDollar[1].pos, yyDollar[4].stringListNode.str, yyDollar[6].expr))
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.statements = []ASTStatement{{Pos: yyDollar[1].expr.pos(), Expr: yyDollar[1].expr}}
		}
	case 21:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.statements = []ASTStatement{{Pos: yyDollar[1].pos, Expr: yyDollar[2].expr}}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.expr = NewASTFuncall(yyDollar[1].expr, yyDollar[3].paramVals)
		}
	case 24:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTPipe(yyDollar[1].expr, yyDollar[3].expr)
		}
	case 25:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &ASTLogicalOp{AndAnd: false, LHS: yyDollar[1].expr, RHS: yyDollar[3].expr}
		}
	case 26:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &ASTLogicalOp{AndAnd: true, LHS: yyDollar[1].expr, RHS: yyDollar[3].expr}
		}
	case 27:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinPlusValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 28:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinMinusValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 29:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinMultiplyValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinDivideValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinModValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinEQValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinEQOrRhsNullValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinEQOrLhsNullValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 35:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinEQOrBothNullValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 36:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinNEValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 37:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinGTValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 38:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinGEValue, yyDollar[1].expr, yyDollar[3].expr)
		}
	case 39:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinGTValue, yyDollar[3].expr, yyDollar[1].expr)
		}
	case 40:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[1].expr.pos(), builtinGEValue, yyDollar[3].expr, yyDollar[1].expr)
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[2].expr.pos(), builtinNegateValue, yyDollar[2].expr)
		}
	case 42:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.expr = NewASTBuiltinFuncall(yyDollar[2].expr.pos(), builtinNotValue, yyDollar[2].expr)
		}
	case 43:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTStructFieldRef(yyDollar[1].expr, yyDollar[3].stringNode.str)
		}
	case 44:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.expr = NewASTLambda(yyDollar[1].pos, yyDollar[3].stringListNode.str, &ASTBlock{Pos: yyDollar[1].pos, Statements: yyDollar[5].statements})
		}
	case 45:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.expr = NewASTLambda(yyDollar[1].pos, yyDollar[2].stringListNode.str, yyDollar[4].expr)
		}
	case 46:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.expr = &ASTCondOp{Pos: yyDollar[1].pos, Cond: yyDollar[3].expr, Then: yyDollar[5].expr, Else: yyDollar[7].expr}
		}
	case 47:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.expr = &ASTCondOp{Pos: yyDollar[1].pos, Cond: yyDollar[2].expr, Then: yyDollar[3].expr, Else: yyDollar[5].expr}
		}
	case 57:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = &ASTVarRef{Pos: yyDollar[1].stringNode.pos, Var: symbol.Intern(yyDollar[1].stringNode.str)}
		}
	case 58:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.expr = &ASTColumnRef{Pos: yyDollar[1].pos, Col: symbol.Intern(yyDollar[2].stringNode.str), Deprecated: true}
		}
	case 59:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.expr = &ASTImplicitColumnRef{Pos: yyDollar[1].pos, Col: symbol.Intern(yyDollar[2].stringNode.str)}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = NewASTStructLiteral(yyDollar[1].pos, yyDollar[2].structFields)
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 62:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.paramVals = nil
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.paramVals = append(yyDollar[1].paramVals, yyDollar[3].paramVals...)
		}
	case 66:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.paramVals = []ASTParamVal{NewASTParamVal(yyDollar[1].expr.pos(), "", yyDollar[1].expr)}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.paramVals = append(yyDollar[1].paramVals, NewASTParamVal(yyDollar[3].expr.pos(), "", yyDollar[3].expr))
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.paramVals = []ASTParamVal{NewASTParamVal(yyDollar[1].stringNode.pos, yyDollar[1].stringNode.str, yyDollar[3].expr)}
		}
	case 69:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.paramVals = append(yyDollar[1].paramVals, NewASTParamVal(yyDollar[3].stringNode.pos, yyDollar[3].stringNode.str, yyDollar[5].expr))
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.structField = NewASTStructLiteralField(yyDollar[1].stringNode.pos, yyDollar[1].stringNode.str, yyDollar[3].expr)
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.structField = NewASTStructLiteralField(yyDollar[1].expr.pos(), "", yyDollar[1].expr)
		}
	case 72:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.structField = NewASTStructLiteralField(yyDollar[1].expr.pos(), "", NewASTStructFieldRegex(yyDollar[1].expr.pos(), yyDollar[1].expr, yyDollar[3].stringNode.str))
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.structField = NewASTStructLiteralField(yyDollar[1].stringNode.pos, "", NewASTStructFieldRegex(yyDollar[1].stringNode.pos, nil, yyDollar[1].stringNode.str))
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.structFields = []ASTStructLiteralField{yyDollar[1].structField}
		}
	case 75:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.structFields = append(yyDollar[1].structFields, yyDollar[3].structField)
		}
	case 76:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.stringListNode = stringListNode{}
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.stringListNode = stringListNode{pos: yyDollar[1].stringNode.pos, str: []string{yyDollar[1].stringNode.str}}
		}
	case 78:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stringListNode.str = append(yyDollar[1].stringListNode.str, yyDollar[3].stringNode.str)
		}
	}
	goto yystack /* stack new state and value */
}

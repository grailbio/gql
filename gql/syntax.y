%{
package gql

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

%}

%union {
  pos scanner.Position
  statement ASTStatement
  statements []ASTStatement
  statementOrLoad ASTStatementOrLoad
  statementsOrLoads []ASTStatementOrLoad
  expr ASTNode
  // elseifExpr ASTNode
  stringNode stringNode
  stringListNode stringListNode
  structField ASTStructLiteralField
  structFields []ASTStructLiteralField
  paramVals []ASTParamVal
}

%token <stringNode> tokIdent tokRegex
%token <expr> tokInt tokString tokBool tokFloat tokChar tokDateTime tokDuration tokNull
%token <expr> tokOrOr tokAndAnd tokAssign
%token <expr> tokEQEQ tokEQOrRhsNull tokEQOrLhsNull tokEQOrBothNull
%token <expr> tokNE tokLEQ tokGEQ '>' '<'
%token <pos> '|' '{' '$' '&' tokFunc tokLoad tokCond tokIf tokElse
%type <statementOrLoad> loadStatement
%type <statementsOrLoads> loadStatements
%type <statements> legacyFunctionBlock
%type <statement> assignment toplevelStatement blockStatement
%type <statements> toplevelStatements blockStatements
%type <expr> expr term block
%type <structField> structField
%type <structFields> structFields
%type <stringListNode> paramNameList
%type <paramVals> paramList positionalParamList namedParamList

%left tokOrOr
%left tokAndAnd
%left '|'
%left '<' '>' tokLEQ tokGEQ tokNE tokEQEQ tokEQOrBothNull tokEQOrRhsNull tokEQOrLhsNull
%left '+' '-' '^'
%left '*' '/' '%'
%left unary
%right '.' '[' ']' '(' ')' '{' '}'
%left deref

%%

start: loadStatements optionalSemicolon { yylex.(*parserState).statements = $1 }
| loadStatements ';' toplevelStatements optionalSemicolon {
	sptr := &yylex.(*parserState).statements
        *sptr = $1
	for _, s := range $3 {
		(*sptr) = append((*sptr), ASTStatementOrLoad{ASTStatement:s})
	}}
| toplevelStatements optionalSemicolon {
	sptr := &yylex.(*parserState).statements
        (*sptr) = nil
	for _, s := range $1 {
		(*sptr) = append((*sptr), ASTStatementOrLoad{ASTStatement:s})
	}}

toplevelStatements: toplevelStatement { $$ = []ASTStatement{$1} }
| toplevelStatements ';' toplevelStatement { $$ = append($1, $3) }

toplevelStatement: assignment
| tokFunc tokIdent '(' paramNameList ')' expr {$$ = NewASTStatement($1, $2.str, NewASTLambda($1, $4.str, $6))}
| expr { $$ = ASTStatement{Expr: $1} }


optionalSemicolon: /*empty*/
| ';'

loadStatements: loadStatement { $$ = []ASTStatementOrLoad{$1} }
| loadStatements ';' loadStatement { $$ = append($1, $3) }

loadStatement: tokLoad tokString { $$ = ASTStatementOrLoad{LoadPath: $2.(*ASTLiteral).Literal.Str(nil)} }

assignment: tokIdent tokAssign expr { $$ = ASTStatement{Pos:$1.pos, LHS: symbol.Intern($1.str), Expr:$3} }

// '{ ... }'. A block must start with an assignment statement to distinguish between a block and a struct literal.
block: '{' blockStatements ';' expr optionalSemicolon '}' {$$ = &ASTBlock{Pos: $1, Statements: append($2, ASTStatement{Pos: $4.pos(), Expr: $4})}}

blockStatements: blockStatement { $$ = []ASTStatement{$1} }
| blockStatements ';' blockStatement { $$ = append($1, $3) }

blockStatement:  assignment
| tokFunc tokIdent '(' paramNameList ')' expr {$$ = NewASTStatement($1, $2.str, NewASTLambda($1, $4.str, $6))}

// Legacy lambda of form "func(args) { expr... }". The "expr..." part doesn't
// need to start with an assignment, to keep backward compatibility.
legacyFunctionBlock: block { $$ = []ASTStatement{{Pos:$1.pos(), Expr: $1}} }
| '{' expr optionalSemicolon '}' { $$ = []ASTStatement{{Pos: $1, Expr: $2}} }

expr: term
| expr '(' paramList ')' { $$ = NewASTFuncall($1, $3) }
| expr '|' expr { $$ = NewASTPipe($1, $3) }
| expr tokOrOr expr { $$ = &ASTLogicalOp{AndAnd:false, LHS: $1, RHS: $3} }
| expr tokAndAnd expr { $$ = &ASTLogicalOp{AndAnd:true, LHS: $1, RHS: $3} }
| expr '+' expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinPlusValue, $1, $3) }
| expr '-' expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinMinusValue, $1, $3) }
| expr '*' expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinMultiplyValue, $1, $3) }
| expr '/' expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinDivideValue, $1, $3) }
| expr '%' expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinModValue, $1, $3) }
| expr tokEQEQ expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinEQValue, $1, $3) }
| expr tokEQOrRhsNull expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinEQOrRhsNullValue, $1, $3) }
| expr tokEQOrLhsNull expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinEQOrLhsNullValue, $1, $3) }
| expr tokEQOrBothNull expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinEQOrBothNullValue, $1, $3) }
| expr tokNE expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinNEValue, $1, $3) }
| expr '>' expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinGTValue, $1, $3) }
| expr tokGEQ expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinGEValue, $1, $3) }
| expr '<' expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinGTValue, $3, $1) }
| expr tokLEQ expr { $$ = NewASTBuiltinFuncall($1.pos(), builtinGEValue, $3, $1) }
| '-' expr %prec unary { $$ = NewASTBuiltinFuncall($2.pos(), builtinNegateValue, $2) }
| '!' expr %prec unary { $$ = NewASTBuiltinFuncall($2.pos(), builtinNotValue, $2) }
| expr '.' tokIdent %prec deref { $$ = NewASTStructFieldRef($1, $3.str) }
| tokFunc '(' paramNameList ')' legacyFunctionBlock {$$ = NewASTLambda($1, $3.str, &ASTBlock{Pos: $1, Statements:$5})}
| '|' paramNameList '|' expr {$$ = NewASTLambda($1, $2.str, $4) }
| tokCond '(' expr ',' expr ',' expr ')' { $$ = &ASTCondOp{Pos:$1, Cond:$3, Then:$5, Else:$7} }
| tokIf expr expr tokElse expr {$$ = &ASTCondOp{Pos:$1, Cond:$2, Then:$3, Else:$5}}
| block

// TODO(saito) the above if-then rule causes many shift/reduce conflicts. Add
// precedence defs.

term: tokInt
| tokFloat
| tokNull
| tokString
| tokDateTime
| tokDuration
| tokBool
| tokChar
| tokIdent {$$ = &ASTVarRef{Pos:$1.pos, Var: symbol.Intern($1.str)}}
| '$' tokIdent {$$ = &ASTColumnRef{Pos:$1, Col: symbol.Intern($2.str), Deprecated: true}}
| '&' tokIdent {$$ = &ASTImplicitColumnRef{Pos:$1, Col: symbol.Intern($2.str)}}
| '{' structFields '}' { $$ = NewASTStructLiteral($1, $2) }
| '(' expr ')' { $$ = $2 }

paramList: /*empty*/ { $$ = nil }
| positionalParamList
| positionalParamList ',' namedParamList { $$ = append($1, $3...) }
| namedParamList

positionalParamList: expr { $$ = []ASTParamVal{NewASTParamVal($1.pos(), "", $1)}}
| positionalParamList ',' expr { $$ = append($1, NewASTParamVal($3.pos(), "", $3)) }

namedParamList: tokIdent tokAssign expr { $$ = []ASTParamVal{NewASTParamVal($1.pos, $1.str, $3)} }
| namedParamList ',' tokIdent tokAssign expr { $$ = append($1, NewASTParamVal($3.pos, $3.str, $5)) }

structField: tokIdent ':' expr { $$ = NewASTStructLiteralField($1.pos, $1.str, $3) }
| expr {$$ = NewASTStructLiteralField($1.pos(), "", $1) }
| expr '.' tokRegex %prec deref { $$ = NewASTStructLiteralField($1.pos(), "", NewASTStructFieldRegex($1.pos(), $1, $3.str)) }
| tokRegex %prec deref { $$ = NewASTStructLiteralField($1.pos, "", NewASTStructFieldRegex($1.pos, nil, $1.str)) }

structFields: structField { $$ = []ASTStructLiteralField{$1} }
| structFields ',' structField { $$ = append($1, $3) }


paramNameList: /*empty*/ { $$ = stringListNode{} }
| tokIdent { $$ = stringListNode{pos:$1.pos, str:[]string{$1.str} } }
| paramNameList ',' tokIdent { $$.str = append($1.str, $3.str) }

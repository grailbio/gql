package gql

import (
	"bytes"
	"context"
	"io"
	_ "net/http/pprof" // Pprof is included to be exposed on the local diagnostic web server.
	"os"
	"regexp"
	"sync"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/gql/symbol"
	"github.com/pkg/errors"
)

// Default values for cacheRoot when using EC2 bigslice system and otherwise.
const (
	DefaultCacheRoot      = "s3://grail-query/cache4"
	DefaultLocalCacheRoot = "/tmp/grail-query/cache4"
)

var (
	// Variables in this block are copied from Opts in Init.

	// BackgroundContext is used in stringers, etc, when the user doesn't
	// explicitly pass a context. Copied from Opts.BackgroundContext in Init.
	BackgroundContext context.Context
	cacheRoot         string
	// Bigslice session to be used for distributed operations.
	bsSession *exec.Session
	// overwriteFiles controls whether write() overwrites existing files.
	overwriteFiles bool
	// Path RE of files assumed to be immutable. Immutable files are hashed
	// quickly by just using their pathnames.
	immutableFilesRE []*regexp.Regexp
)

// TestSetOverwriteFiles temporarily overrides overwriteFiles.  Return the old
// value. For unittests only.
func TestSetOverwriteFiles(v bool) bool {
	old := overwriteFiles
	overwriteFiles = v
	return old
}

// Opts is passed to gql.Init
type Opts struct {
	// BackgroundContext is the default context used in stringers and other
	// non-critical codepaths.
	// If unset, context.Background() is used.
	BackgroundContext context.Context
	// CacheDir is directory that stores materialized tables for caching.  If
	// unset, gql.DefaultCacheRoot is used when bsSession != nil. Else
	// gql.DefaultLocalCacheRoot is used.
	CacheDir string
	// OverwriteFiles controls whether write() function overwrites existing files.
	OverwriteFiles bool
	// BigsliceSession is an initialized bigslice session. If unset, a local
	// bigslice executor will be created.
	BigsliceSession *exec.Session
	// ImmutableFilesRE x lists regexps of paths of files that can be assumed to
	// be immutable.  Immutable files can be hashed quickly using just their
	// pathnames, so they improve performance of gql.
	//
	// If nil, "^s3://grail-clinical.*" and "^s3://grail-results.*" are used.
	ImmutableFilesRE []*regexp.Regexp
}

var initMu sync.Mutex

// Session represents a GQL session. It contains global variable bindings.
type Session struct {
	mu sync.Mutex

	// Runtime bindings. env is guarded by mu, but *env is immutable, so it can be
	// read outside mu. When adding a new global variable, one must lock mu,
	// create a new Bindings object w/ the new variable, then replace env.
	env *bindings

	// Variable type info.
	//
	// INVARIANT: env and aiEnv store the same set of variables/consts, with
	// matching types.
	aiEnv aiBindings

	// Stores inferred types of AST nodes.
	types *astTypes
}

// Bindings returrs the bindings for the global symbols.
func (s *Session) Bindings() *bindings {
	s.mu.Lock()
	b := s.env
	s.mu.Unlock()
	return b
}

// For unittests only.
func (s *Session) aiBindings() aiBindings { return s.aiEnv }

// EvalFile reads a script and evaluates it. Returns the value computed by the
// last expression.
func (s *Session) EvalFile(ctx context.Context, path string) Value {
	text, err := file.ReadFile(ctx, path)
	if err != nil {
		log.Panicf("open %v: %v", path, err)
	}
	statements, err := s.Parse(path, text)
	if err != nil {
		log.Panicf("parse %s: %v", path, err)
	}
	return s.EvalStatements(ctx, statements)
}

// parserState is used to exchange state between the yacc-generated code and Session.Parse.
type parserState struct {
	lex        *lexer
	statements []ASTStatementOrLoad
	err        error
}

// Lex implements the yacc's yyLexer interface.
func (p *parserState) Lex(lval *yySymType) int {
	return p.lex.next(lval)
}

// Error implements the yacc's yyLexer interface.
func (p *parserState) Error(s string) {
	if p.err == nil {
		p.err = errors.New(p.lex.pos() + ":" + s)
	}
}

// Parse parses the given text. filename is embedded in an error messages.  It
// returns io.EOF if the text is incomplete. If the text is unparsable for other
// reasons, it returns non-nil errors other than io.EOF.
func (s *Session) Parse(filename string, text []byte) ([]ASTStatementOrLoad, error) {
	p := parserState{lex: newLexer(filename, bytes.NewReader(text))}
	yyParse(&p)
	if p.err != nil {
		if p.lex.eof {
			return nil, io.EOF
		}
		return nil, p.err
	}
	return p.statements, nil
}

// SetGlobal sets the global variable value.
//
// REQUIRES: the variable must not exist already, otherwise the process crashes.
func (s *Session) SetGlobal(name string, value Value) {
	s.SetGlobals(map[string]Value{name: value})
}

// SetGlobals sets a set of global variables.
//
// REQUIRES: each variable must not exist already, otherwise the process crashes.
func (s *Session) SetGlobals(vars map[string]Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	newEnv := s.env.clone()
	for name, value := range vars {
		if value.Type() == FuncType {
			log.Panicf("setglobal %v: function cannot be set", name)
		}
		sym := symbol.Intern(name)
		newEnv.setGlobal(sym, value)
		valueCopy := value
		s.aiEnv.setGlobal(sym, AIType{Type: value.Type(), Literal: &valueCopy})
	}
	s.env = newEnv
}

// EvalStatements evaluates the statement and returns the value of the expression
// within. If st is of form "var := expr", binds var to the result of the
// expression so that subsequent Eval calls can refer to the variable.
func (s *Session) EvalStatements(ctx context.Context, statements []ASTStatementOrLoad) Value {
	var loads, others []ASTStatementOrLoad
	for _, st := range statements {
		if st.LoadPath != "" {
			loads = append(loads, st)
			// The load statements must precede any other statement; see the grammar
			// definition.
			if len(others) > 0 {
				panic(statements)
			}
		} else {
			others = append(others, st)
		}
	}

	// Process loads first.
	var val Value
	for _, st := range loads {
		data, err := file.ReadFile(ctx, st.LoadPath)
		if err != nil {
			log.Panicf("load %s: %v", st.LoadPath, err)
		}
		subStatements, err := s.Parse(st.LoadPath, data)
		if err != nil {
			log.Panicf("load %s: %v", st.LoadPath, err)
		}
		if len(statements) == 0 {
			log.Panicf("load %s: empty file", st.LoadPath)
		}
		val = s.EvalStatements(ctx, subStatements)
	}

	analyze := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, st := range others {
			s.types.add(st.Expr, &s.aiEnv)
			transformAST(s.types, &st.Expr)
			if st.LHS != symbol.Invalid {
				s.aiEnv.setGlobal(st.LHS, s.types.getType(st.Expr))
			}
		}
	}

	setGlobal := func(sym symbol.ID, val Value) {
		s.mu.Lock()
		defer s.mu.Unlock()
		newEnv := s.env.clone()
		newEnv.setGlobal(sym, val)
		s.env = newEnv
	}

	analyze()
	for _, st := range others {
		val = st.Expr.eval(ctx, s.Bindings())
		if st.LHS != symbol.Invalid {
			setGlobal(st.LHS, val)
		}
	}
	return val
}

// Eval evaluates an expression.
func (s *Session) Eval(ctx context.Context, expr ASTNode) Value {
	return expr.eval(ctx, s.Bindings())
}

// NewSession creates a new empty session.
func NewSession() *Session {
	if BackgroundContext == nil {
		log.Panic("gql.Init not yet called")
	}
	s := &Session{
		env: &bindings{
			frames: []*callFrame{
				globalConsts,
				&callFrame{},
			}},
		aiEnv: aiBindings{
			Frames: []aiFrame{
				aiGlobalConsts,
				aiFrame{},
			}},
		types: newASTTypes(),
	}
	return s
}

// Init initializes the GQL runtime. It must be called once before using
// evaluating gql expressions.
func Init(opts Opts) {
	initMu.Lock()
	if BackgroundContext != nil {
		log.Error.Printf("gql.Init called twice") // This happens in unittests.
		initMu.Unlock()
		return
	}
	BackgroundContext = opts.BackgroundContext
	if BackgroundContext == nil {
		BackgroundContext = context.Background()
	}
	initMu.Unlock()

	overwriteFiles = opts.OverwriteFiles
	immutableFilesRE = opts.ImmutableFilesRE
	if immutableFilesRE == nil {
		immutableFilesRE = []*regexp.Regexp{
			regexp.MustCompile("^s3://grail-clinical.*"),
			regexp.MustCompile("^s3://grail-results.*"),
			regexp.MustCompile("^s3://grail-tidy-datasets.*"),
		}
	}
	symbol.MarkPreInternedSymbols()
	bsSession = opts.BigsliceSession
	cacheRoot = opts.CacheDir

	if bsSession == nil {
		bsSession = exec.Start(exec.Local, exec.Status(new(status.Status)))
		if cacheRoot == "" {
			cacheRoot = DefaultLocalCacheRoot
		}
	} else if cacheRoot == "" {
		cacheRoot = DefaultCacheRoot
	}
	log.Debug.Printf("gql: using cachedir %s", cacheRoot)
	if os.Getenv("BIGMACHINE_MODE") != "" {
		panic("bigmachine slave")
	}
}

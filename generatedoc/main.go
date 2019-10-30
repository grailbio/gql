package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/grail"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
	"github.com/grailbio/base/vcontext"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/lib"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/gql/termutil"
)

var (
	docTemplateDirFlag = flag.String("doc-template-dir", ".", "Directory that contains README.md templates")

	sess *gql.Session
	ctx  context.Context
)

func translateDoc(text string, out io.Writer) {
	s := bufio.NewScanner(bytes.NewReader([]byte(text)))
	for s.Scan() {
		line := s.Text()
		for _, ch := range []string{"├", "║", "│"} {
			if strings.Contains(line, ch) {
				line = strings.Replace(strings.TrimSpace(line), ch, "|", -1)
			}
		}
		line = strings.Replace(line, "┤", "|", -1)
		line = strings.Replace(line, "─", "-", -1)
		line = strings.Replace(line, "┼", "|", -1)
		if _, err := out.Write([]byte(line)); err != nil {
			log.Panic(err)
		}
		if _, err := out.Write([]byte{'\n'}); err != nil {
			log.Panic(err)
		}
	}
}

func builtinFunctionsDoc() string {
	sess := gql.NewSession()
	out := bytes.NewBuffer(nil)

	globals := sess.Bindings().GlobalVars()
	sort.Slice(globals, func(i, j int) bool { return globals[i].Str() < globals[j].Str() })
	remaining := map[symbol.ID]bool{}
	for _, name := range globals {
		remaining[name] = true
	}
	showHelp := func(name string) {
		id := symbol.Intern(name)
		if !remaining[id] {
			log.Panicf("function not found (or described twice): %s", name)
		}
		delete(remaining, id)
		val, ok := sess.Bindings().Lookup(id)
		must.True(ok, id)
		translateDoc(fmt.Sprintf("#### %s\n\n%s\n\n", name, gql.DescribeValue(val)), out)
	}
	out.WriteString("### Table manipulation\n\n")
	for _, name := range []string{"map", "filter", "reduce", "flatten", "concat", "cogroup",
		"firstn", "minn", "sort", "join", "transpose", "gather", "spread", "collapse", "joinbed", "count", "pick",
		"table", "readdir", "table_attrs", "force"} {
		showHelp(name)
	}
	mark := func(ops []string) {
		for _, name := range ops {
			id := symbol.Intern(name)
			if !remaining[id] {
				panic(name)
			}
			delete(remaining, id)
		}
	}

	out.WriteString("### Row manipulation\n\n")
	out.WriteString(`
#### Struct comprehension

Usage: {col1:expr1, col2:expr2, ..., colN:exprN}

{...} composes a struct. For example,

    x := table({f0:"foo", f1:123}, {f0:"bar", f1:234})

creates the following table and assigns it to $x.

        ║ f0  ║ f1  ║
        ├─────┼─────┤
        │foo  │ 123 │
        │bar  │ 234 │

The "colK:" part of each element can be omitted, in which case the column name
is auto-computed using the following rules:

- If expression is of form "expr.field" (struct field reference), then
  "field" is used as the column name.

- Similarly, if expression is of form "$field" (struct field reference), then
  "field" is used as the column name.

- If expression is of form "var", then "var" is used as the column name.

- Else, column name will be "fN", where "N" is 0 for the first column, 1 for the
  2nd column, and so on.

For example:

    x := table({col0:"foo", f1:123}, {col1:"bar", f1:234})
    y := 10
    x | map({$col0, z, newcol: $col1})

        ║ col0  ║ z  ║ newcol ║
        ├───────┼────┼────────┤
        │foo    │ 10 │123     │
        │bar    │ 10 │234     │


The struct literal expression can contain a regex of form

        expr./regex/

"expr" must be another struct. "expr" is most often "_". "{var./regex/}" is
equivalent to a struct that contains all the fields in "var" whose names match
regex.  Using the above example,

        x | map({_./.*1$/})

will pick only column f1. The result will be

        ║ f1  ║
        ├─────┤
        │ 123 │
        │ 234 │

As an syntax suger, you can omit the "var." part if var is "_". So "map($x,
{_./.*1$/})" can also be written as "map($x, {/.*1$/})".

`)

	for _, name := range []string{"unionrow", "optionalfield", "contains"} {
		showHelp(name)
	}
	out.WriteString("### File I/O\n\n")
	for _, name := range []string{"read", "write", "writecols"} {
		showHelp(name)
	}
	mark([]string{"infix:==", "infix:!=", "infix:>=", "infix:>", "infix:==?", "infix:?==", "infix:?==?"})
	translateDoc(`### Predicates

    expr0 == expr1
    expr0 > expr1
    expr0 >= expr1
    expr0 < expr1
    expr0 <= expr1
    max(expr1, expr2, ..., exprN)
    min(expr1, expr2, ..., exprN)

    expr0 ?== expr1
    expr0 ==? expr1
    expr0 ?==? expr1

These predicates can be applied to any scalar values, including
ints, floats, strings, chars, and dates, and nulls.
The two sides of the operator must be of the same type, or null.
A null value is treated as ∞. Thus, 1 < NA, but 1 > -NA

Predicates "==?", "?==", "?==?" are the same as "==", as long as both sides are
non-null. "X==?Y" is true if either X==Y, or Y is null. "X?==Y" is true if
either X==Y, or X is null. "X?==?Y" is true if either X==Y, or X is null, or Y
is null. These predicates can be used to do outer, left, or right joins with
join builtin.

`, out)

	mark([]string{"infix:%", "infix:*", "infix:/", "infix:+", "infix:-", "prefix:-", "min", "max"})

	showHelp("isnull")
	showHelp("istable")
	showHelp("isstruct")

	translateDoc(`### Arithmetic and string operators

    expr0 + expr1

The "+" operator can be applied to ints, floats, strings, and structs.  The two
sides of the operator must be of the same type. Adding two structs will produce
a struct whose fields are union of the inputs. For example,

    {a:10,b:20} + {c:30} == {a:10,b:20,c:30}
    {a:10,b:20,c:40} + {c:30} == {a:10,b:20,c:30}

As seen in the second example, if a column appears in both inputs, the second
value will be taken.


    -expr0

The unary "-" can be applied to ints, floats, and strings.  Expression "-str"
produces a new string whose sort order is lexicographically reversed. That is,
for any two strings A and B, if A < B, then -A > -B. Unary "-" can be used to
sort rows in descending order of string column values.

    expr0 * expr1
    expr0 % expr1
    expr0 - expr1
    expr0 / expr1

The above operators can be applied to ints and floats.  The two sides of the
operator must be of the same type.


`, out)

	showHelp("string_count")
	showHelp("regexp_match")
	showHelp("regexp_replace")
	showHelp("string_len")
	showHelp("string_has_suffix")
	showHelp("string_has_prefix")
	showHelp("string_replace")
	showHelp("substring")
	showHelp("sprintf")
	showHelp("string")
	showHelp("int")
	showHelp("float")
	showHelp("hash64")
	showHelp("land")
	showHelp("lor")

	showHelp("isset")

	mark([]string{"prefix:!"})
	translateDoc(`### Logical operators

    !expr

`, out)

	out.WriteString("### Miscellaneous functions\n\n")
	for _, name := range []string{"print"} {
		showHelp(name)
	}

	if len(remaining) > 0 {
		var names []string
		for name := range remaining {
			names = append(names, name.Str())
		}
		log.Panicf("Undocumented function: %+v", names)
	}
	return out.String()
}

var (
	includeRe    = regexp.MustCompile(`{{include:\s*([^}]+)}}`)
	evalRe       = regexp.MustCompile(`(?s){{eval(,[^:]*)?:\s*(.*?)}}`)
	evalScriptRe = regexp.MustCompile(`(?s){{evalscript(,[^:]*)?:\s*(.*?)}}`)
)

func openFile(path string) file.File {
	in, err := file.Open(ctx, filepath.Join(path))
	if err != nil {
		log.Panicf("open %s: %v", path, err)
	}
	return in
}

func closeFile(f file.File) {
	if err := f.Close(ctx); err != nil {
		log.Panic(err)
	}
}

// Option for {{run: blahblah}}.
type evalOpts struct {
	maxLines int
}

func parseRunOpts(s string) evalOpts {
	opts := evalOpts{maxLines: 100}
	m := regexp.MustCompile(`maxlines=(\d+)`).FindStringSubmatch(s)
	if m != nil {
		var (
			val int64
			err error
		)
		if val, err = strconv.ParseInt(m[1], 0, 32); err != nil {
			log.Panic(err)
		}
		opts.maxLines = int(val)
	}
	return opts
}

func evalToString(ctx context.Context, expr string, opts evalOpts, e *errors.Once) string {
	var val gql.Value
	func() {
		defer func() {
			if err := recover(); err != nil {
				log.Error.Printf("eval `%s`: %v", expr, err)
				e.Set(errors.E("eval", expr, err))
				val = gql.NewString(fmt.Sprintf("ERROR: %s", expr))
			}
		}()
		statements, err := sess.Parse("(input)", []byte(expr))
		if err != nil {
			log.Error.Printf("eval '%s': %v", expr, err)
			e.Set(err)
			return
		}
		val = sess.EvalStatements(ctx, statements)
	}()
	buf := bytes.Buffer{}
	for _, line := range strings.Split(expr, "\n") {
		buf.WriteString("    " + line + "\n")
	}
	buf.WriteString("\n\n")

	out := termutil.NewBufferPrinter()
	args := gql.PrintArgs{
		Out:     out,
		Mode:    gql.PrintValues,
		TmpVars: &gql.TmpVars{},
	}
	val.Print(ctx, args)

	for i, line := range strings.Split(out.String(), "\n") {
		if i > opts.maxLines {
			break
		}
		if val.Type() != gql.TableType {
			// Show a non-table value as a code block.  For a table printouts, the
			// markdown displayer will turn it into a pretty html table.
			buf.WriteString("    ")
		}
		buf.WriteString(line + "\n")
	}
	return buf.String()
}

func generateDoc() {
	out := bufio.NewWriter(os.Stdout)
	bb, err := ioutil.ReadFile("README.md.tpl")
	if err != nil {
		log.Panicf("README.md.tpl: %v. You must invoke -generate-doc in the cmd/grail-query source directory.", err)
	}
	tpl := string(bb)
	tpl = strings.Replace(tpl, "{{builtin}}", builtinFunctionsDoc(), -1)
	tpl = includeRe.ReplaceAllStringFunc(tpl, func(s string) string {
		m := includeRe.FindStringSubmatch(s)
		if m == nil {
			panic(s)
		}
		in := openFile(m[1])
		defer closeFile(in)
		out := strings.Builder{}
		scanner := bufio.NewScanner(in.Reader(ctx))
		for scanner.Scan() {
			out.WriteString("        ")
			out.Write(scanner.Bytes())
			out.WriteByte('\n')
		}
		return out.String()
	})

	e := errors.Once{}
	tpl = evalRe.ReplaceAllStringFunc(tpl, func(s string) string {
		m := evalRe.FindStringSubmatch(s)
		evalOpts := parseRunOpts(m[1])
		expr := m[2]
		return evalToString(ctx, expr, evalOpts, &e)
	})

	tpl = evalScriptRe.ReplaceAllStringFunc(tpl, func(s string) string {
		m := evalScriptRe.FindStringSubmatch(s)
		evalOpts := parseRunOpts(m[1])
		path := m[2]
		expr, err := file.ReadFile(ctx, path)
		if err != nil {
			e.Set(errors.E("readfile", path, err))
			return "ERROR: " + path
		}
		return evalToString(ctx, "{"+string(expr)+"}", evalOpts, &e)
	})

	translateDoc(tpl, out)
	e.Set(out.Flush())
	if err := e.Err(); err != nil {
		log.Panic(err)
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	shutdown := grail.Init()
	defer shutdown()
	ctx = vcontext.Background()
	if err := os.Chdir(*docTemplateDirFlag); err != nil {
		log.Panic(err)
	}
	gql.Init(gql.Opts{OverwriteFiles: true})
	sess = gql.NewSession()
	lib, err := sess.Parse("lib", []byte(lib.Script))
	if err != nil {
		log.Panicf("load lib: %v", err)
	}
	sess.EvalStatements(ctx, lib)
	generateDoc()
}

// Package cmd implements command-line parsing and a REPL loop.
package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"text/tabwriter"
	"unicode/utf8"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/vcontext"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/termutil"
	"github.com/yasushi-saito/readline"
	"github.com/yasushi-saito/readline/creadline"
	"v.io/x/lib/vlog"
)

// command defines a GQL command.
type command struct {
	callback func(ctx context.Context, args string)
	help     string
}

// Env captures all the state needed to run gql commands.  Thread compatible
type Env struct {
	// Sess is the GQL session that runs the commands.
	sess *gql.Session
	// Interactive is true if the application is running under an interactive
	// terminal.
	interactive bool
	// To implement "help", "quit", etc.
	builtinCmds map[string]command
	// Variables created as a placeholder for nested tables during interactive
	// display.
	tmpVars *gql.TmpVars
	orgLog  *vlog.Logger
}

var (
	pipeRE = regexp.MustCompile(`(.*)\|\s*(less)$`)

	// redirectRE matches >>path or >path. The "path" deliberately restricts the
	// characters to avoid matching a legit GQL expression.
	redirectRE = regexp.MustCompile(`(.*?)(>?)>\s*([-\w\d.,=~_/:]+)$`)
)

// Separated from parseCommandline for unittesting.
func parseRedirect(line string) (prefix string, out string, append bool, pipe bool) {
	prefix = strings.TrimSpace(line)
	if m := pipeRE.FindStringSubmatch(prefix); m != nil {
		prefix = strings.TrimSpace(m[1])
		out = strings.TrimSpace(m[2])
		pipe = true
	} else if m := redirectRE.FindStringSubmatch(prefix); m != nil {
		prefix = strings.TrimSpace(m[1])
		append = (m[2] != "")
		out = strings.TrimSpace(m[3])
	}
	return
}

// New creates a new environment. Arg interactive should be true if this is an
// interactive commandline session.
func New(sess *gql.Session, interactive bool) *Env {
	env := &Env{
		sess:        sess,
		interactive: interactive,
		orgLog:      vlog.Log,
		tmpVars:     &gql.TmpVars{},
	}

	env.builtinCmds = map[string]command{
		"logdir": command{
			callback: env.runLogdir,
			help: `Usage: log [dirname]

  The logdir command sends log messages to files under the given directory.
  Invoking "logdir" without an argument will send log messages to stderr.`},
		"quit": command{
			callback: env.runQuit,
			help: `Usage: quit

  Quit terminates GQL.`},
		"help": command{
			callback: env.runHelp,
			help: `Usage: help [value]

  Help shows help messages. If "value" is given, shows the help for the
  value. Value is typically a table name.`},
		"history": command{
			callback: env.runHistory,
			help: `Usage: history

  Shows the list of past inputs.`},
	}
	return env
}

// parseCommandline checks if a commandline contains a redirect suffix such as
// '>file'. If so, it removes the suffix from the commandline and returns a
// Printer object that matches the redirect spec.
func (c *Env) parseCommandline(line string) (string, termutil.Printer, bool) {
	prefix, out, append, pipe := parseRedirect(line)
	prefix = strings.TrimSpace(line)
	if m := pipeRE.FindStringSubmatch(prefix); m != nil {
		prefix = strings.TrimSpace(m[1])
		out = strings.TrimSpace(m[2])
		pipe = true
	} else if m := redirectRE.FindStringSubmatch(prefix); m != nil {
		prefix = strings.TrimSpace(m[1])
		append = (m[2] != "")
		out = strings.TrimSpace(m[3])
	}

	if out != "" {
		if pipe {
			p, err := termutil.NewPipePrinter(out)
			if err == nil {
				return prefix, p, true
			}
			log.Error.Print(err)
		} else {
			p, err := termutil.NewFilePrinter(out, append)
			if err == nil {
				return prefix, p, true
			}
			log.Error.Print(err)
		}
	}
	return prefix, c.NewOutput(), false
}

// runLogdir implements the "logdir" command.
//
// TODO(saito) This code assumes that the underlying logger is vlog.
func (c *Env) runLogdir(ctx context.Context, args string) {
	path := strings.TrimSpace(args)
	if path == "" {
		vlog.Log = c.orgLog
		return
	}
	if err := os.Mkdir(path, 0755); err != nil && !os.IsExist(err) {
		log.Error.Printf("logdir %s: %v", path, err)
		return
	}
	vl := vlog.NewLogger("vlog")
	vl.Configure(vlog.LogDir(path))      // nolint: errcheck
	vl.Configure(vlog.LogToStderr(true)) // nolint: errcheck
	vlog.Log = vl
}

// Loop runs an interactive eval loop. It never retuns.
func (c *Env) Loop() {
	termutil.InstallSignalHandler()
	for {
		termutil.ClearSignal()
		ctx, done := termutil.WithCancel(vcontext.Background())
		func() {
			defer done()
			line, err := readline.Readline("gql> ")
			if err != nil {
				fmt.Printf("\nreadline: %v\n", err)
				return
			}
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				return
			}
			tokens := strings.SplitN(trimmed, " ", 2)
			if cmd, ok := c.builtinCmds[tokens[0]]; ok {
				args := ""
				if len(tokens) > 1 {
					args = tokens[1]
				}
				cmd.callback(ctx, args)
				return
			}
			c.runEval(ctx, line)
		}()
	}
}

// runEval treats "line" as a GQL expression and evaluates it.  If the line is
// an incomplete expression, it prompts the user for further input.
func (c *Env) runEval(ctx context.Context, line string) {
	line += "\n"
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Recovered from error: %v: %v", err, string(debug.Stack()))
		}
		line = strings.Replace(line, "\n", " ", -1)
		line = strings.TrimSpace(line)
		if err := readline.AddHistory(line); err != nil {
			log.Error.Printf("readline.AddHistory: %v", err)
		}
	}()
	for {
		expr, out, redirected := c.parseCommandline(line)
		defer out.Close()
		statements, err := c.sess.Parse("(stdin)", []byte(expr))
		switch {
		case err == nil:
			if len(statements) == 0 {
				log.Printf("Found no statement")
				return
			}
			val := c.sess.EvalStatements(ctx, statements)
			c.PrintValue(ctx, val, gql.PrintValues, out)
			return
		case err != io.EOF:
			log.Error.Printf("Readline error: %v", err)
			return
		case strings.HasSuffix(expr, ";") || redirected:
			log.Error.Printf("Parse error: %v", err)
			return
		default:
			l, err := readline.Readline("... ")
			if err != nil {
				fmt.Printf("\nreadline: %v\n", err)
				return
			}
			line += l + "\n"
		}
	}
}

// PrintValue prints the given value to the terminal with paging.
func (c *Env) PrintValue(ctx context.Context, val gql.Value, mode gql.PrintMode, out termutil.Printer) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Recovered from error: %v: %v", err, string(debug.Stack()))
		}
	}()
	args := gql.PrintArgs{
		Out:     out,
		Mode:    mode,
		TmpVars: c.tmpVars,
	}
	val.Print(ctx, args)
	if args.Out.Ok() {
		args.Out.Write([]byte("\n"))
		c.tmpVars.Flush(c.sess)
	}
}

// NewOutput creates a Printer object that prints to the standard output.
func (c *Env) NewOutput() termutil.Printer {
	if c.interactive {
		return termutil.NewTerminalPrinter(os.Stdout)
	}
	return termutil.NewBatchPrinter(os.Stdout)
}

func (c *Env) runQuit(ctx context.Context, args string) {
	os.Exit(0)
}

func (c *Env) runHistory(ctx context.Context, args string) {
	defer func() {
		if err := readline.AddHistory(strings.TrimSpace("history " + args)); err != nil {
			log.Error.Printf("readline.AddHistory: %v", err)
		}
	}()
	_, out, _ := c.parseCommandline(args)
	defer out.Close()

	h := creadline.HistoryGetHistoryState()
	first := 0
	if len(h.Entries) > 1000 {
		first = len(h.Entries) - 1000
	}
	for i := first; i < len(h.Entries); i++ {
		fmt.Fprintf(out, "%3d %s\n", i+1, h.Entries[i].Line)
	}
}

func (c *Env) runHelp(ctx context.Context, args string) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Recovered from error: %v: %v", err, string(debug.Stack()))
		}
		if err := readline.AddHistory(strings.TrimSpace("help " + args)); err != nil {
			log.Error.Printf("readline.AddHistory: %v", err)
		}
	}()

	expr, out, _ := c.parseCommandline(args)
	defer out.Close()

	writeLine := func(s string) {
		out.WriteString(s)
		out.WriteString("\n")
	}
	writeList := func(list []string) {
		sort.Strings(list)
		w := tabwriter.NewWriter(out, 0, 0, 1, ' ', 0)
		last := rune(0)
		col := 0
		for _, name := range list {
			letter, _ := utf8.DecodeRuneInString(name)
			if col > 8 || (last != 0 && letter != last) {
				for col <= 8 {
					fmt.Fprint(w, "\t")
					col++
				}
				fmt.Fprint(w, "\n")
				col = 0
			}
			if col > 0 {
				fmt.Fprint(w, "\t")
			}
			fmt.Fprint(w, name)
			last = letter
			col++
		}
		w.Flush()
		writeLine("")
	}

	if expr != "" {
		if cmd, ok := c.builtinCmds[expr]; ok {
			writeLine(cmd.help)
			return
		}
		statements, err := c.sess.Parse("(stdin)", []byte(expr))
		if err != nil {
			log.Error.Print(err)
			return
		}
		if len(statements) == 0 {
			return
		}
		val := c.sess.EvalStatements(ctx, statements)
		c.PrintValue(ctx, val, gql.PrintDescription, out)
		return
	}
	writeLine("* List of commands:")
	for name, cmd := range c.builtinCmds {
		writeLine("- " + name + "\n" + cmd.help + "\n")
	}
	writeLine(`Any other command will be interpreted as a GQL expression to evaluate.

A command can be followed by ">file", ">>file", or "|less".
- >file writes the outputs to a file.
- >>file appends the outputs to a file.
- |less sends the outputs to the "less" command.

For example,

  ccga |less

will show the contents of the CCGA table through the less command.
`)

	var builtinFuncs, userFuncs, vars []string
	for _, id := range c.sess.Bindings().GlobalVars() {
		name := id.Str()
		if strings.HasPrefix(name, "tmp") || strings.HasPrefix(name, "_") {
			continue
		}
		val, _ := c.sess.Bindings().Lookup(id)
		switch {
		case val.Type() == gql.FuncType && val.Func(nil).Builtin():
			builtinFuncs = append(builtinFuncs, name)
		case val.Type() == gql.FuncType && !val.Func(nil).Builtin():
			userFuncs = append(userFuncs, name)
		default:
			vars = append(vars, name)
		}
	}
	writeLine("\n* List of builtin functions. Type 'help <symbol>' to show help.")
	writeList(builtinFuncs)
	writeLine("\n* List of user-defined functions. Type 'help <symbol>' to show help.")
	writeList(userFuncs)
	writeLine("\n* List of global variables. Type 'help <symbol>' to show help.")
	writeList(vars)
}

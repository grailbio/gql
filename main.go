package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	awssession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/s3file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
	"github.com/grailbio/bigslice/sliceconfig"
	"github.com/grailbio/gql/cmd"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/lib"
	"github.com/yasushi-saito/readline"
	"golang.org/x/crypto/ssh/terminal"
)

var (
	evalFlag           = flag.Bool("eval", false, "If set, evaluate the expressions found in the commandline, show the result, then exit the process")
	overwriteFilesFlag = flag.Bool("overwrite-files", false, "If false, write() will become a noop if the target file already exists")
	outputFlag         = flag.String("output", "", "File to write the final expression value to.")
	cacheDirFlag       = flag.String("cache-dir", "", "The place to store btsv cache files.")
	immutableFilesFlag = flag.String("immutable-files", "", `Comma-separated list of regexps of files assumeb to be immutable.
If empty, "^s3://grail-clinical.*" and "^s3://grail-results.*" are used.`)
)

func setGlobalVarFromFlags(arg string) {
	re0 := regexp.MustCompile("^-?-([a-zA-Z_][a-zA-Z_0-9]*)$")
	re1 := regexp.MustCompile("^-?-([a-zA-Z_][a-zA-Z_0-9]*)=(.*)$")

	if m := re0.FindStringSubmatch(arg); m != nil {
		gql.RegisterGlobalConst(m[1], gql.True)
		log.Printf("Set %s=true", m[1])
		return
	}
	m := re1.FindStringSubmatch(arg)
	must.Truef(m != nil,
		"Failed to parse '%v'. Arg must be either `-flag` (boolean arg) or `-flag=value` (string arg)", arg)
	if n, err := strconv.ParseInt(m[2], 0, 64); err == nil {
		log.Printf("Set %s=%d (int)", m[1], n)
		gql.RegisterGlobalConst(m[1], gql.NewInt(n))
		return
	}
	if f, err := strconv.ParseFloat(m[2], 64); err == nil {
		log.Printf("Set %s=%f (float)", m[1], f)
		gql.RegisterGlobalConst(m[1], gql.NewFloat(f))
		return
	}
	log.Printf("Set %s=`%s` (string)", m[1], m[2])
	gql.RegisterGlobalConst(m[1], gql.NewString(m[2]))
}

func printValue(ctx context.Context, env *cmd.Env, val gql.Value) {
	if *outputFlag != "" {
		must.Truef(val.Type() == gql.TableType,
			"--output value must be a table (it is %v)", val)
		fh := gql.GetFileHandlerByPath(*outputFlag)
		fh.Write(ctx, *outputFlag, &gql.ASTUnknown{}, val.Table(nil), 1, *overwriteFilesFlag)
	} else if val.Type() != gql.InvalidType {
		out := env.NewOutput()
		defer out.Close()
		env.PrintValue(ctx, val, gql.PrintValues, out)
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	file.RegisterImplementation("s3", func() file.Implementation {
		return s3file.NewImplementation(s3file.NewDefaultProvider(awssession.Options{}), s3file.Options{})
	})
	if err := readline.Init(readline.Opts{Name: "grail-query", ExpandHistory: true}); err != nil {
		log.Error.Printf("readline.Init: %v", err)
	}
	session, shutdown := sliceconfig.Parse()
	defer shutdown()
	ctx := context.Background()
	opts := gql.Opts{
		BackgroundContext: ctx,
		OverwriteFiles:    *overwriteFilesFlag,
		CacheDir:          *cacheDirFlag,
		BigsliceSession:   session,
	}
	if *immutableFilesFlag != "" {
		for _, re := range strings.Split(*immutableFilesFlag, ",") {
			opts.ImmutableFilesRE = append(opts.ImmutableFilesRE, regexp.MustCompile(re))
		}
	}
	gql.Init(opts)
	sess := gql.NewSession()
	interactive := terminal.IsTerminal(syscall.Stdin) && terminal.IsTerminal(syscall.Stdout) && len(flag.Args()) == 0
	env := cmd.New(sess, interactive)
	lib, err := sess.Parse("lib", []byte(lib.Script))
	must.Nilf(err, "load lib")
	sess.EvalStatements(ctx, lib)
	if *evalFlag {
		must.True(len(flag.Args()) > 0, "No expression specified with -eval")
		statements, err := sess.Parse("(cmdline)", []byte(strings.Join(flag.Args(), " ")))
		must.Nil(err, "parse expressions in the commandline")
		printValue(ctx, env, sess.EvalStatements(ctx, statements))
		return
	}
	if len(flag.Args()) > 0 {
		log.Printf("Start gql with commandline: %v", os.Args)
		scriptPath := flag.Arg(0)
		for _, arg := range flag.Args()[1:] {
			setGlobalVarFromFlags(arg)
		}
		printValue(ctx, env, sess.EvalFile(ctx, scriptPath))
	}
	// REPL
	must.True(*outputFlag == "", "--output cannot be used in non-REPL mode")
	fmt.Println("Aloha!")
	env.Loop()
}

package main

import (
	"context"
	"flag"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice/sliceconfig"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/lib"
)

var (
	jupyterConnectionFlag = flag.String("jupyter-connection", "", "A JSON file specifying jupyter connection config. This flag will start GQL Jupyter kernel.")
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	session, shutdown := sliceconfig.Parse()
	defer shutdown()
	ctx := context.Background()
	opts := gql.Opts{
		BackgroundContext: ctx,
		OverwriteFiles:    true,
		BigsliceSession:   session,
	}
	gql.Init(opts)
	sess := gql.NewSession()
	lib, err := sess.Parse("lib", []byte(lib.Script))
	if err != nil {
		log.Panicf("load lib: %v", err)
	}
	sess.EvalStatements(ctx, lib)
	jupyterKernel(ctx, *jupyterConnectionFlag, sess)
}

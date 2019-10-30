package main

import (
	"context"
	"flag"
	"log"
	"os"
	"testing"

	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/gqltest"
	"github.com/grailbio/gql/lib"
	"github.com/grailbio/testutil/expect"
)

// Tests in this file require TestMain setup in e2e_test.go

var (
	manualFlag = flag.Bool("run-manual-tests", false, "run the CCGA tests")
	session    *gql.Session
)

func maybeSkipManualTest(t *testing.T) {
	if !*manualFlag {
		t.Skip("-run-manual-tests not set")
	}
}

func TestBasename(t *testing.T) {
	maybeSkipManualTest(t)
	expect.EQ(t, gqltest.Eval(t, `basename("foo/bar.txt")`, session).Str(nil), "bar.txt")
	expect.EQ(t, gqltest.Eval(t, `basename("foo/bar")`, session).Str(nil), "bar")
}

func TestMain(m *testing.M) {
	session = gqltest.NewSession()
	ctx := context.Background()
	if *manualFlag {
		lib, err := session.Parse("lib", []byte(lib.Script))
		if err != nil {
			log.Panicf("load lib: %v", err)
		}
		session.EvalStatements(ctx, lib)
	}
	status := m.Run()
	os.Exit(status)
}

// Package gqltest provides helper functions for unittests
package gqltest

import (
	"context"
	"math"
	"sort"
	"sync"
	"testing"

	"github.com/grailbio/base/must"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/termutil"
	"github.com/grailbio/testutil"
)

var once sync.Once

// Eval parses and evaluates a given expression or statement.
func Eval(t testing.TB, str string, sess *gql.Session) gql.Value {
	statements, err := sess.Parse("(input)", []byte(str))
	must.Nil(err)
	return sess.EvalStatements(context.Background(), statements)
}

// NewSession creates a new gql.Session with good defaults.113
func NewSession() *gql.Session {
	once.Do(func() {
		opts := gql.Opts{
			OverwriteFiles: true,
			CacheDir:       testutil.GetTmpDir() + "/gqlcache",
		}
		gql.Init(opts)
	})
	return gql.NewSession()
}

// ReadTable reads the table contents as a list of strings.
//
// REQUIRES: t.Type()==TableType
func ReadTable(t gql.Value) []string {
	ctx := context.Background()
	s := []string{}
	scanner := t.Table(nil).Scanner(ctx, 0, 1, 1)
	for scanner.Scan() {
		out := termutil.NewBufferPrinter()
		args := gql.PrintArgs{
			Out:                out,
			Mode:               gql.PrintCompact,
			MaxInlinedTableLen: math.MaxInt64,
		}
		scanner.Value().Print(ctx, args)
		s = append(s, out.String())
	}
	return s
}

// ReadTableSorted reads the table contents as a list of strings, then sort them in
// lexicographic ascending order.
//
// REQUIRES: t.Type()==TableType
func ReadTableSorted(t gql.Value) []string {
	ctx := context.Background()
	s := []string{}
	scanner := t.Table(nil).Scanner(ctx, 0, 1, 1)
	for scanner.Scan() {
		s = append(s, scanner.Value().String())
	}
	sort.Strings(s)
	return s
}

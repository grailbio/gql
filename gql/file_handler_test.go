package gql_test

import (
	"testing"

	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/gql/gql"
)

func TestGetFileHandler(t *testing.T) {
	fh := gql.GetFileHandlerByPath("blah.tsv")
	expect.EQ(t, fh.Name(), "tsv")

	fh = gql.GetFileHandlerByPath("blah.tsv.gz")
	expect.EQ(t, fh.Name(), "tsv")

	fh = gql.GetFileHandlerByPath("blah.bam")
	expect.EQ(t, fh.Name(), "bam")
}

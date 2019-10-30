package gql

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/testutil/h"
)

func TestCache(t *testing.T) {
	tempDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()

	cacheRoot = tempDir
	name := "testcache.txt"
	path, found := LookupCache(ctx, name)
	expect.False(t, found)
	expect.That(t, path, h.HasPrefix(cacheRoot))
	expect.NoError(t, ioutil.WriteFile(path, []byte("blah"), 0600))
	ActivateCache(ctx, name, path)

	path2, found := LookupCache(ctx, name)
	expect.True(t, found)
	expect.EQ(t, path2, path)
}

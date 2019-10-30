package cmd

import (
	"testing"

	"github.com/grailbio/testutil/expect"
)

func TestParseRedirect(t *testing.T) {
	p, out, append, pipe := parseRedirect("foo")
	expect.EQ(t, p, "foo")
	expect.EQ(t, out, "")

	p, out, append, pipe = parseRedirect("foo >bar")
	expect.EQ(t, p, "foo")
	expect.EQ(t, out, "bar")
	expect.False(t, append)
	expect.False(t, pipe)

	p, out, append, pipe = parseRedirect("foo >> bar")
	expect.EQ(t, p, "foo")
	expect.EQ(t, out, "bar")
	expect.True(t, append)
	expect.False(t, pipe)

	p, out, append, pipe = parseRedirect("foo | less")
	expect.EQ(t, p, "foo")
	expect.EQ(t, out, "less")
	expect.False(t, append)
	expect.True(t, pipe)
}

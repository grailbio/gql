package termutil_test

import (
	"testing"

	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/gql/termutil"
)

func TestBufferPrinter(t *testing.T) {
	p := termutil.NewBufferPrinter()
	p.WriteString("hello")
	expect.EQ(t, p.String(), "hello")
	p.Reset()
	p.WriteString("olleh")
	expect.EQ(t, p.String(), "olleh")
}

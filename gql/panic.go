package gql

import (
	"runtime/debug"

	"github.com/grailbio/base/errors"
)

// Recover runs the given function, catching any panic thrown by the function
// and turning it into an error. If the function finishes without panicking,
// CatchPanic returns nil.
func Recover(cb func()) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.E("panic %v: %v", e, string(debug.Stack()))
		}
	}()
	cb()
	return nil
}

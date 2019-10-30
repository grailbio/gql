package gql

// Logging functions, similar to those in "log" package. They can show the
// source-code location of error.

import (
	"fmt"

	"github.com/grailbio/base/log"
)

// Debugf is similar to log.Debug.Printf(...). Arg "ast" is the source-code
// location of the error. If "ast" is unknown, pass &ASTUnknown{}.
func Debugf(ast ASTNode, format string, args ...interface{}) {
	if log.At(log.Debug) {
		log.Output(2, log.Debug, ast.pos().String()+":"+ast.String()+": "+fmt.Sprintf(format, args...)) // nolint: errcheck
	}
}

// Logf is similar to log.Printf(...). Arg "ast" is the source-code
// location of the error. If "ast" is unknown, pass &ASTUnknown{}.
func Logf(ast ASTNode, format string, args ...interface{}) {
	if log.At(log.Info) {
		log.Output(2, log.Info, ast.pos().String()+":"+ast.String()+": "+fmt.Sprintf(format, args...)) // nolint: errcheck
	}
}

// Logf is similar to log.Error.Printf(...). Arg "ast" is the source-code
// location of the error. If "ast" is unknown, pass &ASTUnknown{}.
func Errorf(ast ASTNode, format string, args ...interface{}) {
	log.Output(2, log.Error, ast.pos().String()+":"+ast.String()+": "+fmt.Sprintf(format, args...)) // nolint: errcheck
}

// Logf is similar to log.Panicf.(...). Arg "ast" is the source-code location of
// the error. If "ast" is unknown, pass &ASTUnknown{}.
func Panicf(ast ASTNode, format string, args ...interface{}) {
	panic(ast.pos().String() + ":" + ast.String() + ": " + fmt.Sprintf(format, args...))
}

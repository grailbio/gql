package gql

import (
	"context"
	"regexp"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/hash"
)

// FileHandler defines methods for reading a file into a Table and writing a
// table into a file.
type FileHandler interface {
	// Name returns the file-type name, e.g., "tsv" for TSV handler, "bam" for BAM
	// handler.
	Name() string
	// Open opens the file and returns a Table that reads its contents.  Arg "ast"
	// can be passed to functions such as Logf, Panicf to report the source-code
	// location on error.
	Open(ctx context.Context, path string, ast ASTNode, hash hash.Hash) Table
	// Write writes the contents of the table to the given file. "nshard" the
	// number of file shards to create. It is meaningful only for btsv files.
	// "overwrite" is true iff. the caller wishes to overwrite the file if it
	// exits already.
	//
	// Arg "ast" can be passed to functions such as Logf, Panicf to report the
	// source-code location on error.
	Write(ctx context.Context, path string, ast ASTNode, table Table, nShard int, overwrite bool)
}

// OptionalCompression is a regexp that matches compressed-file suffixes.
const OptionalCompression = `(\.gz|\.bz2|\.zst)?$`

// GetFileHandlerByName finds the FileHandler object with the given name.
func GetFileHandlerByName(name string) FileHandler {
	for _, t := range fileHandlers {
		if t.fh.Name() == name {
			return t.fh
		}
	}
	panic(name + ": unsupported file handler")
}

// GetFileHandlerByName finds the FileHandler for the given file. It works only
// by examining the path name. It does not read file contents. If multiple
// handlers match the path name, the one with the longest match will be used.
func GetFileHandlerByPath(path string) FileHandler {
	var (
		fh           FileHandler
		longestMatch int
	)
	for _, t := range fileHandlers {
		for _, re := range t.re {
			m := re.FindString(path)
			if len(m) > 0 && len(m) == longestMatch {
				log.Printf("open %s: multiple file types match this file (%s, %s). Using the former",
					fh.Name(), t.fh.Name())
			}
			if len(m) >= longestMatch {
				fh = t.fh
				longestMatch = len(m)
			}
		}
	}
	if fh == nil {
		panic(path + ": unsupported file handler")
	}
	return fh
}

type fileHandlerEntry struct {
	re []*regexp.Regexp
	fh FileHandler
}

var (
	fileHandlerMu sync.Mutex
	fileHandlers  []fileHandlerEntry
)

// RegisterFileHandler registers a file handler. This function is typically
// invoked in an init() function. Arg pathRE is the list of regexps that defines
// the pathame patterrs. If any of the patterns matches a path, the file handler
// is invoked to read and write the file.
func RegisterFileHandler(fh FileHandler, pathRE ...string) {
	fileHandlerMu.Lock()
	defer fileHandlerMu.Unlock()
	for _, t := range fileHandlers {
		if fh.Name() == t.fh.Name() {
			log.Panicf("RegisterFilehandler: duplicate handler '%s'", fh.Name())
		}
	}
	t := fileHandlerEntry{fh: fh}
	for _, re := range pathRE {
		t.re = append(t.re, regexp.MustCompile(re))
	}
	fileHandlers = append(fileHandlers, t)
}

package gql

// s3://grail-clinical-results/28726/P0062X0

import (
	"context"
	"strings"
	"sync"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bio/encoding/pam/pamutil"
	"github.com/grailbio/gql/symbol"
)

// Utility for creating a table out of CCGA tidy data files.  Set of files for a
// specific release data becomes one struct. These structs are combined into one
// table. This table will become accessible with global variable named "ccga".

// Len implements Struct
func (s *dirContents) Len() int {
	s.init()
	return len(s.values)
}

// Field implements Struct
func (s *dirContents) Field(i int) StructField {
	s.init()
	return s.values[i]
}

// Value implements Struct
func (s *dirContents) Value(colName symbol.ID) (Value, bool) {
	s.init()
	for _, f := range s.values {
		if f.Name == colName {
			return f.Value, true
		}
	}
	return Value{}, false
}

// dirContents implements Struct.
type dirContents struct {
	StructImpl
	ctx     context.Context
	rootDir string
	once    sync.Once
	values  []StructField
}

var _ Struct = &dirContents{}

func (s *dirContents) init() {
	s.once.Do(func() {
		seen := map[string]struct{}{}
		l := file.List(s.ctx, s.rootDir, true)
		for l.Scan() {
			f, ok := s.parsePath(&seen, l.Path())
			if !ok {
				continue
			}
			s.values = append(s.values, f)
		}
		if err := l.Err(); err != nil {
			log.Panicf("list %v: %v", s.rootDir, err)
		}
	})
}

func (s *dirContents) parsePath(dirs *map[string]struct{}, path string) (StructField, bool) {
	if !strings.HasPrefix(path, s.rootDir) {
		log.Panicf("list %v: not under %v", path, s.rootDir)
	}
	sampleID := file.Base(s.rootDir)
	relPath := path[len(s.rootDir)+1:]

	if _, ok := (*dirs)[relPath]; ok {
		// PAM/fragmentv3 leaf. We've already handled this.
		return StructField{}, false
	}

	// Check if this is a pam/fragment file shard. For such files, the directory
	// name is exposed to the user.
	if fi, err := pamutil.ParsePath(relPath); err == nil {
		(*dirs)[fi.Dir] = struct{}{}
		relPath = fi.Dir
	}

	// Remove occurrences of sampleIDs from the path so that columns for different
	// samples will have the same set of column names.
	colName := strings.Replace(relPath, sampleID+".", "", -1)
	colName = strings.Replace(colName, sampleID+"/", "", -1)
	colName = strings.Replace(colName, sampleID+"_", "", -1)
	colName = strings.Replace(colName, "/", "_", -1)
	colName = strings.Replace(colName, ".", "_", -1)
	colID := symbol.Intern(colName)
	return StructField{Name: colID, Value: NewFileName(path)}, true
}

// ReadDir creates a Struct consisting of files in a directory.  The field name
// is a sanitized pathname, value is either a Table (if the file is a .tsv,
// .btsv, etc), or a string (if the file cannot be parsed as a table).
func ReadDir(ctx context.Context, rootDir string) Struct {
	if ctx == nil {
		panic("null ctx")
	}
	var s Struct = &dirContents{
		ctx:     ctx,
		rootDir: rootDir,
	}
	InitStruct(s)
	return s
}

func init() {
	RegisterBuiltinFunc("readdir",
		`Usage: readdir(path)

readdir creates a Struct consisting of files in the given directory.  The field
name is a sanitized pathname, value is either a Table (if the file is a .tsv,
.btsv, etc), or a string (if the file cannot be parsed as a table).`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			dir := args[0].Str()
			return NewStruct(ReadDir(ctx, dir))
		},
		func(ast ASTNode, args []AIArg) AIType { return AIStructType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}})
}

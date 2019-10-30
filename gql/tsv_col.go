package gql

import (
	"context"
	"errors"
	"strings"
	"sync"
	"text/template"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/fileio"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/gql/symbol"
)

type columnarTemplate struct {
	Name   string
	Number int
}

func dictPathname(tpl *template.Template, gzipFiles bool) string {
	obuf := &strings.Builder{}
	if err := tpl.Execute(obuf, columnarTemplate{"data_dictionary", 0}); err != nil {
		log.Panicf("failed to execute template for data dictionary: %v", err)
	}
	dict := obuf.String()
	if gzipFiles && (fileio.DetermineType(dict) != fileio.Gzip) {
		dict += fileio.FileSuffix(fileio.Gzip)
	}
	return dict
}

func colPathnames(tpl *template.Template, colIDs []symbol.ID, gzipFiles bool) []string {
	pathnames := make([]string, len(colIDs))
	for ci, colID := range colIDs {
		obuf := &strings.Builder{}
		col := colID.Str()
		name := strings.Replace(col, "\t\n\v\f\r", "_", -1)
		if err := tpl.Execute(obuf, columnarTemplate{name, ci}); err != nil {
			log.Panicf("failed to execute template for column: %v: %v", colID.Str(), err)

		}
		pathnames[ci] = obuf.String()
		if gzipFiles && (fileio.DetermineType(pathnames[ci]) != fileio.Gzip) {
			pathnames[ci] += fileio.FileSuffix(fileio.Gzip)
		}
	}
	return pathnames
}

// columnarTSVWriter is an implementation of tsvWriter.  It creates a
// single-column TSV file for every column.
type columnarTSVWriter struct {
	ast ASTNode // source code location
	// colIDs    []symbol.ID         // list of columns
	colMap tsvColumnMap        // inverse colID -> colindex mappings
	w      []*defaultTSVWriter // writer per column. len(w)==len(colIDs)
	ch     []chan []Value      // for handing off values to writers. len(ch)==len(colIDs).
	buf    [][]Value           // accumulates values to write. len(buf)==len(colIDs).
	found  []bool              // is non-null value found for a column in the current row?
	next   int
	wg     sync.WaitGroup // synchronizes the writers.
}

// Append implements tsvWriter
func (w *columnarTSVWriter) Append(v Value) {
	const batchSize = 8192
	row := v.Struct(w.ast)
	for i := 0; i < row.Len(); i++ {
		f := row.Field(i)
		// Map field index to column index for the table as a whole.
		colIdx := w.colMap.lookupByID(f.Name)
		if colIdx < 0 || w.found[colIdx] {
			log.Panicf("looks like a column %v was used more than once", f.Name)
		}
		w.found[colIdx] = true
		w.buf[colIdx] = append(w.buf[colIdx], f.Value)
	}
	for i, c := range w.found {
		if !c {
			w.buf[i] = append(w.buf[i], Null)
		}
		w.found[i] = false
	}
	w.next++
	if w.next == batchSize {
		for i := range w.buf {
			w.ch[i] <- w.buf[i]
			w.buf[i] = make([]Value, 0, batchSize)
		}
		w.next = 0
	}
}

// Close implements tsvWriter
func (w *columnarTSVWriter) Close() {
	if w.next > 0 {
		for i := range w.buf {
			w.ch[i] <- w.buf[i]
		}
	}
	for _, ch := range w.ch {
		close(ch)
	}
	w.wg.Wait()
	traverse.Each(len(w.w), func(shard int) error { // nolint: errcheck
		w.w[shard].Close()
		return nil
	})
}

// Discard implements tsvWriter
func (w *columnarTSVWriter) Discard() {
	for _, ch := range w.ch {
		close(ch)
	}
	w.wg.Wait()
	traverse.Each(len(w.w), func(shard int) error { // nolint: errcheck
		w.w[shard].Discard()
		return nil
	})
}

func newColumnarTSVWriter(ctx context.Context, paths []string, colIDs []symbol.ID, gzipFiles bool) *columnarTSVWriter {
	w := &columnarTSVWriter{
		w:      make([]*defaultTSVWriter, len(colIDs)),
		colMap: newTSVColumnMap(colIDs),
		found:  make([]bool, len(colIDs)),
		ch:     make([]chan []Value, len(colIDs)),
		buf:    make([][]Value, len(colIDs)),
	}
	traverse.Each(len(colIDs), func(shard int) error { // nolint:errcheck
		w.w[shard] = newDefaultTSVWriter(ctx, paths[shard], []symbol.ID{colIDs[shard]}, true, gzipFiles)
		return nil
	})
	for ci := range colIDs {
		w.ch[ci] = make(chan []Value, 100)
		w.wg.Add(1)
		go func(shard int) {
			for rows := range w.ch[shard] {
				for _, row := range rows {
					w.w[shard].Append(row)
				}
			}
			w.wg.Done()
		}(ci)
	}
	return w
}

// WriteColumnarTSV writes the given table in 'columnar' format, that is,
// each column is written to a separate file.
// Prefix represents a path prefix (eg. an S3 prefix or a directory),
// and format is a template in go's text/template language that is used to
// generate the name of each of the columnar files. The template is executed
// executed with the following fields available:
//
//   Name   string  // Column header but with white space replaced by _
//   Number int     // 1..<num-cols>
//
// The first row of all of the files will contain the column header. If gzip is
// true then the output files will be gzip compressed and a .gz extension
// appended to all of the filenames.  If overwrite is false and the file "path"
// exists, this function returns quickly without overwriting the file.
func WriteColumnarTSV(ctx context.Context, table Table, format string, gzipFiles, overwrite bool) {
	tpl, err := template.New("WriteColumnar").Parse(format)
	if err != nil {
		log.Panicf("failed to parse template '%v': %v", format, err)
	}

	writeTSVHelper(ctx, func(colIDs []symbol.ID) tsvWriter {
		paths := colPathnames(tpl, colIDs, gzipFiles)
		fileExists := errors.New("exists")
		err := traverse.Each(len(paths), func(shard int) error {
			path := paths[shard]
			if _, err := file.Stat(ctx, path); err == nil {
				if !overwrite {
					return fileExists
				}
			}
			return nil
		})
		if err == nil {
			return newColumnarTSVWriter(ctx, paths, colIDs, gzipFiles)
		}
		// TODO(saito) Fix this codepath.
		log.Panic("writecol: non-overwrite mode not yet supported")
		return nil
	}, "", table, gzipFiles)
}

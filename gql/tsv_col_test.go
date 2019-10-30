package gql_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/grailbio/base/traverse"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/gqltest"
	"github.com/grailbio/gql/gqlutil"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/testutil/h"
)

var tsvColumnData = []string{
	`t1
0
10
20
`, `t2
1
11
21
`, `t3
2
13
22
`, `t4
NA
14
24
`,
}

func stripComments(buf []byte) string {
	out := bytes.NewBuffer(make([]byte, 0, 1024))
	sc := bufio.NewScanner(bytes.NewBuffer(buf))
	for sc.Scan() {
		line := sc.Text()
		if line[0] != '#' {
			out.WriteString(sc.Text())
			out.Write([]byte{'\n'})
		}
	}
	return out.String()
}

func TestColumnSharded(t *testing.T) {
	env := gqltest.NewSession()
	tmpDir, cleanup := testutil.TempDir(t, "", "colsharded-")
	defer cleanup()
	filenames := filepath.Join(tmpDir, "cols-{{.Name}}-{{.Number}}.ctsv")
	filenamesGZIP := filepath.Join(tmpDir, "cols-{{.Name}}-{{.Number}}.ctsv")
	gqltest.Eval(t, `t0 := table(
		{t1:0,t2:01,t3:02},
		{t1:10,t2:11,t3:13,t4:14},
		{t4:24,t1:20,t2:21,t3:22})`, env)
	gqltest.Eval(t,
		`t0 | writecols("`+filenames+`")`,
		env)
	gqltest.Eval(t,
		`t0 | writecols("`+filenamesGZIP+`", gzip:=true)`,
		env)

	files := []string{}
	err := filepath.Walk(tmpDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			files = append(files, info.Name())
			return nil
		})
	sort.Strings(files)
	assert.NoError(t, err)
	expect.That(t, files, h.ElementsAre(
		"cols-t1-0.ctsv", "cols-t1-0.ctsv.gz", "cols-t2-1.ctsv", "cols-t2-1.ctsv.gz", "cols-t3-2.ctsv", "cols-t3-2.ctsv.gz", "cols-t4-3.ctsv", "cols-t4-3.ctsv.gz"))
	for i, fn := range files {
		buf, err := ioutil.ReadFile(filepath.Join(tmpDir, fn))
		assert.NoError(t, err)
		if strings.HasSuffix(fn, ".gz") {
			rd, err := gzip.NewReader(bytes.NewBuffer(buf))
			assert.NoError(t, err)
			data, err := ioutil.ReadAll(rd)
			assert.NoError(t, err)
			buf = data
		}
		contents := stripComments(buf)
		expect.EQ(t, contents, tsvColumnData[i/2])
	}

}

func TestColumnShardedLarge(t *testing.T) {
	env := gqltest.NewSession()
	tmpDir, cleanup := testutil.TempDir(t, "", "colsharded-large-")
	defer cleanup()

	nRows := 100002
	colA := make([]int64, nRows)
	colB := make([]int64, nRows)
	colC := make([]int64, nRows)
	keyA := symbol.Intern("colA")
	keyB := symbol.Intern("colB")
	keyC := symbol.Intern("colC")

	for i := 0; i < nRows; i++ {
		colA[i] = rand.Int63()
		colB[i] = rand.Int63()
		colC[i] = rand.Int63()
	}

	row := make([]gql.StructField, 3)
	cleanupA := gqlutil.CreateAndRegisterTable(context.Background(), gqlutil.SimpleRAMTable, env, "large", nRows,
		func(i int) gql.Value {
			row[0] = gql.StructField{keyA, gql.NewInt(colA[i])}
			row[1] = gql.StructField{keyB, gql.NewInt(colB[i])}
			row[2] = gql.StructField{keyC, gql.NewInt(colC[i])}
			return gqlutil.RowAsValue(row)
		})
	defer cleanupA()
	gqltest.Eval(t, fmt.Sprintf(`large | writecols("%v")`, filepath.Join(tmpDir, "large-{{.Name}}.tsv")), env)

	readCol := func(name string) ([]int64, error) {
		rows := make([]int64, 0, nRows)
		filename := filepath.Join(tmpDir, "large-"+name+".tsv")
		file, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(file)
		sc.Scan()
		if got, want := sc.Text(), name; got != want {
			return nil, fmt.Errorf("got %v, want %v", got, want)
		}
		for sc.Scan() {
			v, err := strconv.ParseInt(sc.Text(), 10, 64)
			if err != nil {
				return nil, err
			}
			rows = append(rows, v)
		}
		return rows, nil
	}

	cols := []string{"colA", "colB", "colC"}
	rows := make([][]int64, 3)
	err := traverse.Each(3, func(i int) error {
		c, err := readCol(cols[i])
		rows[i] = c
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range [][]int64{colA, colB, colC} {
		expect.EQ(t, rows[i], v)
	}
}

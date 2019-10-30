package gqlutil_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/gqlutil"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/testutil"
)

var once sync.Once

func initGQL() {
	once.Do(func() {
		opts := gql.Opts{
			CacheDir: testutil.GetTmpDir(),
		}
		gql.Init(opts)
	})
}

func ExampleIndexedTable() {
	initGQL()
	colA := []int64{0, 1, 2, 3}
	colB := []string{"A", "B", "C", "D"}
	keyA := symbol.Intern("colA")
	keyB := symbol.Intern("colB")
	sess := gql.NewSession()
	ctx := context.Background()
	row := make([]gql.StructField, 2)
	cleanup := gqlutil.CreateAndRegisterTable(context.Background(), gqlutil.SimpleRAMTable, sess, "example", len(colA),
		func(i int) gql.Value {
			row[0] = gql.StructField{keyA, gql.NewInt(colA[i])}
			row[1] = gql.StructField{keyB, gql.NewString(colB[i])}
			return gqlutil.RowAsValue(row)
		})
	td, _ := ioutil.TempDir("", "example-")
	defer os.RemoveAll(td)
	fn := filepath.Join(td, "eg.tsv")
	parsed, _ := sess.Parse("", []byte(fmt.Sprintf(`example | write("%v")`, fn)))
	sess.EvalStatements(ctx, parsed)
	data, _ := ioutil.ReadFile(fn)
	if err := cleanup(); err != nil {
		panic(err)
	}
	fmt.Println(string(data))
	// Output:  colA	colB
	// 0	A
	// 1	B
	// 2	C
	// 3	D
}

func TestTableBuilder(t *testing.T) {
	initGQL()
	sess := gql.NewSession()
	ctx := context.Background()
	colA := make([]int64, 100)
	colB := make([]string, 100)
	colC := make([]bool, 100)
	keyA := symbol.Intern("colA")
	keyB := symbol.Intern("colB")
	keyC := symbol.Intern("colC")
	for i := range colA {
		colA[i] = int64(i)
		colB[i] = fmt.Sprintf("%03d", i)
		colC[i] = i%2 == 0
	}

	validateTable := func(name string) {
		parsed, err := sess.Parse("testTableBuilder", []byte(name+"| filter($colC, shards:=3);"))
		if err != nil {
			t.Fatal(err)
		}
		val := sess.EvalStatements(ctx, parsed)
		if got, want := val.Type(), gql.TableType; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		tbl := val.Table(nil)
		sc := tbl.Scanner(ctx, 0, 1, 1)
		n := 0
		for sc.Scan() {
			v := sc.Value()
			if got, want := v.String(), fmt.Sprintf("{colA:%v,colB:%03d,colC:true}", n*2, n*2); got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			n++
		}
		if got, want := n, 50; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	row := make([]gql.StructField, 3)
	cleanupA := gqlutil.CreateAndRegisterTable(ctx, gqlutil.SimpleRAMTable, sess, "example_ram", len(colA),
		func(i int) gql.Value {
			row[0] = gql.StructField{keyA, gql.NewInt(colA[i])}
			row[1] = gql.StructField{keyB, gql.NewString(colB[i])}
			row[2] = gql.StructField{keyC, gql.NewBool(colC[i])}
			return gqlutil.RowAsValue(row)
		})
	defer cleanupA()

	cleanupB := gqlutil.CreateAndRegisterTable(ctx, gqlutil.BTSVDiskTable, sess, "example_disk", len(colA),
		func(i int) gql.Value {
			row[0] = gql.StructField{keyA, gql.NewInt(colA[i])}
			row[1] = gql.StructField{keyB, gql.NewString(colB[i])}
			row[2] = gql.StructField{keyC, gql.NewBool(colC[i])}
			return gqlutil.RowAsValue(row)
		})
	defer cleanupB()

	validateTable("example_ram")
	validateTable("example_disk")

}

# Grail query language

GQL is a bioinformatics query language. You can think of it as an SQL with a funny
syntax.

- It can read and write files on S3 directly.

- It can read common bioinformatics files, such as TSV, BAM, BED as tables.

- It can handle arbitrarily large files regardless of the memory capacity of the
  machine. All data processing happens in a streaming fashion.

- It supports [distributed execution](#distributed-execution) of key functions
  using [bigslice](https://bigslice.io).

- GQL language syntax is very different from SQL, but if you squint enough, you
  can see the correspondence with SQL. GQL syntax differs from SQL for several
  reasons. First, GQL needs to support hierarchical information. For example,
  sequencing or quality data in a BAM record is treated as a subtable inside a
  parent BAM table.  SQL handles such queries poorly - for example, SQL cannot
  read tables whose names are written in a column in another table. Second, some
  GQL functions, such as `transpose`, have no corresponding SQL counterpart.

## Installing GQL

    go install github.com/grailbio/gql

## Running GQL

If you invoke `gql` without argument, it starts an interactive prompt,
`gql>`.  Below are list of interactive commands:

- `help` : shows a help message. You can also invoke 'help name', where _name_
  is a function or a variable name to show the description of the name or the
  variable. For example, "help map" will show help message for `map` builtin function.

- `logdir [dir]` : sends log messages to files under the given directory.
  If the directory is omitted, messages will be sent to stderr.

- `quit` : quits gql

- Any other command will be evaluated as an GQL expression.  In an interactive
  mode, a newline will start evaluation, so an expression must fit in one line.

You can also invoke GQL with a file containing gql statements.  Directory
"scripts" contains simple examples.

When evaluating GQL in a script, an expression can span multiple lines. An
expression is terminated by ';' or EOF.

You can pass a sequence of `-flag=value` or just `-flag` after the script name.
The `flag` will become a global variable and will be accessible as `flag` in the
script.  In the above example, we pass two flags "in" and "out" to the script
`testdata/convert.gql`.

    gql testdata/convert.gql -in=/tmp/test.tsv -out=/tmp/test.btsv

### Basic functions


#### Reading a table

Imagine you have the following TSV file, `gql/testdata/file0.tsv`:

        A	B	C
        10	ab0	cd0
        11	ab1	cd1


GQL treats a tsv file as a 2D table.  Function [read](#read) loads a TSV
file.

    read(`gql/testdata/file0.tsv`) 


| #|  A|   B|   C|
|--|---|----|----|
| 0| 10| ab0| cd0|
| 1| 11| ab1| cd1|



The first column, '#' is not a real column. It just shows the row number.

#### Selection and projection using `filter` and `map`

Operator "|" feeds contents of a table to a function.  [filter](#filter) is a
selection function. It picks rows that matches `expr` and combines them into a
new table.  Within `expr`, you can refer to a column by prefixing `&`.  `&A`
means "value of the column `A` in the current row".
"var := expr" assigns the value of expr to the variable.

    f0 := read(`gql/testdata/file0.tsv`);
    f0 | filter(&A==10)


| #|  A|   B|   C|
|--|---|----|----|
| 0| 10| ab0| cd0|



> Note: `&A==10` is a shorthand for `|_| _.A==10`, which is a function that takes
 argument `_` and computes `_.A==10`. GQL syntactically translates an expression
 that contains '&' into a function. So you can write the above example as
 `f0 | filter(|_|{_.A==10})`. It produces the same result. We will discuss functions
 and '&'-expansions in more detail [later](#functions).

> Note: Expression `table | filter(...)` can be also written as `filter(table, ...)`.
 These two are exactly the same, but the former is usually easier to
 read.  This syntax applies to any GQL function that takes a table as the first
 argument: `A(x) | B(y)` is the same as `B(A(x), y)`. Thus:

    f0 | filter(&A==11)


| #|  A|   B|   C|
|--|---|----|----|
| 0| 11| ab1| cd1|



Function [map](#map) projects a table.

    f0 | map({&A, &C})


| #|  A|   C|
|--|---|----|
| 0| 10| cd0|
| 1| 11| cd1|



In fact, `map` and `filter` are internally one function that accepts different
kinds of arguments. You can combine projection and selection in one function,
like below:

    f0 | map({&A, &C}, filter:=string_has_suffix(&C, "0"))


| #|  A|   C|
|--|---|----|
| 0| 10| cd0|




    f0 | filter(string_has_suffix(&C, "0"), map:={&A, &C})


| #|  A|   C|
|--|---|----|
| 0| 10| cd0|



#### Sorting

Function [sort](#sort) sorts the table by ascending order of the argument. Giving "-&A"
as the sort key will sort the table in descending order of column A:

    f0 | sort(-&A)


| #|  A|   B|   C|
|--|---|----|----|
| 0| 11| ab1| cd1|
| 1| 10| ab0| cd0|



The sort key can be an arbitrary expression. For example, `f0 | sort(&C+&B)` will
sort rows by ascending order of the concatenation of columns C and B.
`f0 | sort(&C+&B)` will produce the same table as `f0` itself.

#### Joining multiple tables

Imagine file `gql/testdata/file1.tsv` with the following contents.

        C	D	E
        10	ef0	ef1
        12	gh0	gh1


Function [join](#join) joins multiple tables into one table.

    f1 := read(`gql/testdata/file1.tsv`);
    join({f0, f1}, f0.A==f1.C, map:={A:f0.A, B:f0.B, C: f0.C, D:f1.D})


| #|  A|   B|   C|   D|
|--|---|----|----|----|
| 0| 10| ab0| cd0| ef0|



The first argument to join is the list of tables to join. It is of the form
`{name0: table0, ..., nameN, tableN}`. Tag `nameK` can be used to name row
values for `tableK` in the rest of the join expression. If you omit the `nameI:`
part of the table list, join tries to guess a reasonable name from the column
value.

NOTE: See [struct comprehension](#struct-comprehension) for more details
about how column names are computed when they are omitted.

The second argument is the join condition.  The optional 'map:=rowspec' argument
specifies the list of columns in the final result.  Join will produce Cartesian
products of rows in the two tables, then create a new table consisting of the
results that satisfy the given join condition. For good performance, the join
condition should be a conjunction of column equality constraints (`table0.col0 ==
table1.col0 && ... && table1.col2 == table2.col2`). Join recognizes this form of
conditions and executes the operation more efficiently.


#### Cogroup and reduce

Imagine a file 'gql/testdata/file2.tsv' like below:

        A	B
        cat	1
        dog	2
        cat	3
        bat	4


Function [cogroup](#cogroup) is a special kind of join, similar to "select ... group by"
in SQL. Cogroup aggregates rows in a table by a column.  The result is a
two-column table where the "key" column is the aggregation key, and "value"
column is a subtable that stores the rows wit the given key.

    read(`gql/testdata/file2.tsv`) | cogroup(&A)


| #| key|                     value|
|--|----|--------------------------|
| 0| bat|             [{A:bat,B:4}]|
| 1| cat| [{A:cat,B:1},{A:cat,B:3}]|
| 2| dog|             [{A:dog,B:2}]|



Column '[{A:cat,B:1},{A:cat,B:3}]` is a shorthand notation for
the following table:

| A   | B |
|-----|---|
| cat | 1 |
| cat | 3 |


Function [reduce](#reduce) is similar to [cogroup](#cogroup), but it applies a user-defined
function over rows with the same key. The function must be commutative. The
result is a two-column table with nested columns. Reduce is generally faster
than cogroup.

    read(`gql/testdata/file2.tsv`) | reduce(&A, |a,b|(a+b), map:=&B)


| #| key| value|
|--|----|------|
| 0| cat|     4|
| 1| dog|     2|
| 2| bat|     4|



Functions such as `cogroup` and `reduce` exist because it is much amenable for
distributed execution compared to generic joins. We explain distributed
execution in a [later section](#distributed-execution)


### Distributed execution

Functions `map`, `reduce`, `cogroup`, `sort`, and `minn` accept argument
`shards`. When set, it causes the functions to execute in parallel.  By default
they will run on the local machine, using multiple CPUs. Invoking GQL with the
following flags will cause these functions to use AWS EC2 spot instances.

    gql scripts/cpg-frequency.gql

To set bigslice up, follow the instructions in https://bigslice.io.

## GQL implementation overview

GQL is a dataflow language, much like other SQL variants. Each table, be it a
leaf table created by `read` and other functions, or an intermediate table
created by functions like `map`, `join` is becomes a dataflow node, and rows
flow between nodes. Consider a simple example for file foo.tsv:

     read(`foo.tsv`) | filter(&A > 10) | sort(-&B) | write(`blah.tsv`)

where foo.tsv is something like below:

         A      B
         10     ab0
         11     ab1

GQL creates four nodes connected sequentially.

     read(`foo.tsv`) ---> filter(&A > 10) ----> sort(-&B) ----> write(`blah.tsv`)

A dataflow graph is driven from the tail. In this example, `write` pulls rows
from `sort`, which pulls rows from `filter`, and so on. Tables are materialized
only when necessary. `read` and `filter` just pass through rows one by one,
whereas `sort` needs to read all the rows from the source and materialize them
on disk.

       f0 := read(`f0.tsv`); f1 := read(`f1.tsv`); f2 := read(`f2.tsv`)
       join({f0, f1, f2}, f0.A==f1.A && f1.C==f2.C)

`Join` function merges nodes.

     read(`f0.tsv`) --> sort(f0.A) \
                                    merge(f0.A==f1.A) ---> sort(f1.C) \
     read(`f1.tsv`) --> sort(f1.A) /                                   merge(f1.C==f2.C)
                                                                      /
     read(`f2.tsv`) ----------------------------------> sort(f2.C)   /


A function that performs bigslice-based distributed execution ships the code
to remote machines. For example:

       read(`s3://bucket/f.tsv`) | cogroup({&A}, shards:=1024)

The expression "read(`s3://bucket/f.tsv`)" along with all the variable bindings
needed to run the expression is serialized and shipped to every bigslice shard.

## GQL syntax

    toplevelstatements := (toplevelstatement ';')* toplevelstatement ';'?

    toplevelstatement :=
      'load' path
      | expr
      | assignment
      | 'func' symbol '(' params* ')' expr // shorthand for symbol:=|symbol...| expr

    assignment := variable ':=' expr

    expr :=
      symbol                  // variable reference
      | literal               // 10, 5.5, "str", 'x', etc
      | expr '(' params* ')'  // function call
      | expr '|' expr         // data piping
      | expr '||' expr        // logical or
      | expr '&&' expr        // logical and
      | 'if' expr expr 'else' expr // conditional
      | 'if' expr expr 'else' 'if' expr 'else' ... // if .. else if .. else if .. else ..
      | '{' structfield, ..., structfield '}'
      | expr '.' colname
      | block
      | funcdef

    structfield := expr | colname ':' expr
    literal := int | float | string | date | 'NA'
    string := "foo" | `foo`
    date := iso8601 format literal, either 2018-03-01 or 2018-03-01T15:40:41Z or 2018-03-01T15:40:41-7:00

    funcdef := '|' params* '|' expr
    block := '{' (assignment ';')* expr '}'

'?' means optional, '*' means zero or more repetitions, and (...) means grouping of parts.

## Data types

### Table

When you read a TSV file, GQL internally turns it into a *Table*. Table contains
a sequence of *rows*, or *structs*.  We use terms structs and rows
interchangeably.  Tables are created by loading a TSV or other table-like files
(*.prio, *.bed, etc). It is also created as a result of function invocation
(`filter`, `join`, etc).  Invoking `read("foo.tsv")` creates a table from file
"foo.tsv". Function `write(table, "foo.tsv")` saves the contents of the table in
the given file.  The `read` function supports the following file formats:

  - *.tsv : Tab-separated-value file. The list of columns must be listed in the
  first row of the file. GQL tries to guess the column type from the file
  contents.

  - .prio : [Fragment file](https://sg.eng.grail.com/grail/grail/-/blob/go/src/grail.com/bio/fragments/f.go). Each fragment is mapped into a row of the following format:

| Column name | type     |
|-------------|----------|
| reference   | string   |
| start       | int   |
| length      | int   |
| plusstrandread       | int   |
| duplicate       | bool   |
| corgcount       | int   |
| pvalue       | float   |
| methylationstates       | array table of integers |
| methylationcoverage | array table |
| methylationbasequalitiesread1 | array table of integers |
| methylationbasequalitiesread2 | array table of integers |

An array table is a table with two columns, 'position' and 'value'. The position
column stores the position of the given value, relative to the reference.  For
example, imagine that the original fragment object has following fields:

```
    frag := F{
      Reference: "chr11",
      FirstCpG: 12345,
      MethylationStates: []MethylationState{UNMETHYLATED, METHYLATED, VARIANT},
      ...
    }
```

Then, the corresponding GQL representation of methylationstates will be

```
    table({position: 12345, value: 1},
          {position: 12346, value: 2},
          {position: 12347, value: 4})
```

Note: In `fragment.proto`, UNMETHYLATED=1, METHYLATED=2, VARIANT=4.

  - .bam, .pam : [BAM](https://samtools.github.io/hts-specs/SAMv1.pdf) or [PAM](https://github.com/grailbio/bio/blob/master/encoding/pam/README.md) file

  - .bed: [BED file](https://genome.ucsc.edu/FAQ/FAQformat.html), three to four columns

  - .bincount : bincount file. Each row has the following columns:

| Column name | type     |
|-------------|----------|
| chrom   | string   |
| start       | int   |
| end      | int   |
| gc       | int   |
| count       | int   |
| length       | int   |
| density       | float   |

  - .cpg_index : CpG index file

  - .btsv : BTSV is a binary version of TSV. It is a format internally used by GQL to
    save and restore table contents.

The `write` function currently supports `*.tsv` and `*.btsv` file types. If
possible, save the data in *.btsv format. It is faster and more compact.

Unlike SQL, rows in a table need not be homogeneous - they can technically have
different sets of columns. Such a table can be created, for example, by
flattening tables with inconsistent schemata. We don't recommend creating such
tables though.

### Row (struct)

Struct represents a row in a table. We also use the term "row" to means the same
thing. We use term "column" or "field" to refer to an individual value in a row.
A column usually stores a scalar value, such as an integer or a string.  But a
column may store a another table. This happens when a column the pathname of
another TSV file (or bincounts, BAM, etc). GQL automatically creates a subtable
for such columns.

Several builtin functions are provided to manipulate rows. They are described in
more detail later in this document.

  - `{col1:expr1, col2:expr2, ..., colN:exprN}` creates a new row with N columns
    named `col1`, `col2`, ..., `colN`. See
    [struct comprehension](#struct-comprehension) for more details.

  - `pick(table, expr)` extracts the first row that satisfies expr from a table


### Scalar values

GQL supports the following scalar types.

  - Integer (int64)

  - Floating point (float64)

  - String

    Strings are enclosed in either with "doublequotes" or \`backquotes\`, like in
    Go.  Note that singlequotes are not supported.

  - Filename: filename is a string that refers to another file.  A filename
    contains a pathname that looks like a table (*.tsv, *.prio, *.bed, etc) are
    automatically opened as a subtable.

  - Char: utf8 character. 'x', 'y'.

  - DateTime (date & time of day)

  - Date  (date without a time-of-day component)

  - Time  (time of day)

    Date, DateTime, and Time are written in
    [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) format.
    Below are samples:

        2018-04-16
        2018-04-16T15:19:35Z
        2018-04-16T15:19:35-7:00
        15:19:35Z
        15:19:35-8:00

  - Null Null is a special value indicating "value isn't there'.  A TSV cell
    with value "NA", "null", or "" becomes a Null in GQL. Symbol "NA" also
    produce null.

    In GQL, a null value is treated as ∞. Thus:

        1 < NA
        1 > -NA
        "foo" < NA
        "foo" > -NA

## Control-flow expressions

    expr0 || expr1
    expr0 && expr1
    if expr0 expr1 else expr2
    if expr0 expr1 else if expr2 expr3 else ...

These expressions have the similar grammars and meanings to Go's.
The 'if' construct is slightly different:

- The then and else parts are expressions, and they need not be enclosed in
  '{...}'.

- The 'else' part is required.

- 'If' is an expression. Its value is that of the 'then' expression if the
  condition is true, and that of the 'else' expression otherwise. In the below example, the value of y is "bar".

      x := 10;
      y := if x > 10 "foo" else "bar"

## Code blocks

      { assignments... expr }

An expression of form `{ assignments... expr }` introduces local variables.  The
following example computes 110 (= 10 * (10+1)). The variables "x" and "y" are
valid only inside the block.

      { x := 10; y := x+1; x * y }

A block is often used as a body of a function, which we describe next.

## Functions

An expression of form `|args...| expr` creates a function.  It can be assigned
to a variable and invoked later. Consider the following example:
      udf := |row| row.date >= 2018-03-05;
      read(`foo.tsv`) | filter(|row|udf(row))

It is the same as

      read(`foo.tsv`) | filter(&date >= 2018-03-05)

GQL also provides syntax sugar

      func udf(row) row.date >= 2018-03-05

It is the same as `udf := |row| row.date >= 2018-03-05`

The function body is often a [code block](#code-blocks).

      func ff(arg) {
         x := arg + 1;
         y := x * x
         y * 3
      };
      ff(4)

The above example will print 75. The variables x and y defined in the body of
`ff` are local to the function invocation, just like in regular Go functions.

> Note: function arguments are lexically bound. Functions can be nested, and they
act as a closure.

The '&'-expressions introduced in [earlier examples](#basic-functions) are
syntax sugar for user-defined functions. It is translated into a
[function](#functions) by the GQL parser. The translation rules are the
following:

- '&'-translation applies only to a function-call argument. It is an error for
  '&' to appear anywhere else.

- When a function-call arg contains '&' anywhere, then the entire argument is
  translated into a function with formal argument named '_', and every
  occurrences of form '&col' is rewritten to become '_.col'.

Original:      table | map({x:&col0, y:&col1})
After rewrite: table | map(|_|{x:_.col0, y:_.col1})

Original:      table | map({x:&col0, y:&col1}) | sort(&x)
After rewrite: table | map(|_|{x:_.col0, y:_.col1}) | sort(|_|_.x)


The '&' rule applies recursively to the entire argument, so it may behave
nonintuitively if '&' appears inside a nested function call. Consider the
following example, which is a bit confusing.

Original:      table | map(map(&col))
After rewrite: table | map(|_|map(_.col))

So we recommend using '&' only for simple expressions like the earlier examples.
For nested maps and other complex examples, it's cleaner to use an explicit
function syntax of form `|args...|expr`.

## Deprecated GQL constructs

The following expressions are deprecated and will be removed soon.

- '$var"

"$var" is very similar to "&var". Expression `table | filter($A==10)` will act
the same as `table | filter($A==10)`. The difference is that '$'-rule is
hard-coded into a few specific builtin functions, such as `map`, `filter`, and
`sort`, whereas '&' is implemented as a generic language rule.

- func(args...) { statements... }

This is an old-style function syntax. Use `|args...| expr` instead.  The
difference between the two is that 'func` syntax requires '{' and '}' even if
the body is a single expression. The '|args...|' form takes '{...}' only when
the body is a [code block](#code-blocks).

## Importing a GQL file

The load statement can be used to load a gql into another gql file.

Assume file `file1.gql` has the following contents:

       x := 10

Assume file `file2.gql` has the following contents:

       load `file1.gql`
       x * 2

If you evaluate file2.gql, it will print "20".

If a gql file can contain multiple load statements.  The load statement must
appear before any other statement.

## Builtin functions

### Table manipulation

#### map


    _tbl | map(expr[, expr, expr, ...] [, filter:=filterexpr] [, shards:=nshards])

Arg types:

- _expr_: one-arg function
- _filterexpr_: one-arg boolean function (default: ::|_|true::)
_ _nshards_: int (default: 0)

Map picks rows that match _filterexpr_ from _tbl_, then applies _expr_ to each
matched row.  If there are multiple _expr_s, the resulting table will apply each
of the expression to every matched row in _tbl_ and combine them in the output.

If _filterexpr_ is omitted, it will match any row.
If _nshards_ > 0, it enables distributed execution.
See the [distributed execution](#distributed-execution) section for more details.

Example: Imagine table ⟪t0⟫ with following contents:

|col0 | col1|
|-----|-----|
|Cat  | 3   |
|Dog  | 8   |

    t0 | map({f0:&col0+&col0, f1:&col1*&col1})

will produce the following table

|f0      | f1   |
|--------|------|
|CatCat  | 9    |
|DogDog  | 64   |

The above example is the same as below.

    t0 | map(|r|{f0:r.col0+r.col0, f1:r.col1*r.col1}).


The next example

    t0 | map({f0:&col0+&col0}, {f0:&col0}, filter:=&col1>4)

will produce the following table

|f0      |
|--------|
|DogDog  |
|Dog     |



#### filter


    tbl | filter(expr [,map:=mapexpr] [,shards:=nshards])

Arg types:

- _expr_: one-arg boolean function
- _mapexpr_: one-arg function (default: ::|row|row::)
- _nshards_: int (default: 0)

Functions [map](#map) and filter are actually the same functions, with slightly
different syntaxes.  ::tbl|filter(expr, map=mapexpr):: is the same as
::tbl|map(mapexpr, filter:=expr)::.


#### reduce


    tbl | reduce(keyexpr, reduceexpr [,map:=mapexpr] [,shards:=nshards])

Arg types:

- _keyexpr_: one-arg function
- _reduceexpr_: two-arg function
- _mapexpr_: one-arg function (default: ::|row|row::)
- _nshards_: int (default: 0)

Reduce groups rows by their _keyexpr_ value. It then invokes _reduceexpr_ for
rows with the same key.

Argument _reduceexpr_ is invoked repeatedly to combine rows or values with the same key.

  - The optional 'map' argument specifies argument to _reduceexpr_.
    The default value (identity function) is virtually never a good function, so
    You should always specify a _mapexpr_ arg.

  - _reduceexpr_must produce a value of the same type as the input args.

  - The _reduceexpr_ must be a commutative expression, since the values are
    passed to _reduceexpr_ in an specified order. If you want to preserve the
    ordering of values in the original table, use the [cogroup](#cogroup)
    function instead.

  - If the source table contains only one row for particular key, the
    _reduceexpr_ is not invoked. The 'value' column of the resulting table will
    the row itself, or the value of the _mapexpr_, if the 'map' arg is set.

If _nshards_ >0, it enables distributed execution.
See the [distributed execution](#distributed-execution) section for more details.

Example: Imagine table ::t0:::

|col0 | col1|
|-----|-----|
|Bat  |  3  |
|Bat  |  4  |
|Bat  |  1  |
|Cat  |  4  |
|Cat  |  8  |

::t0 | reduce(&col0, |a,b|a+b, map:=&col1):: will create the following table:

|key  | value|
|-----|------|
|Bat  | 8    |
|Cat  | 12   |

::t0 | reduce(&col0, |a,b|a+b, map:=1):: will count the occurrences of col0 values:

|key  | value|
|-----|------|
|Bat  | 3    |
|Cat  | 2    |


A slightly silly example, ::t0 | reduce(&col0, |a,b|a+b, map:=&col1*2):: will
produce the following table.

|key  | value|
|-----|------|
|Bat  | 16   |
|Cat  | 24   |

> Note: ::t0| reduce(t0, &col0, |a,b|a+b.col1):: looks to be the same as
::t0 | reduce(&col0, |a,b|a+b, map:=&col1)::, but the former is an error. The result of the
_reduceexpr_ must be of the same type as the inputs. For this reason, you
should always specify a _mapexpr_.


#### flatten


    flatten(tbl0, tbl1, ..., tblN [,subshard:=subshardarg])

Arg types:

- _tbl0_, _tbl1_, ... : table
- _subshardarg_: boolean (default: false)

::tbl | flatten():: (or ::flatten(tbl)::) creates a new table that concatenates the rows of the subtables.
Each table _tbl0_, _tbl1_, .. must be a single-column table where each row is a
another table. Imagine two tables ::table0:: and ::table1:::

table0:

|col0 | col1|
|-----|-----|
|Cat  | 10  |
|Dog  | 20  |

table1:

|col0 | col1|
|-----|-----|
|Bat  | 3   |
|Pig  | 8   |

Then ::flatten(table(table0, table1)):: produces the following table

|col0 | col1|
|-----|-----|
|Cat  | 10  |
|Dog  | 20  |
|Bat  | 3   |
|Pig  | 8   |

::flatten(tbl0, ..., tblN):: is equivalent to
::flatten(table(flatten(tbl0), ..., flatten(tblN)))::.
That is, it flattens each of _tbl0_, ..., _tblN_, then
concatenates their rows into one table.

Parameter _subshard_ specifies how the flattened table is sharded, when it is used
as an input to distributed ::map:: or ::reduce::. When _subshard_ is false (default),
then ::flatten:: simply shards rows in the input tables (_tbl0_, _tbl1_ ,..., _tblN_). This works
fine if the number of rows in the input tables are much larger than the shard count.

When ::subshard=true::, then flatten will to shard the individual subtables
contained in the input tables (_tbl0_, _tbl1_,...,_tblN_). This mode will work better
when the input tables contain a small number (~1000s) of rows, but each
subtable can be very large. The downside of subsharding is that the flatten
implementation must read all the rows in the input tables beforehand to figure
out their size distribution. So it can be very expensive when input tables
contains many rows.


#### concat


    concat(tbl...)

Arg types:

- _tbl_: table

::concat(tbl1, tbl2, ..., tblN):: concatenates the rows of tables _tbl1_, ..., _tblN_
into a new table. Concat differs from flatten in that it attempts to maintain
simple tables simple: that is, tables that are backed by (in-memory) values
are retained as in-memory values; thus concat is designed to build up small(er)
table values, e.g., in a map or reduce operation.



#### cogroup


    tbl | cogroup(keyexpr [,mapexpr=mapexpr] [,shards=nshards])

Arg types:

- _keyexpr_: one-arg function
- _mapexpr_: one-arg function (default: ::|row|row::)
- _nshards_: int (default: 1)

Cogroup groups rows by their _keyexpr_ value.  It is the same as Apache Pig's
reduce function. It achieves an effect similar to SQL's "GROUP BY" statement.

Argument _keyexpr_ is any expression that can be computed from row contents. For
each unique key as computed by _keyexpr_, cogroup emits a two-column row of form

    {key: keyvalue, value: rows}

where _keyvalue_ is the value of keyexpr, and _rows_ is a table containing all
the rows in tbl with the given key.

If argument _mapexpr_ is set, the _value_ column of the output will be the
result of applying the _mapexpr_.

Example: Imagine table t0:

|col0 | col1|
|-----|-----|
|Bat  |  3  |
|Bat  |  1  |
|Cat  |  4  |
|Bat  |  4  |
|Cat  |  8  |

::t0 | cogroup(&col0):: will create the following table:

|key  | value|
|-----|------|
|Bat  | tmp1 |
|Cat  | tmp2 |

where table tmp1 is as below:

|col0 | col1|
|-----|-----|
|Bat  |  3  |
|Bat  |  1  |
|Bat  |  4  |

table tmp2 is as below:

|col0 | col1|
|-----|-----|
|Cat  |  4  |
|Cat  |  8  |

::t0 | cogroup(&col0, map:=&col1):: will create the following table:

|key  | value|
|-----|------|
|Bat  | tmp3 |
|Cat  | tmp4 |

Each row in table tmp1 is a scalar, as below

|  3  |
|  1  |
|  4  |

Similarly, table tmp2 looks like below.

|  4  |
|  8  |

The cogroup function always uses bigslice for execution.  The _shards_ parameter
defines parallelism. See the "distributed execution" section for more details.


#### firstn


    tbl | firstn(n)

Arg types:

- _n_: int

Firstn produces a table that contains the first _n_ rows of the input table.


#### minn


    tbl | minn(n, keyexpr [, shards:=nshards])

Arg types:

- _n_: int
- _keyexpr_: one-arg function
- _nshards_: int (default: 0)

Minn picks _n_ rows that stores the _n_ smallest _keyexpr_ values. If _n_<0, minn sorts
the entire input table.  Keys are compared lexicographically.
Note that we also have a
::sort(keyexpr, shards:=nshards):: function that's equivalent to ::minn(-1, keyexpr, shards:=nshards)::

The _nshards_ arg enables distributed execution.
See the [distributed execution](#distributed-execution) section for more details.

Example: Imagine table t0:

|col0 | col1| col2|
|-----|-----|-----|
|Bat  |  3  | abc |
|Bat  |  4  | cde |
|Cat  |  4  | efg |
|Cat  |  8  | ghi |

::minn(t0, 2, -&col1):: will create

|col0 | col1| col2|
|-----|-----|-----|
|Cat  |  8  | ghi |
|Cat  |  4  | efg |

::minn(t0, -&col0):: will create


|col0 | col1| col2|
|-----|-----|-----|
|Cat  |  4  | efg |
|Cat  |  8  | ghi |

You can sort using multiple keys using {}. For example,
::t0 | minn(10000, {&col0,-&col2}):: will sort two rows first by col0, then by -col2 in case of a
tie.

|col0 | col1| col2|
|-----|-----|-----|
|Cat  |  8  | ghi |
|Cat  |  4  | efg |
|Bat  |  4  | cde |
|Bat  |  3  | abc |



#### sort


    tbl | sort(sortexpr [, shards:=nshards])

::tbl | sort(expr):: is a shorthand for ::tbl | minn(-1, expr)::

#### join


    join({t0:tbl0,t1:tbl1,t2:tbl2}, t0.colA==t1.colB && t1.colB == t2.colC [, map:={colx:t0.colA, coly:t2.colC}])

Arg types:

- _tbl0_, _tbl1_, ..: table

Join function joins multiple tables into one. The first argument lists the table
name and its mnemonic in a struct form. The 2nd arg is the join condition.
The ::map:: arg specifies the format of the output rows.

Imagine the following tables:

table0:

|colA | colB|
|-----|-----|
|Cat  | 3   |
|Dog  | 8   |

table1:

|colA | colC|
|-----|-----|
|Cat  | red |
|Bat  | blue|


Example:

1. ::join({t0:table0, t1:table1}, t0.colA==t1.colA, map:={colA:t0.colA, colB: t0.colB, colC: t1.colC})::

This expression performs an inner join of t0 and t1.

|colA | colB| colC|
|-----|-----|-----|
|Cat  | 3   | red |


2. ::join({t0:table0, t1:table1}, t0.A?==?t1.A,map:={A:t0.A, A2:t1.A,B:t0.B, c:t1.C})::

This expression performs an outer join of t0 and t1.

|   A|  A2|  B|    c|
|----|----|---|-----|
|  NA| bat| NA| blue|
| cat| cat|  3|  red|
| dog|  NA|  8|   NA|


The join condition doesn't need to be just "=="s connected by "&&"s. It can be
any expression, although join provides a special fast-execution path for flat,
conjunctive "=="s, so use them as much as possible.

Caution: join currently is very slow on large tables. Talk to ysaito if you see
any problem.


TODO: describe left/right joins (use ==?, ?==)
TODO: describe cross joins (set non-equality join conditions, such as t0.colA >= t1.colB)

#### transpose


    tbl | transpose({keycol: keyexpr}, {col0:expr0, col1:expr1, .., valcol:valexpr})

Arg types:

_keyexpr_: one-arg function
_expri_: one-arg function
_valexpr_: one-arg function

Transpose function creates a table that transposes the given table,
synthesizing column names from the cell values. _tbl_ must be a two-column table
created by [cogroup](#cogroup). Imagine table t0 created by cogroup:

t0:

|key  |value |
|-----|------|
|120  | tmp1 |
|130  | tmp2 |


Each cell in the ::value:: column must be another table, for example:

tmp1:

|chrom|start|  end|count|
|-----|-----|-----|-----|
|chr1 |    0|  100|  111|
|chr1 |  100|  200|  123|
|chr2 |    0|  100|  234|


tmp2:

|chrom|start|  end|count|
|-----|-----|-----|-----|
|chr1 |    0|  100|  444|
|chr1 |  100|  200|  456|
|chr2 |  100|  200|  478|


::t0 | transpose({sample_id:&key}, {&chrom, &start, &end, &count}):: will produce
the following table.


|sample_id| chr1_0_100| chr1_100_200| chr2_0_100| chr2_100_200|
|---------|-----------|-------------|-----------|-------------|
|120      |   111     |   123       |   234     |    NA       |
|130      |   444     |   456       |   NA      |   478       |

The _keyexpr_ must produce a struct with >= 1 column(s).

The 2nd arg to transpose must produce a struct with >= 2 columns. The last column is used
as the value, and the other columns used to compute the column name.



#### gather


    tbl | gather(colname..., key:=keycol, value:=valuecol)

Arg types:

- _colname_: string
- _keycol_: string
- _valuecol_: string

Gather collapses multiple columns into key-value pairs, duplicating all other columns as needed. gather is based on the R tidyr::gather() function.

Example: Imagine table t0 with following contents:

|col0 | col1| col2|
|-----|-----|-----|
|Cat  | 30  | 31  |
|Dog  | 40  | 41  |

::t0 | gather("col1", "col2, key:="name", value:="value"):: will produce the following table:

| col0| name| value|
|-----|-----|------|
|  Cat| col1|    30|
|  Cat| col2|    31|
|  Dog| col1|    40|
|  Dog| col2|    41|
		

#### spread


    tbl | spread(keycol, valuecol)

Arg types:

- _keycol_: string
- _valuecol_: string

Spread expands rows across two columns as key-value pairs, duplicating all other columns as needed. spread is based on the R tidyr::spread() function.

Example: Imagine table t0 with following contents:

|col0 | col1| col2|
|-----|-----|-----|
|Cat  | 30  | 31  |
|Dog  | 40  | 41  |

::t0 | spread("col1", "col2, key:="col1", value:="col2"):: will produce the following table:

| col0| 30| 40|
|-----|---|---|
|  Cat| 31|   |
|  Dog|   | 41|

Note the blank cell values, which may require the use the function to contains to
test for the existence of a field in a row struct in subsequent manipulations.


#### collapse


    tbl | collapse(colname...)

Arg types:

- _colname_: string

Collapse will collapse multiple rows with non-overlapping values for the specified
columns into a single row when all of the other cell values are identical.

Example: Imagine table t0 with following contents:

|col0 | col1| col2|
|-----|-----|-----|
|Cat  | 30  |     |
|Cat  |     | 41  |

::t0 | collapse("col1", "col2"):: will produce the following table:

|col0 | col1| col2|
|-----|-----|-----|
|Cat  | 30  | 41  |

Note that the collapse will stop if one of the specified columns has multiple
values, for example for t0 below:

|col0 | col1| col2|
|-----|-----|-----|
|Cat  | 30  |     |
|Cat  | 31  | 41  |

::t0 | collapse("col1", "col2"):: will produce the following table:

|col0 | col1| col2|
|-----|-----|-----|
|Cat  | 30  |     |
|Cat  | 30  | 41  |



#### joinbed


    srctable | joinbed(bedtable [, chrom:=chromexpr]
                                [, start:=startexpr]
                                [, end:=endexpr]
                                [, length:=lengthexpr]
                                [, map:=mapexpr])

Arg types:

- bedtable: table (https://uswest.ensembl.org/info/website/upload/bed.html)
- chromexpr: one-arg function (default: ::|row|row.chrom)
- startexpr: one-arg function (default: ::|row|row.start)
- endexpr: one-arg function (default: ::|row|row.end)
- lengthexpr: one-arg function (default: NA)
- mapexpr: two-arg function (srcrow, bedrow) (default: ::|srcrow,bedrow|srcrow::)

Joinbed is a special kind of join operation that's optimized for intersecting
_srctable_ with genomic intervals listed in _bedtable_.

Example:

     bed := read("test.bed")
     bc := read("test.bincount.tsv")
     out := bc | joininbed(bed, chrom:=$chromo))

Optional args _chromexpr_, _startexpr_, _endexpr_, and _lengthexpr_ specify how to extract the
coordinate values from a _srctable_ row. For example:

     bc := read("test.bincount.tsv")
     bc | joinbed(bed, chrom:=&chromo, start=&S, end=&E)

will use columns "chromo", "S", "E" in table "test.bincount.tsv" to
construct a genomic coordinate, then checks if the coordinate intersects with a
row in the bed table.

At most one of _endexpr_ or _lengthexpr_ arg can be set. If _endexpr_ is set, [_startexpr_, _endexpr_)
defines a zero-based half-open range for the given chromosome. If _lengthexpr_ is
set, [_startexpr_, _startexpr_+_lengthexpr_) defines a zero-based half-open coordinate range.  The

The BED table must contain at least three columns. The chromosome name, start
and end coordinates are extracted from the 1st, 2nd and 3rd columns,
respectively. Each coordinate range is zero-based, half-open.

Two coordinate ranges are considered to intersect if they have nonempty overlap,
that is they overlap at least one base.

_mapexpr_ describes the format of rows produced by joinbed. If _mapexpr_ is
omitted, joinbed simply emits the matched rows in _srctable_.

For example, the below example will produce rows with three columns: name, chrom
and pos.  the "name" column is taken from the "featname" in the BED row, the
"pos" column is taken "start" column of the "bc" table row.

     bc := read("test.bincount.tsv")
     bc | joinbed(bed, chrom:=&chromo, start=&S, end=&E, map:=|bcrow,bedrow|{name:bedrow.featname, pos: bcrow.start})

The below is an example of using the "row" argument. It behaves identically to the above exampel.

     bc := read("test.bincount.tsv")
     bc | joinbed(bed, row:=bcrow, chrom:=bcrow.chromo, start=bcrow.S, end=bcrow.E, map:=|bcrow,bedrow|{name:bedrow.featname, pos: bcrow.start})



#### count


tbl | count()

Count counts the number of rows in the table.

Example: imagine table t0:

| col1|
|-----|
|  3  |
|  4  |
|  8  |

::t0 | count():: will produce 3.


#### pick


    tbl | pick(expr)

Arg types:

- _expr_: one-arg boolean function

Pick picks the first row in the table that satisfies _expr_.  If no such row is
found, it returns NA.

Imagine table t0:

|col0 | col1|
|-----|-----|
|Cat  | 10  |
|Dog  | 20  |

::t0 | pick(&col1>=20):: will return {Dog:20}.
::t0 | pick(|row|row.col1>=20):: is the same thing.


#### table


table(expr...)

Arg types:

- _expr_: any

Table creates a new table consisting of the given values.

#### readdir

Usage: readdir(path)

readdir creates a Struct consisting of files in the given directory.  The field
name is a sanitized pathname, value is either a Table (if the file is a .tsv,
.btsv, etc), or a string (if the file cannot be parsed as a table).

#### table_attrs


   tbl |table_attrs()

Example:

     t := read("foo.tsv")
     table_attrs(t).path  (=="foo.tsv")

Table_attrs returns table attributes as a struct with three fields:

 - Field 'type' is the table type, e.g., "tsv", "mapfilter"
 - Field 'name' is the name of the table. It is some random string.
 - Field 'path' is name of the file the table is read from.
   "path" is nonempty only for tables created directly by read(),
   tables that are result of applying map or filter to table created by read().

#### force


    tbl | force()

Force is a performance hint. It is logically a no-op; it just
produces the contents of the source table unchanged.  Underneath, this function
writes the source-table contents in a file. When this expression is evaluated
repeatedly, force will read contents from the file instead of
running _tbl_ repeatedly.


### Row manipulation


#### Struct comprehension

Usage: {col1:expr1, col2:expr2, ..., colN:exprN}

{...} composes a struct. For example,

    x := table({f0:"foo", f1:123}, {f0:"bar", f1:234})

creates the following table and assigns it to $x.

| f0  | f1  |
|-----|-----|
|foo  | 123 |
|bar  | 234 |

The "colK:" part of each element can be omitted, in which case the column name
is auto-computed using the following rules:

- If expression is of form "expr.field" (struct field reference), then
  "field" is used as the column name.

- Similarly, if expression is of form "$field" (struct field reference), then
  "field" is used as the column name.

- If expression is of form "var", then "var" is used as the column name.

- Else, column name will be "fN", where "N" is 0 for the first column, 1 for the
  2nd column, and so on.

For example:

    x := table({col0:"foo", f1:123}, {col1:"bar", f1:234})
    y := 10
    x | map({$col0, z, newcol: $col1})

| col0  | z  | newcol |
|-------|----|--------|
|foo    | 10 |123     |
|bar    | 10 |234     |


The struct literal expression can contain a regex of form

        expr./regex/

"expr" must be another struct. "expr" is most often "_". "{var./regex/}" is
equivalent to a struct that contains all the fields in "var" whose names match
regex.  Using the above example,

        x | map({_./.*1$/})

will pick only column f1. The result will be

| f1  |
|-----|
| 123 |
| 234 |

As an syntax suger, you can omit the "var." part if var is "_". So "map($x,
{_./.*1$/})" can also be written as "map($x, {/.*1$/})".

#### unionrow


    unionrow(x, y)

Arg types:

- _x_, _y_: struct

Example:
    unionrow({a:10}, {b:11}) == {a:10,b:11}
    unionrow({a:10, b:11}, {b:12, c:"ab"}) == {a:10, b:11, c:"ab"}

Unionrow merges the columns of the two structs.
If one column appears in both args, the value from the second arg is taken.
Both arguments must be structs.

#### optionalfield

Usage: optional_field(struct, field [, default:=defaultvalue])

This function acts like struct.field. However, if the field is missing, it
returns the defaultvalue. If defaultvalue is omitted, it returns NA.

Example:

  optionalfield({a:10,b:11}, a) == 10
  optionalfield({a:10,b:11}, c, default:12) == 12
  optionalfield({a:10,b:11}, c) == NA


#### contains


    contains(struct, field)

Arg types:

- _struct_: struct
- _field_: string

Contains returns true if the struct contains the specified field, else it returns false.

### File I/O

#### read

Usage:

    read(path [, type:=filetype])

Arg types:

- _path_: string
- _filetype_: string


Read table contents to a file. The optional argument 'type' specifies the file format.
If the type is unspecified, the file format is auto-detected from the file extension.

- Extension ".tsv" or ".bed" loads a tsv file. If file
  "path_data_dictionary.tsv" exists, it is also read to construct a
  dictionary. If a dictionary tsv file is missing, the cell data types are
  guessed.

- Extension ".prio" loads a fragment file.

- Extension ".btsv" loads a btsv file.

- Extension ".bam" loads a BAM file.

- Extension ".pam" loads a PAM file.


If the type is specified, it must be one of the following strings: "tsv", "bed",
"btsv", "fragment", "bam", "pam". The type arg overrides file-type autodetection
based on path extension.

Example:
  read("blahblah", type:=tsv)
.

#### write

Usage: write(table, "path" [,shards:=nnn] [,type:="format"] [,datadictionary:=false])

Write table contents to a file. The optional argument "type" specifies the file
format. The value should be either "tsv", "btsv", or "bed".  If type argument is
omitted, the file format is auto-detected from the extension of the "path" -
".tsv" for the TSV format, ".btsv" for the BTSV format, ".bed" for the BED format.

- For TSV, write a dictionary file "path_data_dictionary.tsv" is created by
  default. Optional argument datadictionary:=false disables generation of the
  dictionary file. For example:

    read("foo.tsv") | write("bar.tsv", datadictionary:=false)

- When writing a btsv file, the write function accepts the "shards"
  parameter. It sets the number of rangeshards. For example,

    read("foo.tsv") | write("bar.btsv", shards:=64)

  will create a 64way-sharded bar.btsv that has the same content as
  foo.tsv. bar.btsv is actually a directory, and shard files are created
  underneath the directory.

.

#### writecols

Usage: writecols(table, "path-template", [,datadictionary:=false], [gzip:=false])

Write table contents to a set of files, each containing a single column of the
table. The naming of the files is determined using a templating (golang's
text/template). For example, a template of the form:

cols-{{.Name}}-{{.Number}}.ctsv

will have .Name replaced with name of the column and .Number with the index
of the column. So for a table with two columns 'A' and 'B', the files
will cols-A-0.ctsv and cols-B-1.cstv. Note, that if a data dictionary is
to be written it will have 'data-dictionary' for .Name and 0 for .Number.

Files may be optionally gzip compressed if the gzip named parameter is specified
as true.
.

### Predicates

    expr0 == expr1
    expr0 > expr1
    expr0 >= expr1
    expr0 < expr1
    expr0 <= expr1
    max(expr1, expr2, ..., exprN)
    min(expr1, expr2, ..., exprN)

    expr0 ?== expr1
    expr0 ==? expr1
    expr0 ?==? expr1

These predicates can be applied to any scalar values, including
ints, floats, strings, chars, and dates, and nulls.
The two sides of the operator must be of the same type, or null.
A null value is treated as ∞. Thus, 1 < NA, but 1 > -NA

Predicates "==?", "?==", "?==?" are the same as "==", as long as both sides are
non-null. "X==?Y" is true if either X==Y, or Y is null. "X?==Y" is true if
either X==Y, or X is null. "X?==?Y" is true if either X==Y, or X is null, or Y
is null. These predicates can be used to do outer, left, or right joins with
join builtin.

#### isnull


    isnull(expr)

isnull returns true if expr is NA (or -NA). Else it returns false.

#### istable


    istable(expr)

Istable returns true if expr is a table.

#### isstruct


    isstruct(expr)

Isstruct returns true if expr is a struct.

### Arithmetic and string operators

    expr0 + expr1

The "+" operator can be applied to ints, floats, strings, and structs.  The two
sides of the operator must be of the same type. Adding two structs will produce
a struct whose fields are union of the inputs. For example,

    {a:10,b:20} + {c:30} == {a:10,b:20,c:30}
    {a:10,b:20,c:40} + {c:30} == {a:10,b:20,c:30}

As seen in the second example, if a column appears in both inputs, the second
value will be taken.


    -expr0

The unary "-" can be applied to ints, floats, and strings.  Expression "-str"
produces a new string whose sort order is lexicographically reversed. That is,
for any two strings A and B, if A < B, then -A > -B. Unary "-" can be used to
sort rows in descending order of string column values.

    expr0 * expr1
    expr0 % expr1
    expr0 - expr1
    expr0 / expr1

The above operators can be applied to ints and floats.  The two sides of the
operator must be of the same type.


#### string_count


    string_count(str, substr)

Arg types:

- _str_: string
- _substr_: string

Example:
    string_count("good dog!", "g") == 2

Count the number of non-overlapping occurrences of substr in str.

#### regexp_match

Usage: regexp_match(str, re)

Example:
    regexp_match("dog", "o+") == true
    regexp_match("dog", "^o") == false

Check if str matches re.
Uses go's regexp.MatchString (https://golang.org/pkg/regexp/#Regexp.MatchString).

#### regexp_replace


    regexp_replace(str, re, replacement)

Arg types:

- _str_: string
- _re_: string
_ _replacement_: string

Example:
    regexp_replace("dog", "(o\\S+)" "x$1y") == "dxogy"

Replace occurrence of re in str with replacement. It is implemented using Go's
regexp.ReplaceAllString (https://golang.org/pkg/regexp/#Regexp.ReplaceAllString).

#### string_len


    string_len(str)

Arg types:

- _str_: string


Example:
    string_len("dog") == 3

Compute the length of the string. Returns an integer.

#### string_has_suffix


    string_has_suffix(str, suffix)

Arg types:

- _str_: string
- _suffix_: string

Example:
    string_has_suffix("dog", "g") == true

Checks if a string ends with the given suffix

#### string_has_prefix


    string_has_prefix(str, prefix)

Arg types:

- _str_: string
- _prefix_: string

Example:
    string_has_prefix("dog", "d") == true

Checks if a string starts with the given prefix

#### string_replace


    string_replace(str, old, new)

Arg types:

- _str_: string
- _old_: string
_ _new_: string

Example:
    regexp_replace("dogo", "o" "a") == "daga"

Replace occurrence of old in str with new.

#### substring


    substring(str, from [, to])

Substring extracts parts of a string, [from:to].  Args "from" and "to" specify
byte offsets, not character (rune) counts.  If "to" is omitted, it defaults to
∞.

Arg types:

- _str_: string
- _from_: int
_ _to_: int, defaults to ∞

Example:
    substring("hello", 1, 3) == "ell"
    substring("hello", 2) == "llo"


#### sprintf


    sprintf(fmt, args...)

Arg types:

- _fmt_: string
- _args_: any

Example:
    sprintf("hello %s %d", "world", 10) == "hello world 10"

Builds a string from the format string. It is implemented using Go's fmt.Sprintf
The args cannot be structs or tables.

#### string


    string(expr)

Examples:
    string(123.0) == "123.0"
    string(NA) == ""
    string(1+10) == "11"

The string function converts any scalar expression into a string.
NA is translated into an empty string.


#### int


    int(expr)

Int converts any scalar expression into an integer.
Examples:
    int("123") == 123
    int(1234.0) == 1234
    int(NA) == 0

NA is translated into 0.
If expr is a date, int(expr) computes the number of seconds since the epoch (1970-01-01).
If expr is a duration, int(expr) returns the number of nanoseconds.


#### float


    float(expr)

The float function converts any scalar expression into an float.
Examples:
    float("123") == 123.0
    float(1234) == 1234.0
    float(NA) == 0.0

NA is translated into 0.0.
If expr is a date, float(expr) computes the number of seconds since the epoch (1970-01-01).
If expr is a duration, float(expr) returns the number of seconds.


#### hash64


    hash64(arg)

Arg types:

- _arg_: any


Example:
    hash64("foohah")

Compute the hash of the arg. Arg can be of any type, including a table or a row.
The hash is a positive int64 value.

#### land


    land(x, y)

Arg types:

- _x_, _y_: int

Example:
    land(0xff, 0x3) == 3

Compute the logical and of two integers.

#### lor

Usage: lor(x, y)
Example:
    lor(0xff, 0x3) == 255

Compute the logical or of two integers.

#### isset


    isset(x, y)

Arg types:

- _x_, _y_: int

Example:
    isset(0x3, 0x1) == true

Compute whether all bits in the second argument are present in the first.
Useful for whether flags are set.

### Logical operators

    !expr

### Miscellaneous functions

#### print


    print(expr... [,depth:=N] [,mode:="mode"])

Print the list of expressions to stdout.  The depth parameters controls how
nested tables are printed.  If depth=0, nested tables are printed as
"[omitted]".  If depth > 0, nested tables are expanded up to that level.  If the
depth argument is omitted, print fully expands nested tables to the infinite
degree.

The mode argument controls the print format. Valid values are the following:

- "default" prints the expressions in a long format.
- "compact" prints them in a short format
- "description" prints the value description (help message) instead of the values
  themselves.

The default value of mode is "default".




## Future plans

- Invoking R functions inside GQL.

- Invoking GQL inside R.

- Reading VCF and other non-tsv files.

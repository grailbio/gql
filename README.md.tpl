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

{{include: gql/testdata/file0.tsv}}

GQL treats a tsv file as a 2D table.  Function [read](#read) loads a TSV
file.

{{eval: read(`gql/testdata/file0.tsv`) }}

The first column, '#' is not a real column. It just shows the row number.

#### Selection and projection using `filter` and `map`

Operator "|" feeds contents of a table to a function.  [filter](#filter) is a
selection function. It picks rows that matches `expr` and combines them into a
new table.  Within `expr`, you can refer to a column by prefixing `&`.  `&A`
means "value of the column `A` in the current row".
"var := expr" assigns the value of expr to the variable.

{{eval:
f0 := read(`gql/testdata/file0.tsv`);
f0 | filter(&A==10)}}

> Note: `&A==10` is a shorthand for `|_| _.A==10`, which is a function that takes
 argument `_` and computes `_.A==10`. GQL syntactically translates an expression
 that contains '&' into a function. So you can write the above example as
 `f0 | filter(|_|{_.A==10})`. It produces the same result. We will discuss functions
 and '&'-expansions in more detail [later](#functions).

> Note: Expression `table | filter(...)` can be also written as `filter(table, ...)`.
 These two are exactly the same, but the former is usually easier to
 read.  This syntax applies to any GQL function that takes a table as the first
 argument: `A(x) | B(y)` is the same as `B(A(x), y)`. Thus:

{{eval: f0 | filter(&A==11)}}

Function [map](#map) projects a table.

{{eval: f0 | map({&A, &C})}}

In fact, `map` and `filter` are internally one function that accepts different
kinds of arguments. You can combine projection and selection in one function,
like below:

{{eval: f0 | map({&A, &C}, filter:=string_has_suffix(&C, "0"))}}


{{eval: f0 | filter(string_has_suffix(&C, "0"), map:={&A, &C})}}

#### Sorting

Function [sort](#sort) sorts the table by ascending order of the argument. Giving "-&A"
as the sort key will sort the table in descending order of column A:

{{eval: f0 | sort(-&A)}}

The sort key can be an arbitrary expression. For example, `f0 | sort(&C+&B)` will
sort rows by ascending order of the concatenation of columns C and B.
`f0 | sort(&C+&B)` will produce the same table as `f0` itself.

#### Joining multiple tables

Imagine file `gql/testdata/file1.tsv` with the following contents.

{{include: gql/testdata/file1.tsv}}

Function [join](#join) joins multiple tables into one table.

{{eval:
f1 := read(`gql/testdata/file1.tsv`);
join({f0, f1}, f0.A==f1.C, map:={A:f0.A, B:f0.B, C: f0.C, D:f1.D})}}

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

{{include: gql/testdata/file2.tsv}}

Function [cogroup](#cogroup) is a special kind of join, similar to "select ... group by"
in SQL. Cogroup aggregates rows in a table by a column.  The result is a
two-column table where the "key" column is the aggregation key, and "value"
column is a subtable that stores the rows wit the given key.

{{eval: read(`gql/testdata/file2.tsv`) | cogroup(&A)}}

Column '[{A:cat,B:1},{A:cat,B:3}]` is a shorthand notation for
the following table:

║ A   ║ B ║
├─────┼───┤
│ cat │ 1 │
│ cat │ 3 │


Function [reduce](#reduce) is similar to [cogroup](#cogroup), but it applies a user-defined
function over rows with the same key. The function must be commutative. The
result is a two-column table with nested columns. Reduce is generally faster
than cogroup.

{{eval: read(`gql/testdata/file2.tsv`) | reduce(&A, |a,b|(a+b), map:=&B)}}

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

{{builtin}}

## Future plans

- Invoking R functions inside GQL.

- Invoking GQL inside R.

- Reading VCF and other non-tsv files.

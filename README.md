# Grail query language

Last updated: 2019-03-25.

GQL is a query language for tidydata. You can think of it as an SQL with a funny
syntax.

- It has many tidy- and grail-specific functions. For example, one can read
  tsv, prio, bed, bam, pam, bincount files as tables.

- It can read and write files on S3 directly.

- It has many tidy-specific functions, such as supports for grail- or
  bioinformatics- specific file formats (prio, bed, bam, ...).

- It can handle very large files, including BAM and fragment files produced by
  CCGA pipelines.

- It supports [distributed execution](#1-6-distributed-execution) of key functions.

- GQL language syntax is very different from SQL, but if you squint enough, you
  can see the correspondence with SQL. GQL syntax differs from SQL for several
  reasons. First, tidy dataset is naturally hierarchical.  For example, we have
  a wgs_wbc.tsv with a column that records an S3 directory name for analyses
  result. The S3 directory contains *.bam, *.prio and other files. Many of our
  analyses involve computing stats from these files for a certain cohort.  SQL
  handles such queries poorly - for example, SQL cannot read tables whose names
  are written in a column in another table. Second, some GQL functions, such as
  `transpose`, have no corresponding SQL counterpart.

GQL source code is in
[go/src/grail.com/cmd/grail-query](https://sg.eng.grail.com/grail/grail/-/tree/go/src/grail.com/cmd/grail-query).

The newest GQL binaries are at
[s3://grail-bin/linux/amd64/latest/grail-query](https://s3.console.aws.amazon.com/s3/object/grail-bin/linux/amd64/latest/grail-query).

Recent changes:

- 2019-03-25: builtin global variables `ccga1` and `ccga2` have been removed in favor of builtin function `tidydata`.
  `ccga1` is roughly the same as `tidydata("ccga1", "latest", "analysis")`, except that it only loads the latest version.
  For more info about tidydata, read https://docs.google.com/document/d/1qXgyyX0WIE0s9gTabENz4o-qx2IIa8aR4aMimV5faMs/edit#

- 2019-03-25: `substring` builtin added.

- 2019-02-09: CCGA1 & CCGA2 supported via tidydata service. Global tables 'ccga1' and 'ccga2' store pointers
  to other ccga1 and ccga2 tables. If your script uses the old 'ccga' table, please switch to 'ccga1'.
  The 'ccga1' table is mostly a superset of 'ccga'.

Links:

- [DPLYR and GQL](https://docs.google.com/document/d/1SjLiI-98X21AJHzY9MhCVkklA5MRpWH3u83qrK7XtAs/edit#heading=h.8ugf5452ga1v)
- [Slides](https://docs.google.com/presentation/d/1x1eiDpxgl0YA43Cb3qpJ6rw_ibpg8XeGMjAWdWpTJJA/edit)

WARNING: This document is auto-generated using
[go/src/grail.com/cmd/grail-query/release.py](https://sg.eng.grail.com/grail/grail/-/tree/go/src/grail.com/cmd/grail-query/release.py). Do
not modify this page directly.

## Table of contents

* [1. Running GQL](#1-running-gql)
  * [1.1. Basic functions](#1-1-basic-functions)
    * [1.1.1. Reading a table](#1-1-1-reading-a-table)
    * [1.1.2. Selection and projection using `filter` and `map`](#1-1-2-selection-and-projection-using-`filter`-and-`map`)
    * [1.1.3. Sorting](#1-1-3-sorting)
    * [1.1.4. Joining multiple tables](#1-1-4-joining-multiple-tables)
    * [1.1.5. Cogroup and reduce](#1-1-5-cogroup-and-reduce)
  * [1.2. Working with CCGA1 tidydata](#1-2-working-with-ccga1-tidydata)
  * [1.3. Reading bincounts files from wgs_wbc.tsv](#1-3-reading-bincounts-files-from-wgs_wbc-tsv)
  * [1.4. Extracting stats from fragment files](#1-4-extracting-stats-from-fragment-files)
  * [1.5. Intersecting bincounts and a BED file](#1-5-intersecting-bincounts-and-a-bed-file)
  * [1.6. Distributed execution](#1-6-distributed-execution)
  * [1.7. More examples](#1-7-more-examples)
    * [1.7.1. ex1.gql: intersect bincounts file and a BED file.](#1-7-1-ex1-gql-intersect-bincounts-file-and-a-bed-file-)
    * [1.7.2. ex2.gql: merge bincounts for multiple samples](#1-7-2-ex2-gql-merge-bincounts-for-multiple-samples)
    * [1.7.3. ex3.gql: map binned-genomic-position to original_depth (bincount)](#1-7-3-ex3-gql-map-binned-genomic-position-to-original_depth-bincount)
    * [1.7.4. frag-length-historgram.gql: compute length distribution of fragments.](#1-7-4-frag-length-historgram-gql-compute-length-distribution-of-fragments-)
    * [1.7.5. cpg-frequency.gql: frequency of methylation state at each CpG site](#1-7-5-cpg-frequency-gql-frequency-of-methylation-state-at-each-cpg-site)
* [2. GQL implementation overview](#2-gql-implementation-overview)
* [3. GQL syntax](#3-gql-syntax)
* [4. Data types](#4-data-types)
  * [4.1. Table](#4-1-table)
  * [4.2. Row (struct)](#4-2-row-struct)
  * [4.3. Scalar values](#4-3-scalar-values)
* [5. Control-flow expressions](#5-control-flow-expressions)
* [6. Code blocks](#6-code-blocks)
* [7. Functions](#7-functions)
* [8. Deprecated GQL constructs](#8-deprecated-gql-constructs)
* [9. Importing a GQL file](#9-importing-a-gql-file)
* [10. Builtin functions](#10-builtin-functions)
  * [10.1. Table manipulation](#10-1-table-manipulation)
    * [10.1.1. map](#10-1-1-map)
    * [10.1.2. filter](#10-1-2-filter)
    * [10.1.3. reduce](#10-1-3-reduce)
    * [10.1.4. flatten](#10-1-4-flatten)
    * [10.1.5. concat](#10-1-5-concat)
    * [10.1.6. cogroup](#10-1-6-cogroup)
    * [10.1.7. firstn](#10-1-7-firstn)
    * [10.1.8. minn](#10-1-8-minn)
    * [10.1.9. sort](#10-1-9-sort)
    * [10.1.10. join](#10-1-10-join)
    * [10.1.11. transpose](#10-1-11-transpose)
    * [10.1.12. gather](#10-1-12-gather)
    * [10.1.13. spread](#10-1-13-spread)
    * [10.1.14. collapse](#10-1-14-collapse)
    * [10.1.15. joinbed](#10-1-15-joinbed)
    * [10.1.16. count](#10-1-16-count)
    * [10.1.17. pick](#10-1-17-pick)
    * [10.1.18. table](#10-1-18-table)
    * [10.1.19. readdir](#10-1-19-readdir)
    * [10.1.20. table_attrs](#10-1-20-table_attrs)
    * [10.1.21. force](#10-1-21-force)
    * [10.1.22. tidydata](#10-1-22-tidydata)
  * [10.2. Row manipulation](#10-2-row-manipulation)
    * [10.2.1. Struct comprehension](#10-2-1-struct-comprehension)
    * [10.2.2. unionrow](#10-2-2-unionrow)
    * [10.2.3. optionalfield](#10-2-3-optionalfield)
    * [10.2.4. contains](#10-2-4-contains)
  * [10.3. File I/O](#10-3-file-io)
    * [10.3.1. read](#10-3-1-read)
    * [10.3.2. write](#10-3-2-write)
    * [10.3.3. writecols](#10-3-3-writecols)
  * [10.4. Predicates](#10-4-predicates)
    * [10.4.1. isnull](#10-4-1-isnull)
    * [10.4.2. istable](#10-4-2-istable)
    * [10.4.3. isstruct](#10-4-3-isstruct)
  * [10.5. Arithmetic and string operators](#10-5-arithmetic-and-string-operators)
    * [10.5.1. string_count](#10-5-1-string_count)
    * [10.5.2. regexp_match](#10-5-2-regexp_match)
    * [10.5.3. regexp_replace](#10-5-3-regexp_replace)
    * [10.5.4. string_len](#10-5-4-string_len)
    * [10.5.5. string_has_suffix](#10-5-5-string_has_suffix)
    * [10.5.6. string_has_prefix](#10-5-6-string_has_prefix)
    * [10.5.7. string_replace](#10-5-7-string_replace)
    * [10.5.8. substring](#10-5-8-substring)
    * [10.5.9. sprintf](#10-5-9-sprintf)
    * [10.5.10. string](#10-5-10-string)
    * [10.5.11. int](#10-5-11-int)
    * [10.5.12. float](#10-5-12-float)
    * [10.5.13. hash64](#10-5-13-hash64)
    * [10.5.14. land](#10-5-14-land)
    * [10.5.15. lor](#10-5-15-lor)
    * [10.5.16. isset](#10-5-16-isset)
  * [10.6. Logical operators](#10-6-logical-operators)
  * [10.7. Miscellaneous functions](#10-7-miscellaneous-functions)
    * [10.7.1. print](#10-7-1-print)
* [11. Future plans](#11-future-plans)

## 1. Running GQL

If you invoke `grail-query` without argument, it starts an interactive prompt,
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
[$GRAIL/go/src/grail.com/cmd/grail-query/scripts](https://sg.eng.grail.com/grail/grail/-/tree/go/src/grail.com/cmd/grail-query/scripts)
contains nontrivial examples. For example,

    grail-query script/fragment_beta.gql

When evaluating GQL in a script, an expression can span multiple lines, and it
is delimited by ';' or EOF.

You can pass a sequence of `-flag=value` or just `-flag` after the script name.
The `flag` will become a global variable and will be accessible as `flag` in the
script.  In the above example, we pass two flags "in" and "out" to the script
`testdata/convert.gql`.

    grail-query testdata/convert.gql -in=/tmp/test.tsv -out=/tmp/test.btsv

### 1.1. Basic functions


#### 1.1.1. Reading a table

Imagine you have the following TSV file, `testdata/file0.tsv`:

        A	B	C
        10	ab0	cd0
        11	ab1	cd1


GQL treats a tsv file as a 2D table.  Function [read](#10-3-1-read) loads a TSV
file.

    read(`testdata/file0.tsv`) 


| #|  A|   B|   C|
|--|---|----|----|
| 0| 10| ab0| cd0|
| 1| 11| ab1| cd1|



The first column, '#' is not a real column. It just shows the row number.

#### 1.1.2. Selection and projection using `filter` and `map`

Operator "|" feeds contents of a table to a function.  [filter](#10-1-2-filter) is a
selection function. It picks rows that matches `expr` and combines them into a
new table.  Within `expr`, you can refer to a column by prefixing `&`.  `&A`
means "value of the column `A` in the current row".
"var := expr" assigns the value of expr to the variable.

    f0 := read(`testdata/file0.tsv`);
    f0 | filter(&A==10)


| #|  A|   B|   C|
|--|---|----|----|
| 0| 10| ab0| cd0|



> Note: `&A==10` is a shorthand for `|_| _.A==10`, which is a function that takes
 argument `_` and computes `_.A==10`. GQL syntactically translates an expression
 that contains '&' into a function. So you can write the above example as
 `f0 | filter(|_|{_.A==10})`. It produces the same result. We will discuss functions
 and '&'-expansions in more detail [later](#7-functions).

> Note: Expression `table | filter(...)` can be also written as `filter(table, ...)`.
 These two are exactly the same, but the former is usually easier to
 read.  This syntax applies to any GQL function that takes a table as the first
 argument: `A(x) | B(y)` is the same as `B(A(x), y)`. Thus:

    f0 | filter(&A==11)


| #|  A|   B|   C|
|--|---|----|----|
| 0| 11| ab1| cd1|



Function [map](#10-1-1-map) projects a table.

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



#### 1.1.3. Sorting

Function [sort](#10-1-9-sort) sorts the table by ascending order of the argument. Giving "-&A"
as the sort key will sort the table in descending order of column A:

    f0 | sort(-&A)


| #|  A|   B|   C|
|--|---|----|----|
| 0| 11| ab1| cd1|
| 1| 10| ab0| cd0|



The sort key can be an arbitrary expression. For example, `f0 | sort(&C+&B)` will
sort rows by ascending order of the concatenation of columns C and B.
`f0 | sort(&C+&B)` will produce the same table as `f0` itself.

#### 1.1.4. Joining multiple tables

Imagine file `testdata/file1.tsv` with the following contents.

        C	D	E
        10	ef0	ef1
        12	gh0	gh1


Function [join](#10-1-10-join) joins multiple tables into one table.

    f1 := read(`testdata/file1.tsv`);
    join({f0, f1}, f0.A==f1.C, map:={A:f0.A, B:f0.B, C: f0.C, D:f1.D})


| #|  A|   B|   C|   D|
|--|---|----|----|----|
| 0| 10| ab0| cd0| ef0|



The first argument to join is the list of tables to join. It is of the form
`{name0: table0, ..., nameN, tableN}`. Tag `nameK` can be used to name row
values for `tableK` in the rest of the join expression. If you omit the `nameI:`
part of the table list, join tries to guess a reasonable name from the column
value.

NOTE: See [struct comprehension](#10-2-1-struct-comprehension) for more details
about how column names are computed when they are omitted.

The second argument is the join condition.  The optional 'map:=rowspec' argument
specifies the list of columns in the final result.  Join will produce Cartesian
products of rows in the two tables, then create a new table consisting of the
results that satisfy the given join condition. For good performance, the join
condition should be a conjunction of column equality constraints (`table0.col0 ==
table1.col0 && ... && table1.col2 == table2.col2`). Join recognizes this form of
conditions and executes the operation more efficiently.


#### 1.1.5. Cogroup and reduce

Imagine a file 'testdata/file2.tsv' like below:

        A	B
        cat	1
        dog	2
        cat	3
        bat	4


Function [cogroup](#10-1-6-cogroup) is a special kind of join, similar to "select ... group by"
in SQL. Cogroup aggregates rows in a table by a column.  The result is a
two-column table where the "key" column is the aggregation key, and "value"
column is a subtable that stores the rows wit the given key.

    read(`testdata/file2.tsv`) | cogroup(&A)


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


Function [reduce](#10-1-3-reduce) is similar to [cogroup](#10-1-6-cogroup), but it applies a user-defined
function over rows with the same key. The function must be commutative. The
result is a two-column table with nested columns. Reduce is generally faster
than cogroup.

    read(`testdata/file2.tsv`) | reduce(&A, |a,b|(a+b), map:=&B)


| #| key| value|
|--|----|------|
| 0| cat|     4|
| 1| dog|     2|
| 2| bat|     4|



Functions such as `cogroup` and `reduce` exist because it is much amenable for
distributed execution compared to generic joins. We explain distributed
execution in a [later section](#1-6-distributed-execution)

### 1.2. Working with CCGA1 tidydata

    ccga1 := tidydata("ccga1", "latest", "analysis")


    {
    	version: 2019-03-08-21-11-08,
    	tableset: analysis,
    	type: ,
    	assay_tm: tmp0,
    	assay_tm_raw_analysis: tmp1,
    	assay_tm_raw_bcl2fastq: tmp2,
    	assay_tm_raw_sequencer_qc: tmp3,
    	assay_tm_raw_tm: tmp4,
    	clinical: tmp5,
    	clinical_aux: tmp6,
    	cv_classification_solid: tmp7,
    	cv_classification_solid_and_liquid: tmp8,
    	cv_classification_solid_and_liquid_patient_ids: tmp9,
    	cv_classification_solid_patient_ids: tmp10,
    	flowcell: tmp11,
    	participants: tmp12,
    	pecan_cfdna: tmp13,
    	pecan_wbc: tmp14,
    	pre_analytical: tmp15,
    	sample_containers: tmp16,
    	tissue: tmp17,
    	wgbs_cfdna_methyl: tmp18,
    	wgs_cfdna: tmp19,
    	wgs_wbc: tmp20
    }


`tidydata` is a built-in function for reading tidydata tables.  The above
example reads the latest ccga1 dataset and assign it to a variable `ccga1`.
ccga1 is a structure that contains ccga1 toplevel tables as its fields.  For
example, typing `ccga1.cv_classification_solid` will show the contents of
`cv_classification_solid` table (which is stored in
`s3://grail-tidy-datasets/CCGA/ccga_classification_training_sample_set/2018-01-12aa/cv_classification_solid.tsv`):

    tmp1

|  #| replicate| patient_id| fold|
|---|----------|-----------|-----|
|  0|         1|         10|    2|
|  1|         1|       1000|    8|
|  2|         1|       1012|    2|
|  3|         1|       1014|    2|



### 1.3. Reading bincounts files from wgs_wbc.tsv

    wgs_wbc_20180409 := ccga1.wgs_wbc;
    wgs_wbc_20180409 | map({&analysis_lims_sample_id, &output_cna_bin_counts_file})
    


|    #| analysis_lims_sample_id|                                                      output_cna_bin_counts_file|
|-----|------------------------|--------------------------------------------------------------------------------|
|    0|                 P0080B0| s3://grail-clinical-results/35999/P0080B0/cna/P0080B0_normalized_bin_counts.tsv|
|    1|                 P0092B0| s3://grail-clinical-results/36316/P0092B0/cna/P0092B0_normalized_bin_counts.tsv|
|    2|                 P007ZG1| s3://grail-clinical-results/35953/P007ZG1/cna/P007ZG1_normalized_bin_counts.tsv|
|    3|                 P007E00| s3://grail-clinical-results/37316/P007E00/cna/P007E00_normalized_bin_counts.tsv|


> Note: variable `ccga1` was created in the previous example.

The above example creates a new table that selects two columns,
`analysis_lims_sample_id`, and `output_cna_bin_counts_file`, from `wgs_wbc.tsv`.

Expression `table | pick(expr)` picks the first row that matches `expr` in
`table`. The result of `pick` is a naked row.  The above example picks the first
(and the only) row where the `date` column is 2018-01-12, then picks the column
`wgs_wbc` from that row. As a result, variable `wbs_wbc_20180409` is set to the table
`wgs_wbc.tsv` for date 2018-01-12.

> Note: You could have just typed `tmp12` to read the same table, but the `tmp`
variable values depend on queries you performed in the past, so you can't rely
on them in a gql script.


Typing `tmp0` shows the contents of the bincounts for sample `P004400`.

    tmp0

| chromo|   start|     end|        original_depth|         baseline_only|          baseline_gc|       baseline_gc_pca|            null_depth|  mask|          bin_z_score| segment_ids|
|-------|--------|--------|----------------------|----------------------|---------------------|----------------------|----------------------|------|---------------------|------------|
|   chr1|       0|  100000|                    NA|                    NA|                   NA|                    NA|                    NA| FALSE|                   NA|          NA|
|   chr1|  100000|  200000|                    NA|                    NA|                   NA|                    NA|                    NA| FALSE|                   NA|          NA|
|   chr1|  200000|  300000|                    NA|                    NA|                   NA|                    NA|                    NA| FALSE|                   NA|          NA|
|   chr1|  300000|  400000|                    NA|                    NA|                   NA|                    NA|                    NA| FALSE|                   NA|          NA|
|   chr1|  400000|  500000|                    NA|                    NA|                   NA|                    NA|                    NA| FALSE|                   NA|          NA|
|   chr1|  500000|  600000|                    NA|                    NA|                   NA|                    NA|                    NA| FALSE|                   NA|          NA|
|   chr1|  600000|  700000|                    NA|                    NA|                   NA|                    NA|                    NA| FALSE|                   NA|          NA|
|   chr1|  700000|  800000|                    NA|                    NA|                   NA|                    NA|                    NA| FALSE|                   NA|          NA|
|   chr1|  800000|  900000|   -0.0314176469473365|   -0.0407265835884164|   0.0174236594025126|   -0.0113639420249305|   0.00120722229061954|  TRUE|     -0.8689459184024|          NA|

Command `help tmp0` shows some useful information about this table.

    help tmp0

    {
        "Name": "P004200_normalized_bin_counts",
        "Path": "s3://grail-clinical-results/27819/P004200/cna/P004200_normalized_bin_counts.tsv",
        "Columns": [
            {"Name": "chromo","Type": 5},
            {"Name": "start","Type": 3},
            {"Name": "end","Type": 3},
            {"Name": "original_depth","Type": 4},
            {"Name": "baseline_only","Type": 4},
            {"Name": "baseline_gc","Type": 4},
            {"Name": "baseline_gc_pca","Type": 4},
            {"Name": "null_depth","Type": 4},
            {"Name": "mask","Type": 2},
            {"Name": "bin_z_score","Type": 4},
            {"Name": "segment_ids","Type": 3}
        ],
        "Description": ""
    }


> Note: This output is a JSON dump of
[gql.TableAttrs](https://sg.eng.grail.com/search?q=repo:%5Egrail/grail%24+file:gql/table.go+tableattrs). The
type enums are defined in
[value_type.go](https://sg.eng.grail.com/grail/grail/-/blob/go/src/grail.com/cmd/grail-query/gql/value_type.go).

The next statement creates a new table that contains rows where `participant_id` starts with `ccga_40` (e.g., ccga_401, ccga_4010).

    ccga1.wgs_wbc | filter(string_has_prefix(&participant_id, "ccga_40")) | map({&participant_id, &analysis_lims_sample_id, &output_cna_bin_counts_file})
    


|  #| participant_id| analysis_lims_sample_id|                                                      output_cna_bin_counts_file|
|---|---------------|------------------------|--------------------------------------------------------------------------------|
|  0|       ccga_401|                 P0093T0| s3://grail-clinical-results/36149/P0093T0/cna/P0093T0_normalized_bin_counts.tsv|
|  1|       ccga_406|                 P00B6N0| s3://grail-clinical-results/37484/P00B6N0/cna/P00B6N0_normalized_bin_counts.tsv|
|  2|       ccga_407|                 P00ABQ1| s3://grail-clinical-results/36230/P00ABQ1/cna/P00ABQ1_normalized_bin_counts.tsv|
|  3|      ccga_4038|                 P009VR0| s3://grail-clinical-results/36197/P009VR0/cna/P009VR0_normalized_bin_counts.tsv|
|  4|      ccga_4049|                 P007EA0| s3://grail-clinical-results/37289/P007EA0/cna/P007EA0_normalized_bin_counts.tsv|
|  5|      ccga_4075|                 P006S01| s3://grail-clinical-results/37248/P006S01/cna/P006S01_normalized_bin_counts.tsv|
|  6|      ccga_4077|                 P0077G2| s3://grail-clinical-results/37310/P0077G2/cna/P0077G2_normalized_bin_counts.tsv|
|  7|      ccga_4078|                 P0080P0| s3://grail-clinical-results/37896/P0080P0/cna/P0080P0_normalized_bin_counts.tsv|
|  8|      ccga_4078|                 P0080P9| s3://grail-clinical-results/38334/P0080P9/cna/P0080P9_normalized_bin_counts.tsv|


### 1.4. Extracting stats from fragment files

Imagine you have file `testdata/priofiles.tsv` that looks like below:

        path
        s3://grail-clinical-results/21832/P0097R0/fragments/fragments.prio
        s3://grail-clinical-results/22267/P00A140/fragments/fragments.prio
        s3://grail-clinical-results/22220/P009310/fragments/fragments.prio


The following script will list fragments longer than 150bps.

    priofiles := read(`testdata/priofiles.tsv`);
    priofiles
      | map(read(&path) | firstn(3))
      | flatten()
      | filter(&length > 150)
      | map({&reference, &start, &length})
    


| #|                reference| start| length|
|--|-------------------------|------|-------|
| 0| AC000385.2_111814-113563|  1230|    187|
| 1| AC000385.2_111814-113563|  1243|    181|
| 2| AC000385.2_111814-113563|  1245|    183|
| 3| AB000882.1_134248-136276|  1147|    157|
| 4| AB000882.1_134248-136276|  1147|    157|



> Note: `| firstn(3)` is added to run the code quickly.
It will cause only the first three fragments to be read for each file.
Change the 2nd line to `map(read(&path))` to read all the fragments.

`flatten` is a function that takes a nested table and merges the rows in the
inner tables into a flat table. In the above example,  the `map(read(..)...)` line
produces a nested table, where each row is another table containing fragments of
a .prio file. `flatten()` will create a flat list of fragments.

    priofiles
     | map(read(&path) | firstn(3))
     | flatten(subshard:=true)
     | reduce(&length, |a,b|(a+b), map:=1)
    


| #| key| value|
|--|----|------|
| 0| 187|     1|
| 1| 181|     1|
| 2| 183|     1|
| 3| 139|     1|
| 4| 144|     2|
| 5| 124|     1|
| 6| 157|     2|



> Note: The "subshard" argument to `flatten`
is an optimization hint that tells gql to split each
subtable in the subsequence reduce. As a general rule of thumb, you should
set `subshard:=true` if there are a small number of subtables (~1000s).

### 1.5. Intersecting bincounts and a BED file

Lets pick the bincounts files from wgs_wbc samples.

    bincounts_20180409 := ccga1.wgs_wbc | map({bincounts:read(&output_cna_bin_counts_file)})
    


|    #| bincounts|
|-----|----------|
|    0|      tmp0|
|    1|      tmp1|
|    2|      tmp2|
|    3|      tmp3|


x contains just the `output_cna_bin_counts_file` column in `wgs_cfdna.tsv`. The
column values are table containing bincount rows.

We then load a BED file. It lists (chromosome, start, end) regions that of some
interest.

    test4_bed := read("testdata/test4.bed")


| #| chrom|  start|    end| featname|
|--|------|-------|-------|---------|
| 0|  chr1|      5|     85|  region1|
| 1|  chr1|    100|    200|  region2|
| 2|  chr1| 300000| 300180|  region3|
| 3|  chr2|    300|    382|  region4|



The following `join` expression creates a new table that lists a subset of bins
that intersect with the BED file.

    // Use just the first sample
    bincounts0 := (bincounts_20180409 | pick(true)).bincounts;
    join({bed: test4_bed, bincounts: bincounts0}, bed.chrom==bincounts.chromo && bed.start < bincounts.end && bed.end > bincounts.start)
    


| #| bed_chrom| bed_start| bed_end| bed_featname| bincounts_chromo| bincounts_start| bincounts_end| bincounts_original_depth| bincounts_baseline_only| bincounts_baseline_gc| bincounts_baseline_gc_pca| bincounts_null_depth| bincounts_mask| bincounts_bin_z_score| bincounts_segment_ids|
|--|----------|----------|--------|-------------|-----------------|----------------|--------------|-------------------------|------------------------|----------------------|--------------------------|---------------------|---------------|----------------------|----------------------|
| 0|      chr1|         5|      85|      region1|             chr1|               0|        100000|                       NA|                      NA|                    NA|                        NA|                   NA|          false|                    NA|                    NA|
| 1|      chr1|       100|     200|      region2|             chr1|               0|        100000|                       NA|                      NA|                    NA|                        NA|                   NA|          false|                    NA|                    NA|
| 2|      chr1|    300000|  300180|      region3|             chr1|          300000|        400000|                       NA|                      NA|                    NA|                        NA|                   NA|          false|                    NA|                    NA|
| 3|      chr2|       300|     382|      region4|             chr2|               0|        100000|                       NA|                      NA|                    NA|                        NA|                   NA|          false|                    NA|                    NA|



While this example works, it is slow since the `join` statement creates a
cross-product of bed rows and bincount rows, then filters the result. Because we
very often filter rows by BED targets, GQL provides a special function called
(joinbed)[#joinbed]. The following expression is same as the above one.

    bincounts0 | joinbed(test4_bed, chrom:=&chromo)


| #| chromo|  start|    end| original_depth| baseline_only| baseline_gc| baseline_gc_pca| null_depth|  mask| bin_z_score| segment_ids|
|--|-------|-------|-------|---------------|--------------|------------|----------------|-----------|------|------------|------------|
| 0|   chr1|      0| 100000|             NA|            NA|          NA|              NA|         NA| false|          NA|          NA|
| 1|   chr1| 300000| 400000|             NA|            NA|          NA|              NA|         NA| false|          NA|          NA|
| 2|   chr2|      0| 100000|             NA|            NA|          NA|              NA|         NA| false|          NA|          NA|



### 1.6. Distributed execution

Functions `map`, `reduce`, `cogroup`, `sort`, and `minn` accept argument
`shards`. When set, it causes the functions to execute in parallel.  By default
they will run on the local machine, using multiple CPUs. Invoking GQL with the
following flags will cause these functions to use AWS EC2 spot instances.

    gql --bigslice.system=ec2-gql --bigslice.parallelism=1024 scripts/cpg-frequency.gql

The `--bigslice.system` flag tells GQL to use AWS. `--bigslice.parallelism` flag
sets the maximum number of CPU to use.  GQL allocates m4.16x machines, with
64CPUs each.  Thus, the actual number of machines in the above example will be
capped at 16 (=1024/64).

The following example computes a very fine-grained bincount for the CCGA1
training set.  It is an example of map followed by reduce.  It takes about 6hrs
using 16 EC2 machines.

    {// Compute bincounts with width 1, for wgs_cfdna young-and-healthy trainingset.
    x := ccga | pick(&date==2018-04-09);
    
    // young_and_healthy is a list of *.prio tables of young and healthy patients in wgs_cfdna.
    young_and_healthy := join({cfdna: x.wgs_cfdna, clinical: x.clinical},
       cfdna.patient_id==clinical.patient_id && clinical.ynghvfl,
       map:=(readdir(cfdna.analysis_results_location).fragments_fragments_prio | firstn(10)));
    
    // List the fragments in all the *.prio files
    // "subshard:=true" causes individual *.prio table to be sharded during reduce() scan.
    locs := young_and_healthy | firstn(3) | flatten(subshard:=true);
    
    // Pick long fragments from the locs table, then collace by their start coordinates.
    // The reduce phase runs in a 4096-parallel fashion.
    locs
      | filter(&length >= 165)
      | reduce({&reference,&start}, |a,b|(a+b), map:=1, shards:=16)
    }


|  #|                                            key| value|
|---|-----------------------------------------------|------|
|  0|    {reference:AC002479.1_45018-50970,start:39}|     1|
|  1| {reference:AC000385.2_111814-113563,start:648}|     1|
|  2|     {reference:AC002479.1_45018-50970,start:0}|     4|
|  3| {reference:AC000385.2_111814-113563,start:613}|     1|
|  4| {reference:AC000385.2_111814-113563,start:620}|     1|
|  5|    {reference:AC002479.1_45018-50970,start:22}|     1|
|  6|    {reference:AC002479.1_45018-50970,start:20}|     2|
|  7| {reference:AC000385.2_111814-113563,start:658}|     1|
|  8|     {reference:AC002479.1_45018-50970,start:1}|     1|
|  9| {reference:AC000385.2_111814-113563,start:606}|     1|
| 10|    {reference:AC002479.1_45018-50970,start:10}|     1|
| 11| {reference:AC000385.2_111814-113563,start:609}|     1|
| 12| {reference:AC000385.2_111814-113563,start:671}|     1|
| 13|    {reference:AC002479.1_45018-50970,start:25}|     1|



### 1.7. More examples

These examples can be found under [go/src/grail.com/grail-query/scripts](https://sg.eng.grail.com/grail/grail/-/tree/go/src/grail.com/cmd/grail-query/scripts).

#### 1.7.1. ex1.gql: intersect bincounts file and a BED file.

    {// Intersect bincounts file and a BED file.
    
    // See also ex3.gql. Ex1 may be easier to understand, but slower.
    bed := read(`testdata/test4.bed`);
    
    // Pick wgs_wbc.tsv file for the given date
    wbc := (ccga | pick(&date == 2018-04-09)).wgs_wbc;
    
    // Pick bincount files for patients < 105.
    wbc_bincounts := wbc | map({path:&output_cna_bin_counts_file}, filter:=int(&patient_id)<105);
    
    // Subset the contents only for chr1 and chr2 for speed
    chr12 := wbc_bincounts | map(filter(&path, &chromo == "chr1" || &chromo == "chr2"));
    flattened_chr12 := chr12 | flatten();
    
    // Then pick only those chrom ranges that intersect with the test.bed file.
    join({bed:bed, bc:flattened_chr12}, bed.chrom==bc.chromo && bed.start < bc.end && bed.end > bc.start);
    }


|   #| bed_chrom| bed_start| bed_end| bed_featname| bc_chromo| bc_start| bc_end| bc_original_depth| bc_baseline_only| bc_baseline_gc| bc_baseline_gc_pca| bc_null_depth| bc_mask| bc_bin_z_score| bc_segment_ids|
|----|----------|----------|--------|-------------|----------|---------|-------|------------------|-----------------|---------------|-------------------|--------------|--------|---------------|---------------|
|   0|      chr1|         5|      85|      region1|      chr1|        0| 100000|                NA|               NA|             NA|                 NA|            NA|   false|             NA|             NA|
|   1|      chr1|       100|     200|      region2|      chr1|        0| 100000|                NA|               NA|             NA|                 NA|            NA|   false|             NA|             NA|
|   2|      chr1|    300000|  300180|      region3|      chr1|   300000| 400000|                NA|               NA|             NA|                 NA|            NA|   false|             NA|             NA|
|   3|      chr1|         5|      85|      region1|      chr1|        0| 100000|                NA|               NA|             NA|                 NA|            NA|   false|             NA|             NA|
|   4|      chr1|       100|     200|      region2|      chr1|        0| 100000|                NA|               NA|             NA|                 NA|            NA|   false|             NA|             NA|
|   5|      chr1|    300000|  300180|      region3|      chr1|   300000| 400000|                NA|               NA|             NA|                 NA|            NA|   false|             NA|             NA|
|   6|      chr1|         5|      85|      region1|      chr1|        0| 100000|                NA|               NA|             NA|                 NA|            NA|   false|             NA|             NA|
|   7|      chr1|       100|     200|      region2|      chr1|        0| 100000|                NA|               NA|             NA|                 NA|            NA|   false|             NA|             NA|
|   8|      chr1|    300000|  300180|      region3|      chr1|   300000| 400000|                NA|               NA|             NA|                 NA|            NA|   false|             NA|             NA|


#### 1.7.2. ex2.gql: merge bincounts for multiple samples

    {// Map binned-genomic-position to original_depth (bincount)
    
    // Merge bincounts file from wgs_wbc and wgs_cfdna.
    // Pick wgs_wbc.tsv files for the given date
    wbc := (ccga | pick(&date == 2018-04-09)).wgs_wbc;
    
    // Pick only the bincounts column.
    wbc_bincounts := wbc
      | map({&output_cna_bin_counts_file})
      | firstn(3); // pick only the first three samples for speed
    
    // Do the same for wgs_cfdna
    cfdna := (ccga | pick(&date == 2018-04-09)).wgs_cfdna;
    cfdna_bincounts := cfdna
      | map({&output_cna_bin_counts_file})
      | firstn(3); // pick only the first three samples for speed
    
    // Merge the two sets of files. The result is a single-column (output_cna_bin_counts_file) table containing references to
    // bincounts file.
    bincounts := concat(wbc_bincounts, cfdna_bincounts);
    
    // Flattening this file will merge the contents of the bincounts file into one big TSV.
    flatten(bincounts)
      | filter(&chromo=="chr1" && &start >= 4400000 && &end < 4700000)
    }


|  #| chromo|   start|     end|     original_depth|        baseline_only|          baseline_gc|      baseline_gc_pca|            null_depth| mask|         bin_z_score| segment_ids|
|---|-------|--------|--------|-------------------|---------------------|---------------------|---------------------|----------------------|-----|--------------------|------------|
|  0|   chr1| 4400000| 4500000| 0.0604300217470676|  -0.0186984637505173|   0.0100296284782812|  0.00556808908424332|   0.00569775988984644| true|   0.632664310869752|           1|
|  1|   chr1| 4500000| 4600000| 0.0527460201744692|  -0.0106216714290025|   0.0186516851770958|    0.013650580111456|   0.00189648262847797| true|    1.22765369492603|           1|
|  2|   chr1| 4400000| 4500000| 0.0722048239189877| -0.00767488423300133| -0.00966003021569688|  -0.0109353687541571|    0.0113389208359297| true|   -1.30514769624456|           1|
|  3|   chr1| 4500000| 4600000| 0.0748022486854292|   0.0106831172239733|  0.00863407201215128|  0.00224728753977069|    0.0113206172753892| true| -0.0618306056281056|           1|
|  4|   chr1| 4400000| 4500000|  0.132231170140113|   0.0519906625328862|    0.028318472530195|  0.00435959782604009|   0.00188803000114035| true|   0.490765025667235|           1|
|  5|   chr1| 4500000| 4600000|  0.111203860692816|   0.0467248985333968|   0.0226369946710024| -0.00128770532264326|   -0.0030282806531593| true|  -0.461567591239798|           1|
|  6|   chr1| 4400000| 4500000|  0.167040694305716|  -0.0557553160151578|  -0.0236284124357816| -0.00102941166253482|   0.00606144920375562| true|  -0.237157396183639|           1|
|  7|   chr1| 4500000| 4600000|  0.169863611571754|  -0.0621850562272939|  -0.0293927332254693| -0.00592858669723591|   0.00331720360915966| true|  -0.388908324625219|           1|
|  8|   chr1| 4400000| 4500000|  0.338243258917126|    0.116644946556969|   0.0250942315058141|  0.00278800506400071|   0.00170965238647414| true|   0.188891718772645|           1|
|  9|   chr1| 4500000| 4600000|  0.326524324632732|    0.095673780881525|  0.00265875640496815| -0.00811114629834231|   0.00172998779944622| true|  -0.599450349887104|           1|
| 10|   chr1| 4400000| 4500000|  0.214745190194047| -0.00371095529258575| -0.00309661951373303|  -0.0132794439640649| -0.000425476979920791| true|   -1.60434253580274|           1|
| 11|   chr1| 4500000| 4600000|  0.210014752945729|  -0.0176935768209954|  -0.0168864430328646|  -0.0250244999016134|   -0.0065082727582253| true|   -2.23100795766009|           1|



#### 1.7.3. ex3.gql: map binned-genomic-position to original_depth (bincount)

    {// Pick regions of bincounts that intersect with a BED file.
    bed := read("testdata/cancer_genes_cos_positive_sorted.bed")
      | firstn(5); // use only the first five regions for speed
    
    // Pick wgs_cfdna.tsv file for the given date
    wbc := (ccga | pick(&date == 2018-04-09)).wgs_cfdna;
    
    // Create a new table, where each row is {sample_id, bincount tsv file}
    bincounts := wbc
      | map({sample_id: &analysis_lims_sample_id, &output_cna_bin_counts_file})
      | firstn(5);  // for speed
    
    // For each bincount tsv file found in bincounts, intersect it with the bed file.
    intersected_bincounts := bincounts
      | map({&sample_id, bin_counts: &output_cna_bin_counts_file | joinbed(bed, chrom:=|r|r.chromo)});
    
    // Transpose the result.
    output := intersected_bincounts | transpose({&sample_id}, {&chromo,&start,&end,&original_depth});
    output
    }


| #| sample_id| chr1_2400000_2500000| chr1_2900000_3000000| chr1_3000000_3100000| chr1_3100000_3200000| chr1_3200000_3300000| chr1_3300000_3400000|
|--|----------|---------------------|---------------------|---------------------|---------------------|---------------------|---------------------|
| 0|   P0012B0|   0.0757370005706921|    0.162633591078803|    0.175811892486609|    0.190178935875402|    0.167474358164801|    0.158403131086637|
| 1|   P008YP0|    0.348446992021829|    0.412213475761098|    0.462157858491464|    0.461528795924875|    0.436154766813461|    0.431798517516055|
| 2|   P001ZN0|     0.11475141587537|    0.239156632647729|     0.24807647509821|     0.26246842547942|     0.23706104200645|     0.24380710042482|
| 3|   P0065M0|    0.258239687243892|    0.347404234085828|    0.382090529982255|    0.388051786716601|    0.368430405272561|    0.345687835519221|
| 4|   P005ZK0|    0.207964031912892|    0.310372361225768|     0.33733452948412|    0.345626749324009|     0.32301191740844|    0.313654767560878|



#### 1.7.4. frag-length-historgram.gql: compute length distribution of fragments.

    {// Compute length distribution of fragments.
    young_and_healthy := young_and_healthy_frag_files(2018-04-09)
      | firstn(3); // for speed
    
    locs := young_and_healthy
      | map(|yh| (yh.prio
                    | firstn(100) // for speed
                    | map(|f|{f.reference, f.length, yh.fragment_count})));
    
    flatten(locs, subshard:=true)
      | reduce(&length, |a,b|(a+b), map:=1.0 / float(&fragment_count), shards:=8)
    }


|  #| key|                  value|
|---|----|-----------------------|
|  0| 128| 1.3125940773912346e-09|
|  1| 155| 1.3125940773912346e-09|
|  2| 163| 6.3752529019941194e-09|
|  3| 165| 1.8878428970134267e-08|
|  4| 177|  7.783747365274474e-09|
|  5| 183|  9.814292375655456e-09|
|  6| 188|  5.314071847267162e-09|
|  7| 194|  4.751633551052571e-09|
|  8| 211| 1.8750323736058254e-09|


#### 1.7.5. cpg-frequency.gql: frequency of methylation state at each CpG site

    {// This script computes the frequency of methylation state at each CpG site.
    // The result is a table with three columns:
    //
    // - position: cpg site index
    // - state: methylation state as defined in fragments.Fragment.Read (0: unknown, 1: unmethylated, 2: methylated, etc)
    // - value: the number of samples with the given (position, state)
    //
    // One can postprocess the output of this script to compute the β value.
    //
    cohort_label := "breast"; // part of the output filename
    cancer_type := "Breast"; // matched against the primccat column in clinical.tsv
    
    // find_prio_files picks *.prio for samples with cancer_type.
    // Output row format: {prio: table}.
    find_prio_files := func(date) {
      x := ccga | pick(&date == date);
      evaluable_samples := x.wgbs_cfdna_methyl | filter(&assay_lock_evaluable);
      cohort := x.clinical | filter(regexp_match(&primccat, cancer_type));
    
      join({sample: evaluable_samples, clinical: cohort},
          sample.patient_id==clinical.patient_id,
          map:={prio: optional_fragment_file(sample.analysis_results_location)})
    };
    
    // process_prio converts a fragment file into a flat list of <cpg_index, meth_state>.
    process_prio := func(prio_table) {
      prio_table
        | firstn(10)  // pick just the first 10 fragments for speed
        | map(&methylation_states)
        | flatten();
    };
    
    meth_states := find_prio_files(2018-04-09)
       | firstn(3) // pick the first 3 fragment files for speed
       | map(process_prio(&prio))
       | flatten(subshard:=true);
    
    meth_states
       | reduce({&pos, &value}, |a,b|(a+b), map:=1, shards:=128)
       | map({pos:&key.pos, state:&key.value, value:&value});
    }


|  #|      pos| state| value|
|---|---------|------|------|
|  0| 29834181|     2|     1|
|  1| 29834184|     2|     2|
|  2| 29834126|     1|     2|
|  3| 29834182|     5|     1|
|  4| 29834126|     2|     9|
|  5| 29834182|     2|    13|
|  6| 29834185|     2|     3|
|  7| 29834116|     1|     1|
|  8| 29834182|     1|     1|
|  9| 29834184|     5|     1|



## 2. GQL implementation overview

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

## 3. GQL syntax

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

## 4. Data types

### 4.1. Table

When you read a TSV file, GQL internally turns it into a *Table*. Table contains
a sequence of *rows*, or *structs*.  We use terms structs and rows
interchangeably.  Tables are created by loading a TSV or other table-like files
(*.prio, *.bed, etc). It is also created as a result of function invocation
(`filter`, `join`, etc).  Invoking `read("foo.tsv")` creates a table from file
"foo.tsv". Function `write(table, "foo.tsv")` saves the contents of the table in
the given file.  The `read` function supports the following file formats:

  - *.tsv : Tab-separated-value file. The list of columns must be listed in the
  first row of the file. GQL also looks for file `path_data_dictionary.tsv`. It
  it exists, the file is loaded as a tidy dictionary definition provides
  supplies column type and description. If the dictionary file is missing, GQL
  tries to guess the column type from the file contents.

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
tables, but it sometimes happens because some tidy tsv tables have inconsistent
schemata to begin with.

### 4.2. Row (struct)

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
    [struct comprehension](#10-2-1-struct-comprehension) for more details.

  - `pick(table, expr)` extracts the first row that satisfies expr from a table


### 4.3. Scalar values

GQL supports the following scalar types. The list matches the types supported in
[tidytsv
dictionaries](https://sg.eng.grail.com/grail/grail/-/blob/go/src/grail.com/tidy/dictionary/dict.go#L37).

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

## 5. Control-flow expressions

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

## 6. Code blocks

      { assignments... expr }

An expression of form `{ assignments... expr }` introduces local variables.  The
following example computes 110 (= 10 * (10+1)). The variables "x" and "y" are
valid only inside the block.

      { x := 10; y := x+1; x * y }

A block is often used as a body of a function, which we describe next.

## 7. Functions

An expression of form `|args...| expr` creates a function.  It can be assigned
to a variable and invoked later. Consider the following example:
      udf := |row| row.date >= 2018-03-05;
      read(`foo.tsv`) | filter(|row|udf(row))

It is the same as

      read(`foo.tsv`) | filter(&date >= 2018-03-05)

GQL also provides syntax sugar

      func udf(row) row.date >= 2018-03-05

It is the same as `udf := |row| row.date >= 2018-03-05`

The function body is often a [code block](#6-code-blocks).

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

The '&'-expressions introduced in [earlier examples](#1-1-basic-functions) are
syntax sugar for user-defined functions. It is translated into a
[function](#7-functions) by the GQL parser. The translation rules are the
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

## 8. Deprecated GQL constructs

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
the body is a [code block](#6-code-blocks).

## 9. Importing a GQL file

The load statement can be used to load a gql into another gql file.

Assume file `file1.gql` has the following contents:

       x := 10

Assume file `file2.gql` has the following contents:

       load `file1.gql`
       x * 2

If you evaluate file2.gql, it will print "20".

If a gql file can contain multiple load statements.  The load statement must
appear before any other statement.

## 10. Builtin functions

### 10.1. Table manipulation

#### 10.1.1. map


    _tbl | map(expr[, expr, expr, ...] [, filter:=filterexpr] [, shards:=nshards])

Arg types:

- _expr_: one-arg function
- _filterexpr_: one-arg boolean function (default: `|_|true`)
_ _nshards_: int (default: 0)

Map picks rows that match _filterexpr_ from _tbl_, then applies _expr_ to each
matched row.  If there are multiple _expr_s, the resulting table will apply each
of the expression to every matched row in _tbl_ and combine them in the output.

If _filterexpr_ is omitted, it will match any row.
If _nshards_ > 0, it enables distributed execution.
See the [distributed execution](#1-6-distributed-execution) section for more details.

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



#### 10.1.2. filter


    tbl | filter(expr [,map:=mapexpr] [,shards:=nshards])

Arg types:

- _expr_: one-arg boolean function
- _mapexpr_: one-arg function (default: `|row|row`)
- _nshards_: int (default: 0)

Functions [map](#10-1-1-map) and filter are actually the same functions, with slightly
different syntaxes.  `tbl|filter(expr, map=mapexpr)` is the same as
`tbl|map(mapexpr, filter:=expr)`.


#### 10.1.3. reduce


    tbl | reduce(keyexpr, reduceexpr [,map:=mapexpr] [,shards:=nshards])

Arg types:

- _keyexpr_: one-arg function
- _reduceexpr_: two-arg function
- _mapexpr_: one-arg function (default: `|row|row`)
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
    ordering of values in the original table, use the [cogroup](#10-1-6-cogroup)
    function instead.

  - If the source table contains only one row for particular key, the
    _reduceexpr_ is not invoked. The 'value' column of the resulting table will
    the row itself, or the value of the _mapexpr_, if the 'map' arg is set.

If _nshards_ >0, it enables distributed execution.
See the [distributed execution](#1-6-distributed-execution) section for more details.

Example: Imagine table `t0`:

|col0 | col1|
|-----|-----|
|Bat  |  3  |
|Bat  |  4  |
|Bat  |  1  |
|Cat  |  4  |
|Cat  |  8  |

`t0 | reduce(&col0, |a,b|a+b, map:=&col1)` will create the following table:

|key  | value|
|-----|------|
|Bat  | 8    |
|Cat  | 12   |

`t0 | reduce(&col0, |a,b|a+b, map:=1)` will count the occurrences of col0 values:

|key  | value|
|-----|------|
|Bat  | 3    |
|Cat  | 2    |


A slightly silly example, `t0 | reduce(&col0, |a,b|a+b, map:=&col1*2)` will
produce the following table.

|key  | value|
|-----|------|
|Bat  | 16   |
|Cat  | 24   |

> Note: `t0| reduce(t0, &col0, |a,b|a+b.col1)` looks to be the same as
`t0 | reduce(&col0, |a,b|a+b, map:=&col1)`, but the former is an error. The result of the
_reduceexpr_ must be of the same type as the inputs. For this reason, you
should always specify a _mapexpr_.


#### 10.1.4. flatten


    flatten(tbl0, tbl1, ..., tblN [,subshard:=subshardarg])

Arg types:

- _tbl0_, _tbl1_, ... : table
- _subshardarg_: boolean (default: false)

`tbl | flatten()` (or `flatten(tbl)`) creates a new table that concatenates the rows of the subtables.
Each table _tbl0_, _tbl1_, .. must be a single-column table where each row is a
another table. Imagine two tables `table0` and `table1`:

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

Then `flatten(table(table0, table1))` produces the following table

|col0 | col1|
|-----|-----|
|Cat  | 10  |
|Dog  | 20  |
|Bat  | 3   |
|Pig  | 8   |

`flatten(tbl0, ..., tblN)` is equivalent to
`flatten(table(flatten(tbl0), ..., flatten(tblN)))`.
That is, it flattens each of _tbl0_, ..., _tblN_, then
concatenates their rows into one table.

Parameter _subshard_ specifies how the flattened table is sharded, when it is used
as an input to distributed `map` or `reduce`. When _subshard_ is false (default),
then `flatten` simply shards rows in the input tables (_tbl0_, _tbl1_ ,..., _tblN_). This works
fine if the number of rows in the input tables are much larger than the shard count.

When `subshard=true`, then flatten will to shard the individual subtables
contained in the input tables (_tbl0_, _tbl1_,...,_tblN_). This mode will work better
when the input tables contain a small number (~1000s) of rows, but each
subtable can be very large. The downside of subsharding is that the flatten
implementation must read all the rows in the input tables beforehand to figure
out their size distribution. So it can be very expensive when input tables
contains many rows.


#### 10.1.5. concat


    concat(tbl...)

Arg types:

- _tbl_: table

`concat(tbl1, tbl2, ..., tblN)` concatenates the rows of tables _tbl1_, ..., _tblN_
into a new table. Concat differs from flatten in that it attempts to maintain
simple tables simple: that is, tables that are backed by (in-memory) values
are retained as in-memory values; thus concat is designed to build up small(er)
table values, e.g., in a map or reduce operation.



#### 10.1.6. cogroup


    tbl | cogroup(keyexpr [,mapexpr=mapexpr] [,shards=nshards])

Arg types:

- _keyexpr_: one-arg function
- _mapexpr_: one-arg function (default: `|row|row`)
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

`t0 | cogroup(&col0)` will create the following table:

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

`t0 | cogroup(&col0, map:=&col1)` will create the following table:

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


#### 10.1.7. firstn


    tbl | firstn(n)

Arg types:

- _n_: int

Firstn produces a table that contains the first _n_ rows of the input table.


#### 10.1.8. minn


    tbl | minn(n, keyexpr [, shards:=nshards])

Arg types:

- _n_: int
- _keyexpr_: one-arg function
- _nshards_: int (default: 0)

Minn picks _n_ rows that stores the _n_ smallest _keyexpr_ values. If _n_<0, minn sorts
the entire input table.  Keys are compared lexicographically.
Note that we also have a
`sort(keyexpr, shards:=nshards)` function that's equivalent to `minn(-1, keyexpr, shards:=nshards)`

The _nshards_ arg enables distributed execution.
See the [distributed execution](#1-6-distributed-execution) section for more details.

Example: Imagine table t0:

|col0 | col1| col2|
|-----|-----|-----|
|Bat  |  3  | abc |
|Bat  |  4  | cde |
|Cat  |  4  | efg |
|Cat  |  8  | ghi |

`minn(t0, 2, -&col1)` will create

|col0 | col1| col2|
|-----|-----|-----|
|Cat  |  8  | ghi |
|Cat  |  4  | efg |

`minn(t0, -&col0)` will create


|col0 | col1| col2|
|-----|-----|-----|
|Cat  |  4  | efg |
|Cat  |  8  | ghi |

You can sort using multiple keys using {}. For example,
`t0 | minn(10000, {&col0,-&col2})` will sort two rows first by col0, then by -col2 in case of a
tie.

|col0 | col1| col2|
|-----|-----|-----|
|Cat  |  8  | ghi |
|Cat  |  4  | efg |
|Bat  |  4  | cde |
|Bat  |  3  | abc |



#### 10.1.9. sort


    tbl | sort(sortexpr [, shards:=nshards])

`tbl | sort(expr)` is a shorthand for `tbl | minn(-1, expr)`

#### 10.1.10. join


    join({t0:tbl0,t1:tbl1,t2:tbl2}, t0.colA==t1.colB && t1.colB == t2.colC [, map:={colx:t0.colA, coly:t2.colC}])

Arg types:

- _tbl0_, _tbl1_, ..: table

Join function joins multiple tables into one. The first argument lists the table
name and its mnemonic in a struct form. The 2nd arg is the join condition.
The `map` arg specifies the format of the output rows.

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

1. `join({t0:table0, t1:table1}, t0.colA==t1.colA, map:={colA:t0.colA, colB: t0.colB, colC: t1.colC})`

This expression performs an inner join of t0 and t1.

|colA | colB| colC|
|-----|-----|-----|
|Cat  | 3   | red |


2. `join({t0:table0, t1:table1}, t0.A?==?t1.A,map:={A:t0.A, A2:t1.A,B:t0.B, c:t1.C})`

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

#### 10.1.11. transpose


    tbl | transpose({keycol: keyexpr}, {col0:expr0, col1:expr1, .., valcol:valexpr})

Arg types:

_keyexpr_: one-arg function
_expri_: one-arg function
_valexpr_: one-arg function

Transpose function creates a table that transposes the given table,
synthesizing column names from the cell values. _tbl_ must be a two-column table
created by [cogroup](#10-1-6-cogroup). Imagine table t0 created by cogroup:

t0:

|key  |value |
|-----|------|
|120  | tmp1 |
|130  | tmp2 |


Each cell in the `value` column must be another table, for example:

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


`t0 | transpose({sample_id:&key}, {&chrom, &start, &end, &count})` will produce
the following table.


|sample_id| chr1_0_100| chr1_100_200| chr2_0_100| chr2_100_200|
|---------|-----------|-------------|-----------|-------------|
|120      |   111     |   123       |   234     |    NA       |
|130      |   444     |   456       |   NA      |   478       |

The _keyexpr_ must produce a struct with >= 1 column(s).

The 2nd arg to transpose must produce a struct with >= 2 columns. The last column is used
as the value, and the other columns used to compute the column name.



#### 10.1.12. gather


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

`t0 | gather("col1", "col2, key:="name", value:="value")` will produce the following table:

| col0| name| value|
|-----|-----|------|
|  Cat| col1|    30|
|  Cat| col2|    31|
|  Dog| col1|    40|
|  Dog| col2|    41|
		

#### 10.1.13. spread


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

`t0 | spread("col1", "col2, key:="col1", value:="col2")` will produce the following table:

| col0| 30| 40|
|-----|---|---|
|  Cat| 31|   |
|  Dog|   | 41|

Note the blank cell values, which may require the use the function to contains to
test for the existence of a field in a row struct in subsequent manipulations.


#### 10.1.14. collapse


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

`t0 | collapse("col1", "col2")` will produce the following table:

|col0 | col1| col2|
|-----|-----|-----|
|Cat  | 30  | 41  |

Note that the collapse will stop if one of the specified columns has multiple
values, for example for t0 below:

|col0 | col1| col2|
|-----|-----|-----|
|Cat  | 30  |     |
|Cat  | 31  | 41  |

`t0 | collapse("col1", "col2")` will produce the following table:

|col0 | col1| col2|
|-----|-----|-----|
|Cat  | 30  |     |
|Cat  | 30  | 41  |



#### 10.1.15. joinbed


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
- mapexpr: two-arg function (srcrow, bedrow) (default: `|srcrow,bedrow|srcrow`)

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



#### 10.1.16. count


tbl | count()

Count counts the number of rows in the table.

Example: imagine table t0:

| col1|
|-----|
|  3  |
|  4  |
|  8  |

`t0 | count()` will produce 3.


#### 10.1.17. pick


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

`t0 | pick(&col1>=20)` will return {Dog:20}.
`t0 | pick(|row|row.col1>=20)` is the same thing.


#### 10.1.18. table


table(expr...)

Arg types:

- _expr_: any

Table creates a new table consisting of the given values.

#### 10.1.19. readdir

Usage: readdir(path)

readdir creates a Struct consisting of files in the given directory.  The field
name is a sanitized pathname, value is either a Table (if the file is a .tsv,
.btsv, etc), or a string (if the file cannot be parsed as a table).

#### 10.1.20. table_attrs


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

#### 10.1.21. force


    tbl | force()

Force is a performance hint. It is logically a no-op; it just
produces the contents of the source table unchanged.  Underneath, this function
writes the source-table contents in a file. When this expression is evaluated
repeatedly, force will read contents from the file instead of
running _tbl_ repeatedly.


#### 10.1.22. tidydata


    tidydata(dataset, version, tableset[, filters])

Example:

    c := tidydata("ccga1", "compass_e2e.1", "analysis");
    c.clinical // dump the clinical table for ccga1 e2e.

Arg types:

- _dataset_: the dataset name, "ccga1" or "ccga2".
_ _version_: the version to fetch. "latest" will read the latest dataset version.
- _table_: "training", "testing", etc.
- _filters_: comma-separated list of filters.

The "tidydata" function reads tables from the tidydata service. The result is a
struct (i.e., naked row), where each column is a subtable name and the value is
the subtable rows. For more information about tidydata, see:

https://docs.google.com/document/d/1qXgyyX0WIE0s9gTabENz4o-qx2IIa8aR4aMimV5faMs/edit#


### 10.2. Row manipulation


#### 10.2.1. Struct comprehension

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

#### 10.2.2. unionrow


    unionrow(x, y)

Arg types:

- _x_, _y_: struct

Example:
    unionrow({a:10}, {b:11}) == {a:10,b:11}
    unionrow({a:10, b:11}, {b:12, c:"ab"}) == {a:10, b:11, c:"ab"}

Unionrow merges the columns of the two structs.
If one column appears in both args, the value from the second arg is taken.
Both arguments must be structs.

#### 10.2.3. optionalfield

Usage: optional_field(struct, field [, default:=defaultvalue])

This function acts like struct.field. However, if the field is missing, it
returns the defaultvalue. If defaultvalue is omitted, it returns NA.

Example:

  optionalfield({a:10,b:11}, a) == 10
  optionalfield({a:10,b:11}, c, default:12) == 12
  optionalfield({a:10,b:11}, c) == NA


#### 10.2.4. contains


    contains(struct, field)

Arg types:

- _struct_: struct
- _field_: string

Contains returns true if the struct contains the specified field, else it returns false.

### 10.3. File I/O

#### 10.3.1. read

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
"btsv", "prio", "bam", "pam". The type arg overrides file-type autodetection
based on path extension.

Example:
  read("blahblah", type:=tsv)
.

#### 10.3.2. write

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

#### 10.3.3. writecols

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

### 10.4. Predicates

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

#### 10.4.1. isnull


    isnull(expr)

isnull returns true if expr is NA (or -NA). Else it returns false.

#### 10.4.2. istable


    istable(expr)

Istable returns true if expr is a table.

#### 10.4.3. isstruct


    isstruct(expr)

Isstruct returns true if expr is a struct.

### 10.5. Arithmetic and string operators

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


#### 10.5.1. string_count


    string_count(str, substr)

Arg types:

- _str_: string
- _substr_: string

Example:
    string_count("good dog!", "g") == 2

Count the number of non-overlapping occurrences of substr in str.

#### 10.5.2. regexp_match

Usage: regexp_match(str, re)

Example:
    regexp_match("dog", "o+") == true
    regexp_match("dog", "^o") == false

Check if str matches re.
Uses go's regexp.MatchString (https://golang.org/pkg/regexp/#Regexp.MatchString).

#### 10.5.3. regexp_replace


    regexp_replace(str, re, replacement)

Arg types:

- _str_: string
- _re_: string
_ _replacement_: string

Example:
    regexp_replace("dog", "(o\\S+)" "x$1y") == "dxogy"

Replace occurrence of re in str with replacement. It is implemented using Go's
regexp.ReplaceAllString (https://golang.org/pkg/regexp/#Regexp.ReplaceAllString).

#### 10.5.4. string_len


    string_len(str)

Arg types:

- _str_: string


Example:
    string_len("dog") == 3

Compute the length of the string. Returns an integer.

#### 10.5.5. string_has_suffix


    string_has_suffix(str, suffix)

Arg types:

- _str_: string
- _suffix_: string

Example:
    string_has_suffix("dog", "g") == true

Checks if a string ends with the given suffix

#### 10.5.6. string_has_prefix


    string_has_prefix(str, prefix)

Arg types:

- _str_: string
- _prefix_: string

Example:
    string_has_prefix("dog", "d") == true

Checks if a string starts with the given prefix

#### 10.5.7. string_replace


    string_replace(str, old, new)

Arg types:

- _str_: string
- _old_: string
_ _new_: string

Example:
    regexp_replace("dogo", "o" "a") == "daga"

Replace occurrence of old in str with new.

#### 10.5.8. substring


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


#### 10.5.9. sprintf


    sprintf(fmt, args...)

Arg types:

- _fmt_: string
- _args_: any

Example:
    sprintf("hello %s %d", "world", 10) == "hello world 10"

Builds a string from the format string. It is implemented using Go's fmt.Sprintf
The args cannot be structs or tables.

#### 10.5.10. string


    string(expr)

Examples:
    string(123.0) == "123.0"
    string(NA) == ""
    string(1+10) == "11"

The string function converts any scalar expression into a string.
NA is translated into an empty string.


#### 10.5.11. int


    int(expr)

Int converts any scalar expression into an integer.
Examples:
    int("123") == 123
    int(1234.0) == 1234
    int(NA) == 0

NA is translated into 0.
If expr is a date, int(expr) computes the number of seconds since the epoch (1970-01-01).
If expr is a duration, int(expr) returns the number of nanoseconds.


#### 10.5.12. float


    float(expr)

The float function converts any scalar expression into an float.
Examples:
    float("123") == 123.0
    float(1234) == 1234.0
    float(NA) == 0.0

NA is translated into 0.0.
If expr is a date, float(expr) computes the number of seconds since the epoch (1970-01-01).
If expr is a duration, float(expr) returns the number of seconds.


#### 10.5.13. hash64


    hash64(arg)

Arg types:

- _arg_: any


Example:
    hash64("foohah")

Compute the hash of the arg. Arg can be of any type, including a table or a row.
The hash is a positive int64 value.

#### 10.5.14. land


    land(x, y)

Arg types:

- _x_, _y_: int

Example:
    land(0xff, 0x3) == 3

Compute the logical and of two integers.

#### 10.5.15. lor

Usage: lor(x, y)
Example:
    lor(0xff, 0x3) == 255

Compute the logical or of two integers.

#### 10.5.16. isset


    isset(x, y)

Arg types:

- _x_, _y_: int

Example:
    isset(0x3, 0x1) == true

Compute whether all bits in the second argument are present in the first.
Useful for whether flags are set.

### 10.6. Logical operators

    !expr

### 10.7. Miscellaneous functions

#### 10.7.1. print


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




## 11. Future plans

- Invoking R functions inside GQL.

- Invoking GQL inside R.

- Reading VCF and other non-tsv files.

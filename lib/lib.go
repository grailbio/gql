// Generated from lib/*.gql. DO NOT EDIT.
package lib
var Script = 	"// sample(table, N)\n" +
	"//\n" +
	"// sample picks 1/N rows deterministically randomly from the given table.\n" +
	"// N must be an integer. The bollowing example picks one out of 1000\n" +
	"// rows from foo.tsv.\n" +
	"//\n" +
	"//  read(`foo.tsv`) | sample(1000)\n" +
	"//\n" +
	"func sample(x, ratio) filter(x, hash64(_) % ratio == 0);\n" +
	"\n" +
	"// basename(path)\n" +
	"//\n" +
	"// The same as go's filepath.Base.\n" +
	"func basename(path) regexp_replace(path, \".*/([^/]+)$\", \"${1}\")\n"

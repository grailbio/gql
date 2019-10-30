package gql

import (
	"bytes"
	"html"
	"html/template"
	"strings"

	"github.com/grailbio/base/log"
)

/* Documentation markup convensions:

we use markdown, with the following exceptions.

Section marker:

    # Header
    ## Header

Text style:

  ::codeblock::

    it is translated into `codeblock` in markup, or ##codeblock## in remarkup. "::"
    is U+29DA.

   *bold*
   _italic_
   [text](link)
*/

// docVars is used during document template expansion.
type docVars struct {
	TableVars template.HTML
}

var tableVarDoc = `

- Variable "_" is bound to the current row.

- A symbol of form $col expands to _.col. It is a convenience
  shorthand for reading column "col" of the current row.

- If the "row" argument is set,
  then the row variable will be named after the row argument. the following
  three expressions have the same effect:

` + "```" + `
    map(tbl, $date >= 2018-04-05)
    map(tbl, xxx.date >= 2018-04-05, row:=xxx)
    map(tbl, _.date >= 2018-04-05)
` + "```\n"

func expandDocString(str string) string {
	if !strings.Contains(str, "{{.TableVars}}") {
		// Some docs (e.g., writecol) contain template strings themselves.  Don't
		// expand them.
		return str
	}
	tmpl, err := template.New("gqldoc").Parse(str)
	if err != nil {
		log.Panicf("template.new: %v", err)
	}
	out := bytes.NewBuffer(nil)
	if err := tmpl.Execute(out, docVars{TableVars: template.HTML(tableVarDoc)}); err != nil {
		log.Panicf("template.execute: %v", err)
	}
	return html.UnescapeString(out.String())
}

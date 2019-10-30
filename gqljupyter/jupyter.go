package main

// This file implements Jupyter kernel for GQL.

import (
	"context"
	"encoding/hex"
	"io/ioutil"
	"math/rand"
	"runtime/debug"
	"strings"
	"unicode"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/vcontext"
	"github.com/yunabe/lgo/jupyter/gojupyterscaffold"
	"github.com/grailbio/gql/gql"
	"github.com/grailbio/gql/termutil"
)

// JupyterHandler implements gojupyterscaffold.RequestHandlers.
type jupyterHandler struct {
	sess      *gql.Session
	execCount int
	tmpVars   *gql.TmpVars
}

// HandleKernelInfo implements gojupyterscaffold.RequestHandlers.
func (*jupyterHandler) HandleKernelInfo() gojupyterscaffold.KernelInfo {
	return gojupyterscaffold.KernelInfo{
		ProtocolVersion:       "5.2",
		Implementation:        "gql",
		ImplementationVersion: "0.0.1",
		LanguageInfo: gojupyterscaffold.KernelLanguageInfo{
			Name: "gql",
		},
		Banner: "gql",
	}
}

// sendJupyterResult sends a result of expression evaluation to the Jupyter
// client.  contentType is the mime type, e.g., "text/html" or "text/markdown".
func sendJupyterResult(contentType, content string, cb func(data *gojupyterscaffold.DisplayData, update bool)) {
	// Generate a random display ID string. We never redisplay a result now.
	var buf [16]byte
	rand.Read(buf[:])
	var enc [32]byte
	hex.Encode(enc[:], buf[:])
	data := &gojupyterscaffold.DisplayData{
		Data:      map[string]interface{}{contentType: content},
		Transient: map[string]interface{}{"display_id": enc},
	}
	cb(data, false)
}

// Eval evaluates the given GQL expression and prints the results using as
// specified by printArgs.
func (h *jupyterHandler) eval(ctx context.Context, code string, printArgs gql.PrintArgs) (err error) {
	defer func() {
		if e := recover(); e != nil {
			log.Printf("Recovered from error: %v: %v", err, string(debug.Stack()))
			err = errors.E(e, "eval `"+code+"`")
		}
	}()
	log.Printf("eval: %s", code)
	var statements []gql.ASTStatementOrLoad
	if statements, err = h.sess.Parse("(input)", []byte(code)); err != nil {
		return
	}
	val := h.sess.EvalStatements(ctx, statements)
	val.Print(ctx, printArgs)
	return
}

// JupyterHandler implements gojupyterscaffold.RequestHandlers.
//
// Arg "stream" can be invoked to send stdout and/or stderr messages to the
// caller.  Arg "display" is called to send the results of evaluation to the
// caller.
func (h *jupyterHandler) HandleExecuteRequest(
	ctx context.Context,
	r *gojupyterscaffold.ExecuteRequest,
	stream func(string, string),
	display func(data *gojupyterscaffold.DisplayData, update bool)) *gojupyterscaffold.ExecuteResult {
	h.execCount++

	result := &gojupyterscaffold.ExecuteResult{
		Status:         "ok",
		ExecutionCount: h.execCount,
	}

	var (
		mimeType = "text/html"

		// TODO(saito) figure out a way to show data more incrementally.
		out       = strings.Builder{}
		printArgs = gql.PrintArgs{
			Out:     termutil.NewHTMLPrinter(&out, 50000),
			Mode:    gql.PrintValues,
			TmpVars: h.tmpVars,
		}
	)
	if strings.HasPrefix(r.Code, "?") { // help requested.
		r.Code = r.Code[1:]
		printArgs.Mode = gql.PrintDescription
		printArgs.Out = termutil.NewBatchPrinter(&out)
		mimeType = "text/markdown"
	}
	if err := h.eval(ctx, r.Code, printArgs); err != nil {
		stream("stderr", "eval: `"+r.Code+"`: ` "+err.Error())
		result.Status = "error"
		return result
	}
	sendJupyterResult(mimeType, out.String(), display)
	h.tmpVars.Flush(h.sess)
	return result
}

// JupyterHandler implements gojupyterscaffold.RequestHandlers.
func (h *jupyterHandler) HandleComplete(req *gojupyterscaffold.CompleteRequest) *gojupyterscaffold.CompleteReply {
	log.Error.Printf("complete: %+v (not implemented)", req)
	return nil
}

// GetTokenAroundCursor extracts an GQL identifier around code[cursorPos].
// Returns "" on error.
func getIdentifierAroundCursor(code string, cursorPos int) string {
	isTokChar := func(ch rune) bool {
		return unicode.IsDigit(ch) || unicode.IsLetter(ch) || ch == '_'
	}
	runes := []rune(code)
	limit := cursorPos
	for limit < len(runes) {
		if isTokChar(runes[limit]) {
			limit++
		} else {
			break
		}
	}
	start := cursorPos
	for start >= 1 && start-1 < len(runes) {
		if isTokChar(runes[start-1]) {
			start--
		} else {
			break
		}
	}
	if start >= limit {
		return ""
	}
	return string(runes[start:limit])
}

// HandleInspect implements gojupyterscaffold.RequestHandlers.
//
// This function is called on Shift-TAB to show a tooltip.
func (h *jupyterHandler) HandleInspect(req *gojupyterscaffold.InspectRequest) *gojupyterscaffold.InspectReply {
	tok := getIdentifierAroundCursor(req.Code, req.CursorPos)
	if tok == "" {
		return nil
	}
	var (
		out       = strings.Builder{}
		printArgs = gql.PrintArgs{
			Out:  termutil.NewBatchPrinter(&out),
			Mode: gql.PrintDescription,
		}
		result = &gojupyterscaffold.InspectReply{}
	)
	// TODO(saito) Pass context from the caller.
	if err := h.eval(vcontext.Background(), tok, printArgs); err != nil {
		result.Status = "error"
		return result
	}

	result.Status = "ok"
	result.Found = true
	result.Data = map[string]interface{}{
		"text/plain": out.String(),
	}
	return result
}

// HandleIsComplete implements gojupyterscaffold.RequestHandlers.
func (*jupyterHandler) HandleIsComplete(req *gojupyterscaffold.IsCompleteRequest) *gojupyterscaffold.IsCompleteReply {
	log.Error.Printf("complete: %+v (not implemented)", req)
	return &gojupyterscaffold.IsCompleteReply{
		Status: "complete",
	}
}

// HandleGoFmt implements gojupyterscaffold.RequestHandlers.
//
// TODO(saito) This function is lgo-specific extension. There's no point in
// implementing it.
func (*jupyterHandler) HandleGoFmt(req *gojupyterscaffold.GoFmtRequest) (*gojupyterscaffold.GoFmtReply, error) {
	return nil, errors.New("gql format: not yet supported")
}

// JupyterKernel starts a Jupyter kernel server. conectionPath must store a JSON
// describing the connection back to the notebook server, as defined in
// https://jupyter-client.readthedocs.io/en/stable/kernels.html.
func jupyterKernel(ctx context.Context, connectionPath string, sess *gql.Session) {
	data, err := ioutil.ReadFile(connectionPath)
	if err != nil {
		log.Panicf("jupyterKernel: read %s: %v", connectionPath, err)
	}
	log.Printf("jupyterKernel: session started with %+v", string(data))
	server, err := gojupyterscaffold.NewServer(ctx, connectionPath,
		&jupyterHandler{
			sess:    sess,
			tmpVars: &gql.TmpVars{},
		})
	if err != nil {
		log.Panicf("create jupyter server: %v", err)
	}
	server.Loop()
	log.Printf("jupyterKernel: finsihed")
}

package termutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	gunsafe "github.com/grailbio/base/unsafe"
	"github.com/grailbio/gql/columnsorter"
	"github.com/grailbio/gql/symbol"
	"github.com/yasushi-saito/readline"
	"golang.org/x/crypto/ssh/terminal"
)

var (
	signalOnce  sync.Once
	signalState uint32 // becomes 1 on SIGINT.

	ctxMu     sync.Mutex
	activeCtx = map[*interruptibleContext]struct{}{}
)

// InstallSignalHandler sets up the process to catch SIGINT signals.  All the
// standard Printer objects will report !Ok() after a SIGINT.
func InstallSignalHandler() {
	signalOnce.Do(func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)
		go func() {
			for range ch {
				fmt.Fprintln(os.Stderr, "Interrupted")
				atomic.StoreUint32(&signalState, 1)
				ctxMu.Lock()
				for ctx := range activeCtx {
					ctx.cancel()
				}
				ctxMu.Unlock()
			}
		}()
	})
}

func signaled() bool {
	return atomic.LoadUint32(&signalState) != 0
}

// Arrange so that future Printer.Ok() calls will return Ok().  Note that if the
// process receives SIGINT in a future, Printer.Ok() becomes false.
func ClearSignal() {
	atomic.StoreUint32(&signalState, 0)
}

// InterruptibleContext implements context.Context. It reports a cancellation
// when the user sends SIGINT.
type interruptibleContext struct {
	// Underlying context. It must be a background context that doesn't report
	// cancellation or timeout on its own. This requirement is only to ensure good
	// performance, since interruptibleContext.Err() will be called very often.
	// It is usually set to vcontext.Background().
	bg context.Context
	// interrupted becomes 1 once Err() detects signaled()==true.  The value of
	// signaled() may be reset in the background, so we save it here.
	interrupted uint32
	// "ch" is the channel returned by Done().
	ch chan struct{}
}

var errInterrupted = errors.E("interrupted by user")

// Deadline implements context.Context.
func (ctx *interruptibleContext) Deadline() (time.Time, bool) { return time.Time{}, false }

// Done implements context.Context
func (ctx *interruptibleContext) Done() <-chan struct{} { return ctx.ch }

// Value implements context.Context
func (ctx *interruptibleContext) Err() error {
	if atomic.LoadUint32(&ctx.interrupted) != 0 {
		return errInterrupted
	}
	return nil
}

// Value implements context.Context
func (ctx *interruptibleContext) Value(key interface{}) interface{} {
	return ctx.bg.Value(key)
}

// String returns a human-readable string.
func (ctx *interruptibleContext) String() string {
	return fmt.Sprintf("interruptibleContext: %v", ctx.Err())
}

// REQUIRES: ctxMu is locked.
func (ctx *interruptibleContext) cancel() {
	if _, ok := activeCtx[ctx]; !ok {
		panic("withcancel: callback run twice")
	}
	delete(activeCtx, ctx)
	close(ctx.ch)
	atomic.StoreUint32(&ctx.interrupted, 1)
}

// WithCancel creates a context.Context object that reports cancellation when
// the user presses ^C. The caller must run the returned cancelfunc after use,
// or the context will leak.
//
// REQUIRES: bg must not have deadline or report cancellation on its own.
func WithCancel(bg context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := bg.Deadline(); hasDeadline {
		panic("WithCancel: the underlying context must not have deadline nor be cancellable")
	}
	ctx := &interruptibleContext{bg: bg, ch: make(chan struct{}, 1)}
	ctxMu.Lock()
	activeCtx[ctx] = struct{}{}
	ctxMu.Unlock()
	return ctx, func() {
		ctxMu.Lock()
		defer ctxMu.Unlock()
		if _, ok := activeCtx[ctx]; !ok {
			// The signal handler already cancelled this context.
			return
		}
		ctx.cancel()
	}
}

func screenSize() (int, int) {
	nCol, nRow, err := terminal.GetSize(syscall.Stdout)
	if err != nil {
		nCol, nRow = 80, 25 // an arbitrary default
	}
	nRow -= 4 // leave some space at the top of the screen
	if nRow < 4 {
		nRow = 4
	}
	return nCol, nRow
}

type Column struct {
	Name  symbol.ID
	Value string
}

// Printer is an interface for paging long outputs for an interactive shell.  It
// is a superset of io.Writer.
type Printer interface {
	// Write writes the given text data to the output. The implementation may ask
	// for "continue y/n?" in the middle when the data is long.
	Write(data []byte) (int, error)

	// WriteString is similar to Write(), but it takes a string.
	WriteString(data string)
	// WriteInt writes the value in decimal. It is equivalent to WriteString(fmt.Printf("%v", v))
	WriteInt(v int64)
	// WriteFloat writes the value in dotted decimal. It is equivalent to
	// WriteString(fmt.Printf("%v", v))
	WriteFloat(v float64)
	// WriteTable writes a table. rowCallback will be called repeatedly to
	// retrieve rows in the table.
	WriteTable(rowCallback func() ([]Column, error))

	// Ok() becomes false if the user answers 'N' to a 'continue y/n?' prompt.  Once
	// Ok() returns false, all future Ok() calls will return false, and Write and
	// WriteString become no-ops.
	Ok() bool

	// ScreenSize returns the screen (width, height), as # of characters.
	ScreenSize() (int, int)

	// Close closes the printer and releases its resources.
	Close()
}

// batchPrinter is a non-interactive printer that prints to the given output
// without paging.
type batchPrinter struct {
	out    io.Writer
	err    errors.Once
	fmtBuf [128]byte
}

// NewBatchPrinter creates a Printer that writes to the given output
// non-interactively, without any paging.
func NewBatchPrinter(out io.Writer) Printer {
	return &batchPrinter{out: out}
}

// ScreenSize implements Printer.
func (p *batchPrinter) ScreenSize() (int, int) {
	const maxInt = int(^uint(0) >> 1)
	return maxInt, maxInt
}

func (p *batchPrinter) WriteTable(rowCallback func() ([]Column, error)) {
	defaultWriteTable(p, rowCallback)
}

// Write implements Printer.
func (p *batchPrinter) Write(data []byte) (int, error) {
	n, err := p.out.Write(data)
	if err != nil {
		if p.err.Err() == nil {
			log.Error.Printf("write: %v", err)
		}
		p.err.Set(err)
	}
	return n, err
}

// Reset clears the status of the printer.
func (p *batchPrinter) Reset() {}

// Close implements Printer.
func (p *batchPrinter) Close() {}

// WriteString implements Printer.
func (p *batchPrinter) WriteString(data string) {
	p.Write(gunsafe.StringToBytes(data))
}

// WriteInt implements Printer.
func (p *batchPrinter) WriteInt(v int64) {
	p.Write(strconv.AppendInt(p.fmtBuf[:0], v, 10))
}

// WriteFloat implements Printer.
func (p *batchPrinter) WriteFloat(v float64) {
	p.Write(strconv.AppendFloat(p.fmtBuf[:0], v, 'g', -1, 64))
}

// Ok implements Printer.
func (p *batchPrinter) Ok() bool {
	return !signaled() && p.err.Err() == nil
}

// BufferPrinter is a non-interactive printer that prints to an in-memory buffer
// without paging.  Functions Bytes() and String() will retrieve the buffer
// contents.
type BufferPrinter struct {
	batchPrinter
	buf strings.Builder
}

// NewBufferPrinter creates a new, empty BufferPrinter.
func NewBufferPrinter() *BufferPrinter {
	b := &BufferPrinter{}
	b.batchPrinter.out = &b.buf
	return b
}

// Reset clears the status of the printer.
func (p *BufferPrinter) Reset() {
	p.buf.Reset()
}

// Close implements Printer.
func (p *BufferPrinter) Close() {
	p.Reset()
}

// String yields the data written via Write and WriteString. It is idempotent.
func (p *BufferPrinter) String() string {
	return p.buf.String()
}

// Len returns the number of accumulated bytes; Len() == len(String()).
func (p *BufferPrinter) Len() int {
	return p.buf.Len()
}

// NewFilePrinter creates a Printer that writes to a file.  If append==true, it
// appends contents to the file if the file already exists.
func NewFilePrinter(path string, append bool) (Printer, error) {
	openFlags := os.O_CREATE | os.O_WRONLY
	if append {
		openFlags |= os.O_APPEND
	} else {
		openFlags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, openFlags, 0644)
	if err != nil {
		return nil, errors.E("open "+path, err)
	}
	return &filePrinter{batchPrinter: batchPrinter{out: f}, f: f}, nil
}

type filePrinter struct {
	batchPrinter
	f *os.File
}

// Close implements Printer.
func (p *filePrinter) Close() {
	if err := p.f.Close(); err != nil {
		log.Error.Printf("close %v: %s", p.f.Name(), err)
	}
	p.f = nil
}

// NewFilePrinter creates a Printer that sends data to a new process.  Arg name
// is the name or path of the process, and args are the arguments to the
// process. They are passed to exec.Command.
func NewPipePrinter(name string, arg ...string) (Printer, error) {
	cmd := exec.Command(name, arg...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	pipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, errors.E(fmt.Sprintf("| %s: stdinpipe", name), err)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.E(fmt.Sprintf("| %s: start", name), err)
	}
	return &pipePrinter{
		batchPrinter: batchPrinter{out: pipe},
		name:         name,
		cmdOut:       pipe,
		cmd:          cmd}, nil
}

type pipePrinter struct {
	batchPrinter

	name   string
	cmdOut io.WriteCloser
	cmd    *exec.Cmd
}

// ScreenSize implements Printer.
func (p *pipePrinter) ScreenSize() (int, int) { return screenSize() }

// Close implements Printer.
func (p *pipePrinter) Close() {
	if err := p.cmdOut.Close(); err != nil {
		log.Error.Printf("| %s: close: %s", p.name, err)
	}
	if err := p.cmd.Wait(); err != nil {
		log.Error.Printf("| %s: wait: %s", p.name, err)
	}
	p.out = nil
	p.cmd = nil
}

type terminalPrinter struct {
	out io.Writer // Usually os.Stdout
	ok  bool      // Value of Ok()
	// Buf stores data to print. The contents are never GCed, so that when the
	// user chooses ">file" or "|less", we can dump the all the contents.
	buf bytes.Buffer
	// NextOff points to the data yet to be printed in buf.
	nextOff int
	// RemainingRows x # of rows remaining in the current page.
	remainingRows int
	// redirect becomes non-nil after the user chooses ">" or "|" at a pagination
	// prompt. Once redirect!=nil, all the subsequent outputs are sent to it
	// directly.
	redirect Printer
	fmtBuf   [128]byte
}

// NewTerminalPrinter creates a Printer that performs paging ("continue
// y/n?"). Arg "out" is usually os.Stdout.
func NewTerminalPrinter(out io.Writer) Printer {
	p := &terminalPrinter{out: out, ok: true}
	_, p.remainingRows = p.ScreenSize()
	return p
}

func (p *terminalPrinter) ScreenSize() (int, int) { return screenSize() }

var newline = []byte("\n")

// nextLine extracts and removes a single text line from p.buf.  If p.buf
// doesn't contain a newline, it returns false.
func (p *terminalPrinter) nextLine() ([]byte, bool) {
	buf := p.buf.Bytes()[p.nextOff:]
	i := bytes.IndexByte(buf, '\n')
	if i < 0 {
		return nil, false
	}
	line := buf[:i]
	p.nextOff += i + 1
	return line, true
}

// Write implements Printer.
func (p *terminalPrinter) WriteTable(rowCallback func() ([]Column, error)) {
	defaultWriteTable(p, rowCallback)
}

// defaultWriteTable dumps table rows using a unicode art.
func defaultWriteTable(p Printer, rowCallback func() ([]Column, error)) {
	writeTableHeader := func(
		col0Width int, // width of the first column that shows the row index.
		colWidth []int, // width of the rest of the columns.
		colNames []symbol.ID,
		hasMoreColumns bool) {
		// Print column names
		p.WriteString(fmt.Sprintf("║ %*s", col0Width, "#"))
		for i := range colWidth {
			p.WriteString(fmt.Sprintf("║ %*s", colWidth[i], colNames[i].Str()))
		}
		if !hasMoreColumns {
			p.WriteString("║")
		}
		p.WriteString("\n")
		// Divider row
		p.WriteString("├─")
		for j := 0; j < col0Width; j++ {
			p.WriteString("─")
		}
		for i := range colWidth {
			p.WriteString("┼─")
			for j := 0; j < colWidth[i]; j++ {
				p.WriteString("─")
			}
		}
		if !hasMoreColumns {
			p.WriteString("┤")
		}
		p.WriteString("\n")
	}

	writeTableRow := func(
		rowIndex int, // row index (0, 1, ...)
		col0Width int, // width of the first column that shows the row index.
		colWidth []int, // width of the rest of the columns.
		vals []string, // column contents for 2nd and later columns
		hasMoreColumns bool) {
		p.WriteString(fmt.Sprintf("│ %*d", col0Width, rowIndex))
		for i := range colWidth {
			p.WriteString(fmt.Sprintf("│ %*s", colWidth[i], vals[i]))
		}
		if !hasMoreColumns {
			p.WriteString("│")
		}
		p.WriteString("\n")
	}

	// Pretty-print the list of rows.
	flushRows := func(rowIndex int, rows [][]Column, screenWidth int) {
		cols := columnsorter.New()
		for _, row := range rows {
			names := []symbol.ID{}
			for _, col := range row {
				names = append(names, col.Name)
			}
			cols.AddColumns(names)
		}
		cols.Sort()
		colNames := cols.Columns()
		colWidth := make([]int, len(colNames))

		col0Width := int(math.Log10(float64(rowIndex+len(rows)))) + 1
		for i := range colNames {
			colWidth[i] = len(colNames[i].Str())
		}
		for _, row := range rows {
			for _, col := range row {
				i := cols.Index(col.Name)
				if len(col.Value) > colWidth[i] {
					colWidth[i] = len(col.Value)
				}
			}
		}

		for ci := 0; ci < len(colWidth); {
			// Print as many columns as fit in the screen.
			width := col0Width + 1 // 1 to account for the table's left border.
			startCol := ci
			for ci < len(colNames) &&
				(ci == startCol || width+colWidth[ci]+2 < screenWidth) {
				width += colWidth[ci] + 2 // +2 for table borders
				ci++
			}
			hasMoreColumns := ci < len(colWidth)
			writeTableHeader(col0Width, colWidth[startCol:ci], colNames[startCol:ci], hasMoreColumns)
			vals := make([]string, ci-startCol)
			for ri, row := range rows {
				for i := range vals {
					vals[i] = ""
				}
				for _, col := range row {
					i := cols.Index(col.Name)
					if i >= startCol && i < ci {
						vals[i-startCol] = col.Value
					}
				}
				writeTableRow(rowIndex+ri, col0Width, colWidth[startCol:ci], vals, hasMoreColumns)
			}
			if hasMoreColumns {
				p.WriteString("\n")
			}
		}
	}

	eof := false
	rowIndex := 0
	for !eof && p.Ok() {
		var rows [][]Column
		nCol, nRow := p.ScreenSize()
		for i := 0; i < nRow; i++ {
			row, err := rowCallback()
			if err != nil {
				if err != io.EOF {
					log.Printf("Failed to print the table: %v", err)
				}
				eof = true
				break
			}
			rows = append(rows, row)
		}
		flushRows(rowIndex, rows, nCol)
		rowIndex += len(rows)
	}
}

// Write implements Printer.
func (p *terminalPrinter) Write(data []byte) (int, error) {
	if p.redirect != nil {
		return p.redirect.Write(data)
	}

	p.buf.Write(data)
loop:
	for p.ok {
		if p.remainingRows <= 0 {
			resp, arg := prompt()
			switch resp {
			case respYes:
				_, p.remainingRows = p.ScreenSize()
			case respNo:
				p.ok = false
				break loop
			case respWrite, respAppend:
				f, err := NewFilePrinter(arg, resp == respAppend)
				if err != nil {
					log.Printf("open %s: %v", arg, err)
					break
				}
				p.redirect = f
				fmt.Fprintf(os.Stderr, "Writing data to %s\n", arg)
			case respPipe:
				pipe, err := NewPipePrinter(arg)
				if err != nil {
					log.Error.Printf("|%v: %v", arg, err)
					break
				}
				p.redirect = pipe
			}
			if p.redirect != nil {
				p.redirect.Write(p.buf.Bytes())
				p.buf.Reset()
				break loop
			}
		}
		line, found := p.nextLine()
		if !found {
			break
		}
		if _, err := p.out.Write(line); err != nil {
			panic(err)
		}
		if _, err := p.out.Write(newline); err != nil {
			panic(err)
		}
		p.remainingRows--
	}
	return len(data), nil
}

// WriteString implements Printer.
func (p *terminalPrinter) WriteString(data string) {
	p.Write(gunsafe.StringToBytes(data))
}

// WriteInt implements Printer.
func (p *terminalPrinter) WriteInt(v int64) {
	p.Write(strconv.AppendInt(p.fmtBuf[:0], v, 10))
}

// WriteFloat implements Printer.
func (p *terminalPrinter) WriteFloat(v float64) {
	p.Write(strconv.AppendFloat(p.fmtBuf[:0], v, 'g', -1, 64))
}

// Ok implements Printer.
func (p *terminalPrinter) Ok() bool {
	if signaled() {
		return false
	}
	if !p.ok {
		return false
	}
	if p.redirect != nil && !p.redirect.Ok() {
		return false
	}
	return true
}

// Close implements Printer.
func (p *terminalPrinter) Close() {
	p.ok = true
	p.nextOff = 0
	p.buf.Reset()
	_, p.remainingRows = p.ScreenSize()
	if p.redirect != nil {
		p.redirect.Close()
		p.redirect = nil
	}
}

type userResponse int

const (
	// respYes is the default response. It continues to output in a paginated
	// fashion.
	respYes userResponse = iota
	// respNo aborts the output.
	respNo
	// respPipe pipes the the output to another command.
	respPipe
	// respWrite writes the output to a file, truncating existing contents.
	respWrite
	// respAppend appends the output to a file
	respAppend
)

// Show an interactive prompt and parse the response.  the 2nd return value
// stores the pathname. It is set only for {respWrite, respAppned}.
func prompt() (userResponse, string) {
	for {
		s, err := readline.Readline("Continue? Yes / No / >file / >>file / |less: ")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}
		s = strings.TrimSpace(s)
		if s == "" {
			return respYes, ""
		}
		if strings.HasPrefix(s, ">>") {
			return respWrite, strings.TrimSpace(s[2:])
		}
		if strings.HasPrefix(s, ">") {
			return respWrite, strings.TrimSpace(s[1:])
		}
		if strings.HasPrefix(s, "|") {
			return respPipe, strings.TrimSpace(s[1:])
		}
		lower := strings.ToLower(s)
		if strings.HasPrefix("yes", lower) {
			return respYes, ""
		}
		if strings.HasPrefix("no", lower) || strings.HasPrefix("quit", lower) {
			return respNo, ""
		}
		fmt.Println(`- Yes: continues showing the output page by page.
- No: stops the output.
- >file: writes the output to the given file.
- >>file: appends the output to the given file.
- |command: feeds the output to the given command, typically "|less".

"Y", "y", or an empty input is the same as "Yes".  "N", "n", "Q", or "q" is the same as "No".`)
		continue
	}
}

// htmlPrinter implements Printer. It dumps contents in HTML.
type htmlPrinter struct {
	out           io.Writer
	maxTableCells int
	err           errors.Once
	fmtBuf        [128]byte
}

// NewHTMLPrinter creates a new Printer that writes contents in an HTML format.
func NewHTMLPrinter(out io.Writer, maxTableCells int) Printer {
	return &htmlPrinter{out: out, maxTableCells: maxTableCells}
}

// Write implements Printer.
func (p *htmlPrinter) Write(data []byte) (int, error) {
	n, err := p.out.Write(data)
	if err != nil {
		if p.err.Err() == nil {
			log.Error.Printf("write: %v", err)
		}
		p.err.Set(err)
	}
	return n, err
}

// Reset implements Printer.
func (p *htmlPrinter) Reset() {}

// Close implements Printer.
func (p *htmlPrinter) Close() {}

// WriteString implements Printer.
func (p *htmlPrinter) WriteString(data string) {
	p.Write(gunsafe.StringToBytes(data))
}

// WriteInt implements Printer.
func (p *htmlPrinter) WriteInt(v int64) {
	p.Write(strconv.AppendInt(p.fmtBuf[:0], v, 10))
}

// WriteFloat implements Printer.
func (p *htmlPrinter) WriteFloat(v float64) {
	p.Write(strconv.AppendFloat(p.fmtBuf[:0], v, 'g', -1, 64))
}

// Ok implements Printer.
func (p *htmlPrinter) Ok() bool {
	return !signaled() && p.err.Err() == nil
}

// WriteTable implements Printer.
func (p *htmlPrinter) WriteTable(rowCallback func() ([]Column, error)) {
	var (
		cols  = columnsorter.New()
		nCell int
		rows  [][]Column
		names []symbol.ID
	)
	for p.Ok() && nCell < p.maxTableCells {
		row, err := rowCallback()
		if err != nil {
			if err != io.EOF {
				log.Printf("Failed to print the table: %v", err)
			} else {
				p.err.Set(err)
			}
			break
		}
		names = names[:0]
		for _, col := range row {
			names = append(names, col.Name)
		}
		cols.AddColumns(names)
		rows = append(rows, row)
		nCell += len(row)
	}

	cols.Sort()
	colNames := cols.Columns()

	p.WriteString("<table><thead><tr>")
	for _, col := range colNames {
		p.WriteString("<th>" + col.Str() + "</th>\n")
	}
	p.WriteString("</tr></thead>	\n")

	vals := make([]string, len(colNames))
	for _, row := range rows {
		for i := range vals {
			vals[i] = ""
		}
		for _, col := range row {
			i := cols.Index(col.Name)
			vals[i] = col.Value
		}
		p.WriteString("<tr>")
		for _, val := range vals {
			p.WriteString("<td>" + val + "</td>\n")
		}
		p.WriteString("</tr>\n")
	}
	p.WriteString("</table>\n")
	if nCell >= p.maxTableCells {
		p.WriteString("<small>Rows omitted</small>\n")
	}
}

func (p *htmlPrinter) ScreenSize() (int, int) {
	const maxInt = int(^uint(0) >> 1)
	return maxInt, maxInt
}

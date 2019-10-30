package gql

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/termutil"
)

// Value is a unified representation of a value in gql. It can represent scalar
// values such as int64 and float64, as well as a Table or Struct. A value is
// immutable once constructed.
type Value struct {
	typ ValueType
	p   unsafe.Pointer
	v   uint64
}

// Valid returns true if it stores a value. Note that null is a valid value.
// Only a default-constructed Value returns false.
func (v Value) Valid() bool { return v.typ != InvalidType }

// Type returns the type of the value.
func (v Value) Type() ValueType { return v.typ }

// Prefetch is called to fetch the contents in the background, if possible.
// This method never blocks.
func (v Value) Prefetch(ctx context.Context) {
	switch v.typ {
	case TableType:
		v.Table(nil).Prefetch(ctx)
	case StructType:
		s := v.Struct(nil)
		n := s.Len()
		for i := 0; i < n; i++ {
			s.Field(i).Value.Prefetch(ctx)
		}
	}
}

// NullStatus is the return value of Value.Null()
type NullStatus int

const (
	// PosNull is the standard status of a null value. PosNull sorts after
	// any non-null value.
	PosNull NullStatus = 1
	// NotNull is a placeholder for a non-null value.
	NotNull NullStatus = 0
	// NegNull value is created after negating a null value (expression "-NA" will
	// create such a value).  NegNull sorts before any non-null avlue.
	NegNull NullStatus = -1
)

// NewBool creates a new boolean value.
func NewBool(v bool) Value {
	if v {
		return Value{typ: BoolType, v: 1}
	}
	return Value{typ: BoolType, v: 0}
}

// NewNull creates a new null value.
//
// REQUIRES: v != NotNull.
func NewNull(v NullStatus) Value {
	return Value{typ: NullType, v: uint64(v)}
}

var (
	// Null is a singleton instance of NA.
	Null = NewNull(PosNull)
	// True is a true Bool constant
	True = NewBool(true)
	// False is a false Bool constant
	False = NewBool(false)
)

// Null checks whether the value is null. If v is not null, it returns NotNull.
// Else, it returns either PosNull or NegNull.
func (v Value) Null() NullStatus {
	if v.typ != NullType {
		return NotNull
	}
	return NullStatus(v.v)
}

// Bool extracts a boolean value. "ast" is used only to report source code location on error.
//
// REQUIRES: v.Type()==BoolType
func (v Value) Bool(ast ASTNode) bool {
	if v.typ != BoolType {
		v.wrongTypeError(ast, "bool")
	}
	return v.v != 0
}

// NewInt creates a new integer.
func NewInt(v int64) Value {
	return Value{typ: IntType, v: uint64(v)}
}

// Int extracts an integer value. "ast" is used only to report source code location on error.
//
// REQUIRES: v.Type()==IntType
func (v Value) Int(ast ASTNode) int64 {
	if v.typ != IntType {
		v.wrongTypeError(ast, "int")
	}
	return int64(v.v)
}

// NewFloat creates a new float value.
func NewFloat(v float64) Value {
	uv := *(*uint64)(unsafe.Pointer(&v))
	return Value{typ: FloatType, v: uv}
}

// Float extracts a float64 value. "ast" is used only to report source code
// location on error.
//
// REQUIRES: v.Type()==FloatType
func (v Value) Float(ast ASTNode) float64 {
	if v.typ != FloatType {
		v.wrongTypeError(ast, "float")
	}
	return *(*float64)(unsafe.Pointer(&v.v))
}

// NewString creates a new String value.
func NewString(s string) Value {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return Value{typ: StringType, p: unsafe.Pointer(sh.Data), v: uint64(sh.Len)}
}

// NewFileName creates a new FileName value.
func NewFileName(v string) Value {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&v))
	return Value{typ: FileNameType, p: unsafe.Pointer(sh.Data), v: uint64(sh.Len)}
}

// NewEnum creates a new Enum value.
func NewEnum(v string) Value {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&v))
	return Value{typ: EnumType, p: unsafe.Pointer(sh.Data), v: uint64(sh.Len)}
}

// Str extracts the string value. "ast" is used only to report source code location on error.
//
// REQUIRES: v.Type() is one of {StringType,FileNameType,EnumType}.
func (v Value) Str(ast ASTNode) string {
	if !v.typ.LikeString() {
		v.wrongTypeError(ast, "string")
	}
	sh := reflect.StringHeader{
		Data: uintptr(v.p),
		Len:  int(v.v),
	}
	s := *(*string)(unsafe.Pointer(&sh))
	return s
}

const iso8601DateTimeFormat = "2006-01-02T15:04:05-0700"
const iso8601DateTimeFormatZ = "2006-01-02T15:04:05Z"
const iso8601DateFormat = "2006-01-02"

// ParseDateTime creates a Date or DateTime from a string.  It only accepts
// ISO8601-format strings.
func ParseDateTime(v string) Value {
	if t, err := time.Parse(iso8601DateTimeFormat, v); err == nil {
		return NewDateTime(t)
	}
	if t, err := time.Parse(iso8601DateTimeFormatZ, v); err == nil {
		return NewDateTime(t)
	}
	if t, err := time.Parse(iso8601DateFormat, v); err == nil {
		return NewDate(t)
	}

	// Handle legacy date formats for backward compatibility.
	if t, err := time.Parse("2006-01-02T15:04:05Z", v); err == nil {
		return NewDateTime(t)
	}
	if t, err := time.Parse("2006-01-02T15:04Z", v); err == nil {
		return NewDateTime(t)
	}
	if t, err := time.Parse("2006-01-02T15:04:05-07:00", v); err == nil {
		return NewDateTime(t)
	}
	if t, err := time.Parse("2006-01-02T15:04-07:00", v); err == nil {
		return NewDateTime(t)
	}
	if t, err := time.Parse("2006-01-02T15:04-07:00.000", v); err == nil {
		return NewDateTime(t)
	}
	if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
		return NewDateTime(t)
	}
	if t, err := time.Parse("2006-01-02 15:04:05.000", v); err == nil {
		return NewDateTime(t)
	}
	if t, err := time.Parse("2006-01-02 15:04:05 -0700 MST", v); err == nil {
		return NewDateTime(t)
	}

	log.Panicf("ParseDateTime: failed to parse '%v'. The format must be either '%v' (for datetime) or '%v' (for date)",
		v, iso8601DateTimeFormat, iso8601DateFormat)
	return Value{}
}

// NewDateTime creates a value of DateTimeType.
func NewDateTime(t time.Time) Value {
	return Value{
		typ: DateTimeType,
		v:   uint64(t.UnixNano()),
		p:   unsafe.Pointer(t.Location())}
}

// NewDate creates a value of DateType.
func NewDate(t time.Time) Value {
	return Value{
		typ: DateType,
		v:   uint64(t.UnixNano()),
		p:   unsafe.Pointer(t.Location())}
}

// DateTime extracts the time/date value."ast" is used only to report source code location on error.
//
// REQUIRES: v.Type() is one of {DateTimeType, DateType}
func (v Value) DateTime(ast ASTNode) time.Time {
	if !v.typ.LikeDate() {
		v.wrongTypeError(ast, "datetime")
	}
	loc := (*time.Location)(v.p)
	return time.Unix(0, int64(v.v)).In(loc)
}

// ParseDuration constructs a Value object from a human-readable duration
// string.  The duration format is defined in time.ParseDuration.
func ParseDuration(v string) Value {
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Panicf("parseduration '%s': %v", v, err)
	}
	return NewDuration(d)
}

// NewDuration creates a value of DurationType.
func NewDuration(t time.Duration) Value {
	return Value{typ: DurationType, v: uint64(t)}
}

// Int extracts an integer value. "ast" is used only to report source code location on error.
//
// REQUIRES: v.Type()==DurationType
func (v Value) Duration(ast ASTNode) time.Duration {
	if v.typ != DurationType {
		v.wrongTypeError(ast, "duration")
	}
	return time.Duration(v.v)
}

// NewChar creates a new character value.
func NewChar(ch rune) Value {
	return Value{typ: CharType, v: uint64(ch)}
}

// Char extracts the character value. "ast" is used only to report source code location on error.
//
// REQUIRES: v.Type()==CharType.
func (v Value) Char(ast ASTNode) rune {
	if v.typ != CharType {
		v.wrongTypeError(ast, "char")
	}
	return rune(v.v)
}

// NewStructFragment creates a new Value of type StructFragmentType.
func NewStructFragment(frag []StructField) Value {
	return Value{typ: StructFragmentType, p: unsafe.Pointer(&frag)}
}

// NewStruct creates a Value from a Struct.
func NewStruct(v Struct) Value {
	iface := (*goInterfaceImpl)(unsafe.Pointer(&v))
	return Value{typ: StructType, p: iface.data}
}

// Struct extracts a struct from the value. "ast" is used only to report source
// code location on error.
//
// REQUIRES: v.Type() == StructType,NullType
//
// TODO(saito) do we need to special-case null type here?
func (v Value) Struct(ast ASTNode) Struct {
	switch v.typ {
	case StructType:
		sp := (*StructImpl)(v.p)
		ip := goInterfaceImpl{
			iface: sp.iface,
			data:  v.p,
		}
		return *(*Struct)(unsafe.Pointer(&ip))
	case NullType:
		return nullStruct
	}
	v.wrongTypeError(ast, "struct")
	return nil
}

// NewTable creates a Value object from a table.
func NewTable(v Table) Value {
	return Value{typ: TableType, p: unsafe.Pointer(&v)}
}

func (v Value) wrongTypeError(ast ASTNode, expectedType string) {
	panic(fmt.Sprintf("%v:%v: expect value of type %v, but found '%v' (type %v)", ast.pos(), ast, expectedType, v, v.typ))
}

// Table extracts a table from the value.  "ast" is used only to report source
// code location on error.
//
// REQUIRES: v.Type() == TableType,NullType
//
// TODO(saito) do we need to special-case null type here?
func (v Value) Table(ast ASTNode) Table {
	switch v.typ {
	case TableType:
		return *(*Table)(v.p)
	case NullType:
		return nullTable{}
	}
	v.wrongTypeError(ast, "table")
	return nil
}

// Hash computes a hash of the value.
func (v Value) Hash() hash.Hash {
	switch v.typ {
	case NullType:
		if v.v == uint64(PosNull) {
			return hash.Hash{
				0x2c, 0x32, 0x8a, 0x3b, 0x56, 0x93, 0x1f, 0x07,
				0x0c, 0x7f, 0xca, 0x06, 0x05, 0x40, 0xbb, 0x94,
				0x44, 0xac, 0x23, 0x71, 0x75, 0x56, 0x34, 0xa9,
				0x45, 0x1e, 0x34, 0x76, 0x09, 0x75, 0x9e, 0x49}
		}
		return hash.Hash{
			0x28, 0xff, 0xfb, 0x6c, 0x5c, 0xcd, 0x0b, 0x3f,
			0x29, 0xf9, 0x4f, 0x08, 0xe8, 0xeb, 0x6a, 0xd6,
			0x95, 0x47, 0x8c, 0xa7, 0xb3, 0x90, 0xae, 0x6f,
			0x2f, 0x0d, 0x8b, 0x00, 0x8e, 0x6d, 0x39, 0xa3}
	case BoolType:
		return hash.Bool(v.Bool(nil))
	case IntType:
		return hash.Int(v.Int(nil))
	case FloatType:
		return hash.Float(v.Float(nil))
	case StringType, FileNameType, EnumType:
		return hash.String(v.Str(nil))
	case DateType, DateTimeType:
		return hash.Time(v.DateTime(nil))
	case CharType:
		return hash.Int(int64(v.Char(nil)))
	case FuncType:
		return v.Func(nil).hash
	case StructType:
		return hashStruct(v.Struct(nil))
	case TableType:
		return v.Table(nil).Hash()
	case StructFragmentType:
		h := hash.Hash{
			0xf6, 0xe4, 0x86, 0xe5, 0x83, 0x77, 0x20, 0x40,
			0x65, 0x14, 0x25, 0xbf, 0xe7, 0x79, 0x76, 0xd1,
			0xa3, 0x3f, 0x7f, 0x6c, 0x6d, 0x3b, 0x6f, 0xc7,
			0x29, 0xd5, 0xdd, 0x38, 0x9b, 0x66, 0x20, 0xf6}
		for _, v := range v.StructFragment() {
			h = h.Merge(v.Name.Hash())
			h = h.Merge(v.Value.Hash())
		}
		return h
	}
	log.Panicf("Hash: invalid type %v", v.typ)
	return hash.Hash{}
}

// Marshal encodes the value into the given buffer.
func (v Value) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	enc.PutByte(byte(v.typ))
	switch v.typ {
	case NullType:
		if v.Null() == PosNull {
			enc.PutByte(1)
		} else {
			enc.PutByte(byte(0xff))
		}
	case BoolType:
		if v.Bool(nil) {
			enc.PutByte(1)
		} else {
			enc.PutByte(0)
		}
	case IntType, CharType, DurationType:
		enc.PutVarint(int64(v.v))
	case FloatType:
		enc.PutUint64(v.v) // v.v encodes the floating point in binary.
	case DateType, DateTimeType:
		t := v.DateTime(nil)
		enc.PutUint64(uint64(t.UnixNano()))
		tzName, tzOff := t.Zone()
		enc.PutVarint(int64(tzOff))
		enc.PutString(tzName)
	case StringType, FileNameType, EnumType:
		s := v.Str(nil)
		enc.PutString(s)
	case StructType:
		marshalStruct(v.Struct(nil), ctx, enc)
	case TableType:
		v.Table(nil).Marshal(ctx, enc)
	case FuncType:
		v.Func(nil).Marshal(ctx, enc)
	default:
		log.Panicf("MarshalGOB: invalid type %v", v.typ)
	}
}

// MarshalBinary implements the GOB interface.  This function should be called
// only for static literals embedded in an ASTNode.
//
// Call Marshal() for dynamically constructed Values, especially Table objects,
// that may contain *Funcs.
func (v Value) MarshalBinary() ([]byte, error) {
	ctx := newMarshalContext(BackgroundContext)
	enc := marshal.NewEncoder(nil)
	v.Marshal(ctx, enc)
	if len(ctx.frames) > 0 {
		// This shouldn't happen, since Literal is always a terminal value.
		log.Panicf("Value.MarshalBinary: trying to encode a closure in %v", v)
	}
	return marshal.ReleaseEncoder(enc), nil
}

// UnmarshalBinary implements the GOB interface. This function should be called
// only for static literals embedded in an ASTNode.
//
// Call Marshal() for dynamically constructed Values, especially Table objects,
// that may contain *Funcs.
func (v *Value) UnmarshalBinary(data []byte) error {
	dec := marshal.NewDecoder(data)
	var ctx UnmarshalContext
	v.Unmarshal(ctx, dec)
	marshal.ReleaseDecoder(dec)
	return nil
}

func internTimeLocation(tzName string, tzOff int) *time.Location {
	// TODO(saito) dedup
	return time.FixedZone(tzName, tzOff)
}

// Unmarshal decodes the buffer encoded using Marshal.
func (v *Value) Unmarshal(ctx UnmarshalContext, dec *marshal.Decoder) {
	typ := ValueType(dec.Byte())
	v.typ = typ
	switch typ {
	case NullType:
		switch dec.Byte() {
		case 1:
			v.v = uint64(PosNull)
		case byte(0xff):
			tmp := NegNull
			v.v = uint64(tmp)
		default:
			log.Panicf("NA: %v", dec)
		}
	case BoolType:
		switch dec.Byte() {
		case 1:
			v.v = 1
		case 0:
			v.v = 0
		default:
			log.Panicf("Bool: %v", dec)
		}
	case IntType, CharType, DurationType:
		v.v = uint64(dec.Varint())
	case FloatType:
		v.v = dec.Uint64()
	case DateType, DateTimeType:
		v.v = dec.Uint64()
		tzOff := dec.Varint()
		tzName := dec.String()
		v.p = unsafe.Pointer(internTimeLocation(tzName, int(tzOff)))
	case StringType, FileNameType, EnumType:
		s := dec.String()
		sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
		v.p = unsafe.Pointer(sh.Data)
		v.v = uint64(sh.Len)
	case StructType:
		*v = NewStruct(unmarshalStruct(ctx, dec))
	case TableType:
		*v = NewTable(unmarshalTable(ctx, dec))
	case FuncType:
		*v = NewFunc(unmarshalFunc(ctx, dec))
	default:
		// log.Panicf("UnmarshalBinary: invalid type %v", v.typ)
	}
}

// PrintMode specifies how a value is printed in the Value.Print method.
type PrintMode int

const (
	// PrintValues prints the values. Tables are pretty printed.
	PrintValues PrintMode = iota
	// PrintCompact is similar to PrintValues, but it prints tables in more
	// compact form.  Suitable for use in unittests.
	PrintCompact
	// PrintDescription prints the description of the value, not the actual value.  THe
	// descrtiption includes such as the value type and columns (for a table).
	PrintDescription
)

// PrintArgs define parameters to Value.Print method.
type PrintArgs struct {
	// Out is the output destination
	Out termutil.Printer

	// Mode defines how the value is printed
	Mode PrintMode
	// TmpVars collects names of subtables in case they are not printed in-line.
	// The user can later print the subtable contents by typing these names.  If
	// TmpVars is nil, a nested table will be printed as "[omitted]".
	TmpVars *TmpVars

	// MaxInlinedTableLen is the threshold for printing a nested table inline.
	// When a table's compact representation exceeds this length (bytes), it is
	// printed as "tmpNN" or "[omitted]" depending on whether TmpVars!=nil.
	//
	// If MaxInlinedTableLen <= 0, it is set to 78.
	MaxInlinedTableLen int
}

// defaultMaxInlineTablePrintLen is the default value for PrintArgs.MaxInlinedTableLen.
const defaultMaxInlineTablePrintLen = 78

// TmpVars is used by Value.Print to give names to nested tables when values are
// printed on screen.  TmpVars is a singleton object. Thread compatible.
type TmpVars struct {
	seq    int
	hashes map[hash.Hash]string
	vars   map[string]Value
}

// Register assigns a name of form "tmpNNN" to the given value. If New is invoked for
// the same value multiple times, it returns the same name.
func (a *TmpVars) Register(val Value) string {
	if a.hashes == nil {
		a.hashes = map[hash.Hash]string{}
		a.vars = map[string]Value{}
	}
	h := val.Hash()
	if varName, ok := a.hashes[h]; ok {
		return varName
	}
	seq := a.seq
	a.seq++
	varName := fmt.Sprintf("tmp%d", seq)
	a.hashes[h] = varName
	a.vars[varName] = val
	return varName
}

// Flush adds the registered tmp variables as the global variables in the given
// session. The caller must ensure that "s" is not concurrently used by other
// threads.
func (a *TmpVars) Flush(s *Session) {
	s.SetGlobals(a.vars)
	for k := range a.vars {
		delete(a.vars, k)
	}
}

// PrintValueList prints a list of values in form "[val0, val1, ...]".  Arg depth
// is used as PrintArgs.Depth.
func PrintValueList(vals []Value) string {
	buf := termutil.NewBufferPrinter()
	buf.WriteString("[")
	for i, v := range vals {
		if i > 0 {
			buf.WriteString(",")
		}
		v.Print(BackgroundContext, PrintArgs{Out: buf, Mode: PrintCompact})
	}
	buf.WriteString("]")
	return buf.String()
}

// DescribeValue a shorthand for calling v.Print() with PrintArgs.Mode of PrintDescription.
func DescribeValue(v Value) string {
	out := termutil.NewBufferPrinter()
	v.Print(BackgroundContext, PrintArgs{Out: out, Mode: PrintDescription})
	return out.String()
}

// String produces a human-readable string of the value. It is a shorthand for
// v.Print() with mode PrintCompact
func (v Value) String() string {
	out := termutil.NewBufferPrinter()
	v.Print(BackgroundContext, PrintArgs{Out: out, Mode: PrintCompact})
	return out.String()
}

// Print prints the value according to args.
func (v Value) Print(ctx context.Context, args PrintArgs) { v.printRec(ctx, args, 0) }

// PrintRec is used internally by Print. Arg depth is the recursion depth.
func (v Value) printRec(ctx context.Context, args PrintArgs, depth int) {
	switch v.typ {
	case NullType:
		if args.Mode == PrintDescription {
			args.Out.WriteString("null")
		} else if v.Null() == PosNull {
			args.Out.WriteString("NA")
		} else {
			args.Out.WriteString("-NA")
		}
	case BoolType:
		if args.Mode == PrintDescription {
			args.Out.WriteString("bool")
		} else if v.Bool(nil) {
			args.Out.WriteString("true")
		} else {
			args.Out.WriteString("false")
		}
	case IntType:
		if args.Mode == PrintDescription {
			args.Out.WriteString("int")
			return
		}
		args.Out.WriteInt(v.Int(nil))
	case FloatType:
		if args.Mode == PrintDescription {
			args.Out.WriteString("float")
			return
		}
		args.Out.WriteFloat(v.Float(nil))
	case StringType, FileNameType, EnumType:
		switch args.Mode {
		case PrintDescription:
			switch v.typ {
			case StringType:
				args.Out.WriteString("string")
			case FileNameType:
				args.Out.WriteString("filename")
			case EnumType:
				args.Out.WriteString("enum")
			}
			return
		default:
			args.Out.WriteString(v.Str(nil))
		}
	case DateType:
		if args.Mode == PrintDescription {
			args.Out.WriteString("date")
			return
		}
		args.Out.WriteString(v.DateTime(nil).Format(iso8601DateFormat))
	case DateTimeType:
		if args.Mode == PrintDescription {
			args.Out.WriteString("datetime")
			return
		}
		args.Out.WriteString(v.DateTime(nil).Format(iso8601DateTimeFormat))
	case DurationType:
		if args.Mode == PrintDescription {
			args.Out.WriteString("duration")
			return
		}
		args.Out.WriteString(v.Duration(nil).String())
	case CharType:
		if args.Mode == PrintDescription {
			args.Out.WriteString("char")
			return
		}
		// TODO(saito) avoid string allocation.
		args.Out.WriteString(fmt.Sprintf("%c", v.Char(nil)))
	case StructType:
		st := v.Struct(nil)
		shortArgs := PrintArgs{
			Out:                args.Out,
			Mode:               PrintCompact,
			TmpVars:            args.TmpVars,
			MaxInlinedTableLen: args.MaxInlinedTableLen,
		}
		switch args.Mode {
		case PrintCompact:
			args.Out.WriteString("{")
			nFields := st.Len()
			for fi := 0; fi < nFields; fi++ {
				val := st.Field(fi)
				args.Out.WriteString(val.Name.Str())
				args.Out.WriteString(":")
				val.Value.printRec(ctx, shortArgs, depth+1)
				if fi < nFields-1 {
					args.Out.WriteString(",")
				}
			}
			args.Out.WriteString("}")
		case PrintValues, PrintDescription:
			args.Out.WriteString("{\n")
			nFields := st.Len()
			for fi := 0; fi < nFields; fi++ {
				val := st.Field(fi)
				args.Out.WriteString("\t")
				args.Out.WriteString(val.Name.Str())
				args.Out.WriteString(": ")
				if args.Mode == PrintDescription {
					args.Out.WriteString(DescribeValue(val.Value))
				} else {
					val.Value.printRec(ctx, shortArgs, depth+1)
				}
				if fi < nFields-1 {
					args.Out.WriteString(",")
				}
				args.Out.WriteString("\n")
			}
			args.Out.WriteString("}")
		default:
			log.Panicf("Print: invalid mode %v", args.Mode)
		}
	case TableType:
		table := v.Table(nil)
		switch args.Mode {
		case PrintValues, PrintCompact:
			printTable(ctx, args, table, depth)
			return
		case PrintDescription:
			attrs := table.Attrs(ctx)
			args.Out.WriteString("## " + attrs.Name + "\n\n")
			args.Out.WriteString("**Path**: " + attrs.Path + "\n\n")
			args.Out.WriteString(attrs.Description + "\n\n")
			for _, col := range attrs.Columns {
				args.Out.WriteString("**" + col.Name + "**: (" + col.Type.String() + ")\n\n")
				if col.Description != "" {
					args.Out.WriteString("> " + col.Description + "\n\n")
				}
			}
			return
		default:
			log.Panicf("Print: invalid mode %v", args.Mode)
		}
	case FuncType:
		f := v.Func(nil)
		switch args.Mode {
		case PrintDescription:
			if f.description != "" {
				args.Out.WriteString(expandDocString(f.description))
			} else {
				args.Out.WriteString(fmt.Sprintf("func %s: TODO: no description", f.name.Str()))
			}
			return
		default:
			if f.builtin {
				args.Out.WriteString(f.name.Str())
				break
			}
			args.Out.WriteString(fmt.Sprintf("udf:%v(env: %s)", f.body, f.env.Describe()))
		}
	default:
		log.Panicf("Print: invalid type %v", v.typ)
	}
}

// Hash32 implements the bigslice.Hasher interface.
func (v Value) Hash32() uint32 {
	h := v.Hash()
	return binary.LittleEndian.Uint32(h[:])
}

func hashValues(values []Value) hash.Hash {
	h := hash.Hash{
		0xb6, 0x8c, 0x86, 0x3a, 0x4d, 0x3a, 0xf0, 0x57,
		0x18, 0xe9, 0x06, 0xa3, 0xf1, 0x12, 0xbd, 0xa5,
		0xed, 0x67, 0xeb, 0xa5, 0x3d, 0xbf, 0x61, 0x87,
		0x99, 0x46, 0x45, 0x30, 0x5c, 0xd9, 0xb7, 0x09}
	for _, v := range values {
		h = h.Merge(v.Hash())
	}
	return h
}

// Compare two scalar values (int, float, string, char, bool, null). The caller
// must ensure that the two values are of the same type, and they are scalar.
// "ast" is for displaying error messages.
func compareScalar(ast ASTNode, v0, v1 Value) int {
	null0, null1 := v0.Null(), v1.Null()

	if null0 != NotNull || null1 != NotNull {
		if null0 == null1 {
			return 0
		}
		if null0 < null1 {
			return -1
		}
		return 1
	}
	switch v0.Type() {
	case IntType:
		vv0 := v0.Int(ast)
		vv1 := v1.Int(ast)
		switch {
		case vv0 < vv1:
			return -1
		case vv0 == vv1:
			return 0
		default:
			return 1
		}
	case DurationType:
		vv0 := v0.Duration(ast)
		vv1 := v1.Duration(ast)
		switch {
		case vv0 < vv1:
			return -1
		case vv0 == vv1:
			return 0
		default:
			return 1
		}
	case FloatType:
		vv0 := v0.Float(ast)
		vv1 := v1.Float(ast)
		switch {
		case vv0 < vv1:
			return -1
		case vv0 == vv1:
			return 0
		default:
			return 1
		}
	case StringType, EnumType, FileNameType:
		vv0 := v0.Str(ast)
		vv1 := v1.Str(ast)
		switch {
		case vv0 < vv1:
			return -1
		case vv0 == vv1:
			return 0
		default:
			return 1
		}
	case CharType:
		vv0 := v0.Char(ast)
		vv1 := v1.Char(ast)
		switch {
		case vv0 < vv1:
			return -1
		case vv0 == vv1:
			return 0
		default:
			return 1
		}
	case BoolType:
		vv0 := v0.Bool(ast)
		vv1 := v1.Bool(ast)
		if vv0 == vv1 {
			return 0
		} else if vv1 {
			return -1
		} else {
			return 1
		}
	case DateTimeType, DateType:
		vv0 := v0.DateTime(ast)
		vv1 := v1.DateTime(ast)
		switch {
		case vv0.Before(vv1):
			return -1
		case vv1.Before(vv0):
			return 1
		default:
			return 0
		}
	default:
		log.Panicf("compareValues: non-scalar args: %v (type %v) and %v (type %v)",
			v0, v0.Type(), v1, v1.Type())
		return -1
	}
}

// Compare compares the two values lexicographically. It returns -1,0,1 if
// v0<v1, v0==v1, v0>v1, respectively. It crashes if the two values are not of
// the same type. "ast" is for displaying error messages.
func Compare(ast ASTNode, v0, v1 Value) int {
	if v0.Type() != StructType {
		return compareScalar(ast, v0, v1)
	}
	s0, s1 := v0.Struct(ast), v1.Struct(ast)
	s0Len, s1Len := s0.Len(), s1.Len()
	if s0Len != s1Len {
		log.Panicf("struct signature mismatch: %v %v", v0, v1)
	}
	for ci := 0; ci < s0Len; ci++ {
		cmp := Compare(ast, s0.Field(ci).Value, s1.Field(ci).Value)
		if cmp < 0 {
			return -1
		}
		if cmp > 0 {
			return 1
		}
	}
	return 0
}

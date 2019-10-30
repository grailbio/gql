package gql

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/symbol"
	"github.com/grailbio/gql/termutil"
)

func builtinInvalidArgTypesError(ast ASTNode, args []AIArg) AIType {
	buf := strings.Builder{}
	fmt.Fprintf(&buf, "%v:%v: invalid arg types:", ast.pos(), ast)
	for i, arg := range args {
		if i > 0 {
			buf.WriteString(", ")
		}
		switch {
		case arg.Expr != nil:
			fmt.Fprintf(&buf, "#%d: %s (type %v)", i, arg.Expr.String(), arg.Type.Type)
		case arg.Symbol != symbol.Invalid:
			fmt.Fprintf(&buf, "#%d: %s (symbol)", i, arg.Symbol.Str())
		default:
			fmt.Fprintf(&buf, "#%d: NA", i)
		}
	}
	log.Panic(buf.String())
	return AIType{}
}

func builtinInvalidArgsError(ast ASTNode, args ...Value) Value {
	buf := strings.Builder{}
	fmt.Fprintf(&buf, "%v:%v: invalid args: ", ast.pos(), ast)
	for i, arg := range args {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "'%v' (type %v)", arg, DescribeValue(arg))
	}
	log.Panic(buf.String())
	return Value{}
}

// !X
func builtinNot(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	return NewBool(!args[0].Bool())
}

func builtinInternalEqual(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	switch args[0].Value.Type() {
	case NullType:
		return NewBool(args[0].Value.Null() == args[1].Value.Null())
	case IntType:
		return NewBool(args[0].Int() == args[1].Int())
	case StringType, EnumType, FileNameType:
		return NewBool(args[0].Str() == args[1].Str())
	case FloatType:
		return NewBool(args[0].Float() == args[1].Float())
	case CharType:
		return NewBool(args[0].Char() == args[1].Char())
	case DateTimeType, DateType:
		return NewBool(args[0].DateTime() == args[1].DateTime())
	case DurationType:
		return NewBool(args[0].Duration() == args[1].Duration())
	case BoolType:
		return NewBool(args[0].Bool() == args[1].Bool())
	}
	return builtinInvalidArgsError(ast, args[0].Value, args[1].Value)
}

// X == Y
func builtinEQ(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x, y := args[0].Value, args[1].Value
	if xnull, ynull := x.Null(), y.Null(); xnull != ynull {
		return NewBool(xnull == ynull)
	}
	return builtinInternalEqual(ctx, ast, args)
}

// X ==? Y
func builtinEQOrRhsNull(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x, y := args[0].Value, args[1].Value
	if x.Null() != NotNull {
		return NewBool(false)
	}
	if y.Null() != NotNull {
		return NewBool(true)
	}
	return builtinInternalEqual(ctx, ast, args)
}

// X ?== Y
func builtinEQOrLhsNull(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x, y := args[0].Value, args[1].Value
	if y.Null() != NotNull {
		return False
	}
	if x.Null() != NotNull {
		return True
	}
	return builtinInternalEqual(ctx, ast, args)
}

// X ?==? Y
func builtinEQOrBothNull(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x, y := args[0].Value, args[1].Value
	if x.Null() != NotNull || y.Null() != NotNull {
		return True
	}
	return builtinInternalEqual(ctx, ast, args)
}

// builtinMax computes the maximum of the args.
func builtinMax(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	v := args[0].Value
	for _, arg := range args[1:] {
		if compareScalar(ast, v, arg.Value) < 0 {
			v = arg.Value
		}
	}
	return v
}

// builtinMax computes the minimum of the args.
func builtinMin(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	v := args[0].Value
	for _, arg := range args[1:] {
		if compareScalar(ast, v, arg.Value) > 0 {
			v = arg.Value
		}
	}
	return v
}

// builtinInt converts an arg to an integer.
func builtinInt(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x := args[0].Value
	switch x.Type() {
	case NullType:
		return NewInt(0)
	case IntType:
		return x
	case FloatType:
		return NewInt(int64(args[0].Float()))
	case BoolType:
		v := int64(0)
		if args[0].Bool() {
			v = 1
		}
		return NewInt(v)
	case StringType, FileNameType, EnumType:
		var v int64
		n, err := fmt.Sscanf(args[0].Str(), "%v", &v)
		if n != 1 || err != nil {
			Panicf(ast, "failed to parse '%v' as int: %v", x, err)
		}
		return NewInt(v)
	case CharType:
		return NewInt(int64(args[0].Char()))
	case DateTimeType, DateType:
		return NewInt(args[0].DateTime().Unix())
	case DurationType:
		return NewInt(int64(args[0].Duration()))
	}
	return builtinInvalidArgsError(ast, x)
}

// builtinFloat converts an arg to a float64.
func builtinFloat(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x := args[0].Value
	switch x.Type() {
	case NullType:
		return NewFloat(0.0)
	case IntType:
		return NewFloat(float64(args[0].Int()))
	case FloatType:
		return x
	case BoolType:
		v := float64(0)
		if args[0].Bool() {
			v = 1.0
		}
		return NewFloat(v)
	case StringType, EnumType, FileNameType:
		var v float64
		n, err := fmt.Sscanf(args[0].Str(), "%v", &v)
		if n != 1 || err != nil {
			Panicf(ast, "failed to parse '%v' as float: %v", x, err)
		}
		return NewFloat(v)
	case DateTimeType, DateType:
		return NewFloat(float64(args[0].DateTime().UnixNano()) / float64(1000000000))
	case DurationType:
		return NewFloat(float64(args[0].Duration()) / float64(1000000000))
	}
	return builtinInvalidArgsError(ast, x)
}

// builtinString converts an arg to a string.
func builtinString(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x := args[0].Value
	switch x.Type() {
	case NullType:
		return NewString("")
	case IntType:
		return NewString(fmt.Sprintf("%v", args[0].Int()))
	case FloatType:
		return NewString(fmt.Sprintf("%v", args[0].Float()))
	case BoolType:
		return NewString(fmt.Sprintf("%v", args[0].Bool()))
	case CharType:
		return NewString(fmt.Sprintf("%c", args[0].Char()))
	case StringType, EnumType, FileNameType:
		return x
	case DateType, DateTimeType, DurationType:
		return NewString(x.String())
	}
	return builtinInvalidArgsError(ast, x)
}

func builtinPlus(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	switch args[0].Value.Type() {
	case IntType:
		return NewInt(args[0].Int() + args[1].Int())
	case FloatType:
		return NewFloat(args[0].Float() + args[1].Float())
	case StringType, EnumType, FileNameType:
		return NewString(args[0].Str() + args[1].Str())
	case DurationType:
		return NewDuration(args[0].Duration() + args[1].Duration())
	default:
		return builtinInvalidArgsError(ast, args[0].Value, args[1].Value)
	}
}

func builtinMinus(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x, y := args[0].Value, args[1].Value
	switch args[0].Value.Type() {
	case IntType:
		return NewInt(args[0].Int() - args[1].Int())
	case FloatType:
		return NewFloat(args[0].Float() - args[1].Float())
	case DurationType:
		return NewDuration(args[0].Duration() - args[1].Duration())
	default:
		return builtinInvalidArgsError(ast, x, y)
	}
}

func builtinMultiply(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x, y := args[0].Value, args[1].Value
	switch args[0].Value.Type() {
	case IntType:
		return NewInt(args[0].Int() * args[1].Int())
	case FloatType:
		return NewFloat(args[0].Float() * args[1].Float())
	default:
		return builtinInvalidArgsError(ast, x, y)
	}
	// TODO(saito) support math on durations.
}

func builtinDivide(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x, y := args[0].Value, args[1].Value
	switch x.Type() {
	case IntType:
		return NewInt(args[0].Int() / args[1].Int())
	case FloatType:
		return NewFloat(args[0].Float() / args[1].Float())
	default:
		return builtinInvalidArgsError(ast, x, y)
	}
	// TODO(saito) support math on durations.
}

func builtinMod(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x, y := args[0].Int(), args[1].Int()
	return NewInt(x % y)
}

func negateString(src string) string {
	b := strings.Builder{}
	for i := 0; i < len(src); i++ {
		if src[i] == 0 {
			b.WriteByte(0xff)
			b.WriteByte(0xfe)
		} else {
			b.WriteByte(0xff - src[i])
		}
	}
	b.WriteByte(0xff)
	b.WriteByte(0xff)
	return b.String()
}

// builtinNegate implements -val operator.
func builtinNegate(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	x := args[0].Value
	switch x.Type() {
	case NullType:
		return NewNull(-x.Null())
	case IntType:
		return NewInt(-args[0].Int())
	case FloatType:
		return NewFloat(-args[0].Float())
	case StringType:
		return NewString(negateString(args[0].Str()))
	case EnumType:
		return NewEnum(negateString(args[0].Str()))
	case FileNameType:
		return NewFileName(negateString(args[0].Str()))
	case DurationType:
		return NewDuration(-args[0].Duration())
	default:
		return builtinInvalidArgsError(ast, x)
	}
}

func builtinRegexpReplace(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	// TODO(saito) use private cache.
	src, reStr, repl := args[0].Str(), args[1].Str(), args[2].Str()
	re := regexp.MustCompile(reStr)
	s := re.ReplaceAllString(src, repl)
	return NewString(s)
}

func builtinStringReplace(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	src, old, new := args[0].Str(), args[1].Str(), args[2].Str()
	s := strings.Replace(src, old, new, -1)
	return NewString(s)
}

func builtinSprintf(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	fmtStr := args[0].Str()
	l := make([]interface{}, len(args)-1)
	for i, arg := range args[1:] {
		val := arg.Value
		switch arg.Value.Type() {
		case IntType, DurationType:
			// TODO(saito) Perhaps we should print duration as a human-readable
			// string.
			l[i] = arg.Int()
		case BoolType:
			l[i] = arg.Bool()
		case FloatType:
			l[i] = arg.Float()
		case StringType, FileNameType, EnumType:
			l[i] = arg.Str()
		case DateType, DateTimeType, NullType:
			l[i] = val.String()
		case CharType:
			l[i] = arg.Char()
		default:
			Panicf(ast, "'%v' (type %s) is not scalar", val, DescribeValue(val))
		}
	}
	return NewString(fmt.Sprintf(fmtStr, l...))
}

var (
	// They are used by yacc to parse infix / prefix ops.
	builtinNotValue          Value
	builtinEQValue           Value
	builtinEQOrRhsNullValue  Value
	builtinEQOrLhsNullValue  Value
	builtinEQOrBothNullValue Value
	builtinNEValue           Value
	builtinGEValue           Value
	builtinGTValue           Value
	builtinPlusValue         Value
	builtinMinusValue        Value
	builtinMultiplyValue     Value
	builtinDivideValue       Value
	builtinModValue          Value
	builtinNegateValue       Value

	eqeqSymbolID   = symbol.Intern("infix:==")
	eqeqqSymbolID  = symbol.Intern("infix:==?")
	qeqeqqSymbolID = symbol.Intern("infix:?==")
	qeqeqSymbolID  = symbol.Intern("infix:?==?")
)

// isEqualEqual checks if the given node is a literal referring to "==", "?==",
// "==?", or "?==?".  If so, it returns the interned symbol.ID for these
// literals. Else it return symbol.Invalid.
func isEqualEqual(n ASTNode) symbol.ID {
	lit, ok := n.(*ASTLiteral)
	if !ok {
		return symbol.Invalid
	}
	if lit.Literal.Type() != FuncType {
		return symbol.Invalid
	}
	name := lit.Literal.Func(n).name
	if name == eqeqSymbolID || name == eqeqqSymbolID || name == qeqeqqSymbolID || name == qeqeqSymbolID {
		return name
	}
	return symbol.Invalid
}

func init() {
	boolFuncType := func(ast ASTNode, _ []AIArg) AIType { return AIBoolType }
	stringFuncType := func(ast ASTNode, _ []AIArg) AIType { return AIStringType }
	floatFuncType := func(ast ASTNode, _ []AIArg) AIType { return AIFloatType }
	intFuncType := func(ast ASTNode, _ []AIArg) AIType { return AIIntType }
	minMaxFuncType := func(ast ASTNode, args []AIArg) AIType {
		return combineArgTypes(ast, args)
	}

	// List of types that can be passed to comparison operators, such as "<", ">",
	// "max".
	scalarTypes := []ValueType{NullType, IntType,
		FloatType, StringType, EnumType, FileNameType, CharType, BoolType,
		DateTimeType, DateType, DurationType}

	positionalArg := FormalArg{Positional: true, Required: true}

	builtinNotValue = RegisterBuiltinFunc("prefix:!", "TODO", builtinNot,
		func(ast ASTNode, args []AIArg) AIType {
			if args[0].Type.Is(BoolType) {
				// TODO(saito) constant propagation
				return AIBoolType
			}
			return builtinInvalidArgTypesError(ast, args)
		}, positionalArg)
	builtinEQValue = RegisterBuiltinFunc("infix:==", "TODO", builtinEQ, boolFuncType,
		positionalArg, positionalArg)
	builtinEQOrRhsNullValue = RegisterBuiltinFunc("infix:==?", "TODO", builtinEQOrRhsNull, boolFuncType,
		positionalArg, positionalArg)
	builtinEQOrLhsNullValue = RegisterBuiltinFunc("infix:?==", "TODO", builtinEQOrLhsNull, boolFuncType,
		positionalArg, positionalArg)
	builtinEQOrBothNullValue = RegisterBuiltinFunc("infix:?==?", "TODO", builtinEQOrBothNull, boolFuncType,
		positionalArg, positionalArg)
	builtinNEValue = RegisterBuiltinFunc("infix:!=", "TODO",
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			return NewBool(compareScalar(ast, args[0].Value, args[1].Value) != 0)
		},
		boolFuncType, positionalArg, positionalArg)
	builtinGEValue = RegisterBuiltinFunc("infix:>=", "TODO",
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			return NewBool(compareScalar(ast, args[0].Value, args[1].Value) >= 0)
		},
		boolFuncType, positionalArg, positionalArg)
	builtinGTValue = RegisterBuiltinFunc("infix:>", "TODO",
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			return NewBool(compareScalar(ast, args[0].Value, args[1].Value) > 0)
		},
		boolFuncType, positionalArg, positionalArg)
	RegisterBuiltinFunc("max", "TODO",
		builtinMax, minMaxFuncType,
		FormalArg{Positional: true, Required: true, Variadic: true, Types: scalarTypes})
	RegisterBuiltinFunc("min", "TODO",
		builtinMin,
		func(ast ASTNode, args []AIArg) AIType { return minMaxFuncType(ast, args) },
		FormalArg{Positional: true, Required: true, Variadic: true, Types: scalarTypes})
	builtinPlusValue = RegisterBuiltinFunc("infix:+", "TODO", builtinPlus,
		func(ast ASTNode, args []AIArg) AIType { return combineArgTypes(ast, args) },
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType, FloatType, StringType, DurationType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType, FloatType, StringType, DurationType}})
	builtinMinusValue = RegisterBuiltinFunc("infix:-", "TODO", builtinMinus,
		func(ast ASTNode, args []AIArg) AIType { return combineArgTypes(ast, args) },
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType, FloatType, StringType, DurationType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType, FloatType, StringType, DurationType}})
	builtinMultiplyValue = RegisterBuiltinFunc("infix:*", "TODO",
		builtinMultiply,
		func(ast ASTNode, args []AIArg) AIType { return combineArgTypes(ast, args) },
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType, FloatType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType, FloatType}})
	builtinDivideValue = RegisterBuiltinFunc("infix:/", "TODO", builtinDivide,
		func(ast ASTNode, args []AIArg) AIType { return combineArgTypes(ast, args) },
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType, FloatType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType, FloatType}})
	builtinModValue = RegisterBuiltinFunc("infix:%", "TODO", builtinMod,
		func(ast ASTNode, args []AIArg) AIType { return combineArgTypes(ast, args) },
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}})
	RegisterBuiltinFunc("isnull",
		`
    isnull(expr)

isnull returns true if expr is NA (or -NA). Else it returns false.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			return NewBool(args[0].Value.Type() == NullType)
		}, boolFuncType, positionalArg)
	RegisterBuiltinFunc("contains",
		`
    contains(struct, field)

Arg types:

- _struct_: struct
- _field_: string

Contains returns true if the struct contains the specified field, else it returns false.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			str := args[0].Struct()
			found := false
			name := args[1].Str()
			for i := 0; i < str.Len(); i++ {
				field := str.Field(i)
				if name == field.Name.Str() {
					found = true
					break
				}
			}
			return NewBool(found)
		}, boolFuncType,
		FormalArg{Positional: true, Required: true, Types: []ValueType{StructType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
	)
	RegisterBuiltinFunc("isstruct",
		`
    isstruct(expr)

Isstruct returns true if expr is a struct.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			return NewBool(args[0].Value.Type() == StructType)
		}, boolFuncType, positionalArg)
	RegisterBuiltinFunc("istable",
		`
    istable(expr)

Istable returns true if expr is a table.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			return NewBool(args[0].Value.Type() == TableType)
		}, boolFuncType, positionalArg)
	RegisterBuiltinFunc("int",
		`
    int(expr)

Int converts any scalar expression into an integer.
Examples:
    int("123") == 123
    int(1234.0) == 1234
    int(NA) == 0

NA is translated into 0.
If expr is a date, int(expr) computes the number of seconds since the epoch (1970-01-01).
If expr is a duration, int(expr) returns the number of nanoseconds.
`,
		builtinInt, intFuncType, positionalArg)
	RegisterBuiltinFunc("float",
		`
    float(expr)

The float function converts any scalar expression into an float.
Examples:
    float("123") == 123.0
    float(1234) == 1234.0
    float(NA) == 0.0

NA is translated into 0.0.
If expr is a date, float(expr) computes the number of seconds since the epoch (1970-01-01).
If expr is a duration, float(expr) returns the number of seconds.
`, builtinFloat, floatFuncType, positionalArg)
	RegisterBuiltinFunc("string",
		`
    string(expr)

Examples:
    string(123.0) == "123.0"
    string(NA) == ""
    string(1+10) == "11"

The string function converts any scalar expression into a string.
NA is translated into an empty string.
`,
		builtinString, stringFuncType, positionalArg)

	// unary '-'
	builtinNegateValue = RegisterBuiltinFunc("prefix:-", "TODO", builtinNegate,
		func(ast ASTNode, args []AIArg) AIType {
			a0Type := args[0].Type
			return AIType{Type: a0Type.Type, Any: a0Type.Any}
		},
		FormalArg{Positional: true, Required: true,
			Types: []ValueType{NullType, IntType, FloatType, StringType, DurationType}})

	_ = RegisterBuiltinFunc("print",
		`
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
`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			nArg := len(args)
			mode := args[nArg-1].Str()
			printArgs := PrintArgs{
				Out: termutil.NewBatchPrinter(os.Stdout),
			}
			switch mode {
			case "compact":
				printArgs.Mode = PrintCompact
			case "description":
				printArgs.Mode = PrintDescription
			case "default":
				printArgs.Mode = PrintValues
			default:
				Panicf(ast, "illegal mode `%s`", mode)
			}
			for _, arg := range args {
				arg.Value.Print(ctx, printArgs)
				printArgs.Out.WriteString("\n")
			}
			return True
		}, boolFuncType,
		FormalArg{Positional: true, Required: true, Variadic: true},
		FormalArg{Name: symbol.Depth, DefaultValue: NewInt(math.MaxInt32), Types: []ValueType{IntType}},
		FormalArg{Name: symbol.Mode, DefaultValue: NewString("default"), Types: []ValueType{StringType}})
	RegisterBuiltinFunc("regexp_replace",
		`
    regexp_replace(str, re, replacement)

Arg types:

- _str_: string
- _re_: string
_ _replacement_: string

Example:
    regexp_replace("dog", "(o\\S+)" "x$1y") == "dxogy"

Replace occurrence of re in str with replacement. It is implemented using Go's
regexp.ReplaceAllString (https://golang.org/pkg/regexp/#Regexp.ReplaceAllString).`,
		builtinRegexpReplace,
		func(ast ASTNode, args []AIArg) AIType {
			return AIStringType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}})

	RegisterBuiltinFunc("regexp_match",
		`Usage: regexp_match(str, re)

Example:
    regexp_match("dog", "o+") == true
    regexp_match("dog", "^o") == false

Check if str matches re.
Uses go's regexp.MatchString (https://golang.org/pkg/regexp/#Regexp.MatchString).`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			src, reStr := args[0].Str(), args[1].Str()
			re := regexp.MustCompile(reStr)
			if re.MatchString(src) {
				return True
			}
			return False
		},
		func(ast ASTNode, args []AIArg) AIType { return AIBoolType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}})

	RegisterBuiltinFunc("string_len",
		`
    string_len(str)

Arg types:

- _str_: string


Example:
    string_len("dog") == 3

Compute the length of the string. Returns an integer.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			src := args[0].Str()
			return NewInt(int64(len(src)))
		},
		func(ast ASTNode, args []AIArg) AIType {
			return AIIntType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}})

	RegisterBuiltinFunc("substring",
		`
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
`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			src, from, to := args[0].Str(), args[1].Int(), args[2].Int()
			if to >= int64(len(src)) {
				to = int64(len(src))
			}
			return NewString(src[from:to])
		},
		func(ast ASTNode, args []AIArg) AIType {
			return AIStringType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}},
		FormalArg{Positional: true, Required: false, Types: []ValueType{IntType}, DefaultValue: NewInt(math.MaxInt64)})

	RegisterBuiltinFunc("string_replace",
		`
    string_replace(str, old, new)

Arg types:

- _str_: string
- _old_: string
_ _new_: string

Example:
    regexp_replace("dogo", "o" "a") == "daga"

Replace occurrence of old in str with new.`,
		builtinStringReplace,
		func(ast ASTNode, args []AIArg) AIType { return AIStringType },
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}})

	RegisterBuiltinFunc("string_has_prefix",
		`
    string_has_prefix(str, prefix)

Arg types:

- _str_: string
- _prefix_: string

Example:
    string_has_prefix("dog", "d") == true

Checks if a string starts with the given prefix`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			str := args[0].Str()
			prefix := args[1].Str()
			return NewBool(strings.HasPrefix(str, prefix))
		},
		func(ast ASTNode, args []AIArg) AIType {
			return AIBoolType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}})

	RegisterBuiltinFunc("string_has_suffix",
		`
    string_has_suffix(str, suffix)

Arg types:

- _str_: string
- _suffix_: string

Example:
    string_has_suffix("dog", "g") == true

Checks if a string ends with the given suffix`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			str := args[0].Str()
			suffix := args[1].Str()
			return NewBool(strings.HasSuffix(str, suffix))
		},
		func(ast ASTNode, args []AIArg) AIType {
			return AIBoolType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}})

	RegisterBuiltinFunc("string_count",
		`
    string_count(str, substr)

Arg types:

- _str_: string
- _substr_: string

Example:
    string_count("good dog!", "g") == 2

Count the number of non-overlapping occurrences of substr in str.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			str := args[0].Str()
			substr := args[1].Str()
			return NewInt(int64(strings.Count(str, substr)))
		},
		func(ast ASTNode, args []AIArg) AIType {
			return AIIntType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StringType}})

	RegisterBuiltinFunc("sprintf",
		`
    sprintf(fmt, args...)

Arg types:

- _fmt_: string
- _args_: any

Example:
    sprintf("hello %s %d", "world", 10) == "hello world 10"

Builds a string from the format string. It is implemented using Go's fmt.Sprintf
The args cannot be structs or tables.`,
		builtinSprintf, stringFuncType,
		positionalArg,
		FormalArg{Positional: true, Variadic: true, DefaultValue: Null})

	RegisterBuiltinFunc("hash64",
		`
    hash64(arg)

Arg types:

- _arg_: any


Example:
    hash64("foohah")

Compute the hash of the arg. Arg can be of any type, including a table or a row.
The hash is a positive int64 value.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			h := args[0].Value.Hash()
			v := binary.LittleEndian.Uint64(h[:]) & 0x7fffffffffffffff
			return NewInt(int64(v))
		}, intFuncType,
		positionalArg)

	RegisterBuiltinFunc("land",
		`
    land(x, y)

Arg types:

- _x_, _y_: int

Example:
    land(0xff, 0x3) == 3

Compute the logical and of two integers.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			x, y := args[0].Int(), args[1].Int()
			return NewInt(x & y)
		},
		func(ast ASTNode, args []AIArg) AIType {
			return AIIntType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}})

	RegisterBuiltinFunc("lor",
		`Usage: lor(x, y)
Example:
    lor(0xff, 0x3) == 255

Compute the logical or of two integers.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			x, y := args[0].Int(), args[1].Int()
			return NewInt(x | y)
		},
		func(ast ASTNode, args []AIArg) AIType {
			return AIIntType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}})

	RegisterBuiltinFunc("isset",
		`
    isset(x, y)

Arg types:

- _x_, _y_: int

Example:
    isset(0x3, 0x1) == true

Compute whether all bits in the second argument are present in the first.
Useful for whether flags are set.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			x, y := args[0].Int(), args[1].Int()
			return NewBool(x&y == y)
		},
		func(ast ASTNode, args []AIArg) AIType {
			return AIBoolType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{IntType}})

	RegisterBuiltinFunc("unionrow",
		`
    unionrow(x, y)

Arg types:

- _x_, _y_: struct

Example:
    unionrow({a:10}, {b:11}) == {a:10,b:11}
    unionrow({a:10, b:11}, {b:12, c:"ab"}) == {a:10, b:11, c:"ab"}

Unionrow merges the columns of the two structs.
If one column appears in both args, the value from the second arg is taken.
Both arguments must be structs.`,
		func(ctx context.Context, ast ASTNode, args []ActualArg) Value {
			xs, ys := args[0].Struct(), args[1].Struct()
			xlen, ylen := xs.Len(), ys.Len()
			vals := map[symbol.ID]Value{}
			cols := make([]symbol.ID, 0, xlen+ylen)
			for i := 0; i < xlen; i++ {
				f := xs.Field(i)
				vals[f.Name] = f.Value
				cols = append(cols, f.Name)
			}
			for i := 0; i < ylen; i++ {
				f := ys.Field(i)
				if _, ok := vals[f.Name]; !ok {
					cols = append(cols, f.Name)
				}
				vals[f.Name] = f.Value
			}
			fields := make([]StructField, len(cols))
			for i, col := range cols {
				fields[i] = StructField{Name: col, Value: vals[col]}
			}
			return NewStruct(NewSimpleStruct(fields...))
		},
		func(ast ASTNode, args []AIArg) AIType {
			return AIStructType
		},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StructType}},
		FormalArg{Positional: true, Required: true, Types: []ValueType{StructType}})
}

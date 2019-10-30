package gql

//go:generate stringer -type ValueType value_type.go

// ValueType defines the type of a Value object.
type ValueType byte

// Caution: these values must be stable. They are saved on disk.
const (
	// InvalidType is a sentinel. It is not a valid value.
	InvalidType ValueType = iota
	// NullType represents a missing value
	NullType
	// BoolType represents a true or a false
	BoolType
	// IntType represents an int64 value
	IntType
	// FloatType represents an float64 value
	FloatType
	// StringType represents a UTF-8-encoded string
	StringType
	// FileNameType represents a UTF-8-encoded filename. FileNameType is a legacy
	// type. It is effectively the same as StringType.
	FileNameType
	// FileNameType represents an enum value. EnumType is a legacy type. It is
	// effectively the same as StringType.
	EnumType
	// CharType represents a UTF-8 character.
	CharType
	// DateType represents time.Time, but at a date granurality.
	DateType
	// DateTimeType represents time.Time.
	DateTimeType
	// DurationType represents time.Duration.
	DurationType
	UnusedType // Available for future use.
	// StructType stores a Struct
	StructType
	// StructFragmentType stores []StructField. It is a result of expanding a
	// regex struct pattern.
	StructFragmentType
	// TableType stores a Table
	TableType
	// FuncType stores a Func
	FuncType
)

// LikeString checks if the value's representation is a string.
func (v ValueType) LikeString() bool {
	return v == StringType || v == EnumType || v == FileNameType
}

// LikeDate checks if the value's representation is a time.Time.
func (v ValueType) LikeDate() bool {
	return v == DateType || v == DateTimeType
}

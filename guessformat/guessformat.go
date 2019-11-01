// Package guessformat provides functions for making best-effort guess of the type
// of a TSV column.
package guessformat

import (
	"strconv"
	"strings"

	"github.com/grailbio/base/log"
)

// Type defines possible types of a TSV column.
type Type uint8

const (
	String Type = iota
	Float
	Int
	Bool

	// Unknown is returned by Add() when its guess is not yet finalized.
	Unknown

	numTypes     = Unknown
	minScoreDiff = 64
)

// IsNull reports if the string reprents a missing value, e.g., "", "NA", "N/A".
func IsNull(v string) bool {
	if v == "" {
		return true
	}
	ch := v[0]
	if ch != 'n' && ch != 'N' && ch != '#' && ch != '-' { // quick check
		return false
	}
	if v == "#N/A" || v == "#N/A N/A" || v == "#NA" || v == "-NaN" ||
		v == "-nan" || v == "N/A" || v == "NA" || v == "NULL" || v == "NaN" || v == "nan" {
		return true
	}
	if len(v) >= 7 && ch == '-' && v[1] == '1' && v[3] == '#' &&
		(v[4:] == "IND" || v[4:] == "QNAN") {
		return true
	}
	return false
}

// ParseBool parses a boolean string. E.g., "true", "y", "Y" returns (true,
// true).  A unparsable string returns (false, false).
func ParseBool(v string) (value bool, ok bool) {
	switch v {
	case "Y", "yes":
		return true, true
	case "N", "no":
		return false, true
	}
	b, err := strconv.ParseBool(v)
	return b, (err == nil)
}

// T is used to guess the type of a column in a TSV file.
//
// The caller creates the object T for each column in the TSV file.  It then
// feeds the values of the column through Add() incrementally. Once Add() has an
// unambiguous guess of the column type, it returns one of String, Float, Int,
// or Bool.
type T struct {
	// Score counts the number of times each data type has appeared.
	score        [numTypes]int
	hasBestGuess bool
	bestGuess    Type
}

// BestGuess can be called any time. It returns the most probable data type
// given the values seen so far.
func (t *T) BestGuess() Type {
	if t.hasBestGuess {
		return t.bestGuess
	}
	bestType := Unknown
	for i, v := range t.score {
		if bestType == Unknown || t.score[bestType] < v {
			bestType = Type(i)
		}
	}
	return bestType
}

// Add should be called with a column value found in the TSV file.  It returns
// String,Float, or Int once it determines the column type.  It returns Unknown
// if the type is still ambiguous.
func (t *T) Add(s string) Type {
	if t.hasBestGuess {
		return t.bestGuess
	}
	if IsNull(s) {
		return Unknown
	}
	if _, err := strconv.ParseInt(s, 0, 64); err == nil {
		t.score[Int]++
	} else if _, err := strconv.ParseFloat(s, 64); err == nil {
		t.score[Float]++
	} else if l := strings.ToLower(s); l == "true" || l == "false" {
		t.score[Bool]++
	} else {
		t.score[String]++
	}
	bestType := Unknown
	nextBestType := Unknown
	for i, v := range t.score {
		if bestType == Unknown || t.score[bestType] < v {
			nextBestType = bestType
			bestType = Type(i)
		} else if nextBestType == Unknown || t.score[nextBestType] < v {
			nextBestType = Type(i)
		}
	}
	diff := t.score[bestType] - t.score[nextBestType]
	if diff < 0 {
		log.Panicf("Bad score: %+v", t.score)
	}
	if diff >= minScoreDiff {
		t.bestGuess = bestType
		t.hasBestGuess = true
		return bestType
	}
	return Unknown
}

// Package guessformat provides functions for making best-effort guess of the type
// of a TSV column.
package guessformat

import (
	"strconv"
	"strings"

	"github.com/grailbio/base/log"
)

// Type defines the type values in a TSV column.
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

// T is used to guess the type of a column in a TSV file.
//
// The caller creates the object T for each column in the TSV file.  It then
// feeds the values of the column through Add() incrementally. Once Add() has an
// unambiguous gess of the column type, it returns one of String, Float, or Int.
type T struct {
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
	if s == "" || s == "NA" || s == "na" {
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

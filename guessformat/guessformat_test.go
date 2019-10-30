package guessformat

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func doGuess(t *testing.T, str string) Type {
	v := T{}
	assert.Equal(t, Unknown, v.Add(str))
	return v.BestGuess()
}

func TestBasic(t *testing.T) {
	assert.Equal(t, Int, doGuess(t, "10"))
	assert.Equal(t, Int, doGuess(t, "0x10"))
	assert.Equal(t, Int, doGuess(t, "-12345"))
	assert.Equal(t, Int, doGuess(t, "045"))
	assert.Equal(t, Float, doGuess(t, "-1.0"))
	assert.Equal(t, Float, doGuess(t, "-1e9"))
	assert.Equal(t, Bool, doGuess(t, "false"))
	assert.Equal(t, Bool, doGuess(t, "true"))
	assert.Equal(t, Bool, doGuess(t, "True"))
	assert.Equal(t, Bool, doGuess(t, "TRUE"))
}

func TestGuess0(t *testing.T) {
	v := T{}
	assert.Equal(t, Unknown, v.Add("foo"))
	assert.Equal(t, String, v.BestGuess())
	for i := 0; i < minScoreDiff-2; i++ {
		assert.Equalf(t, Unknown, v.Add("foo"), "i=%d", i)
	}
	assert.Equal(t, String, v.Add("foo"))
}

func TestGuess1(t *testing.T) {
	v := T{}
	assert.Equal(t, Unknown, v.Add("123"))
	assert.Equal(t, Unknown, v.Add("456"))
	assert.Equal(t, Unknown, v.Add("blah"))
	for i := 0; i < minScoreDiff; i++ {
		assert.Equalf(t, Unknown, v.Add("foo"), "i=%d", i)
	}
	assert.Equal(t, String, v.Add("foo"))
}

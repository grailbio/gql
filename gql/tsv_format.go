package gql

import (
	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/guessformat"
)

// TSVColumn defines a column in a TSV-like file.
type TSVColumn struct {
	// Name is the column name
	Name string
	// Type is the type of the cells in the column.
	Type ValueType
	// Description is an optional description of the column.
	Description string `json:",omitempty"`
}

// TSVFormat defines the format of a TSV file. JSON encodable.
type TSVFormat struct {
	// HeaderLines is the number of comment lines at the beginning of the file.
	// The value is typically 0 or 1.
	HeaderLines int
	// List of columns in each row.
	Columns []TSVColumn
}

// GuessTSVFormat guesses a TSVFormat from the first few rows of a TSV file.
func guessTSVFormat(path string, rawRows [][]string) TSVFormat {
	if len(rawRows) == 0 {
		return TSVFormat{0, nil}
	}
	colNames := rawRows[0]
	guesses := make([]guessformat.T, len(colNames))
	for _, row := range rawRows[1:] {
		n := 0
		for ci, col := range row {
			if ci >= len(guesses) {
				log.Error.Printf("tsv1 %v: extra column(s) found in row '%v'", path, row)
				continue
			}
			if guesses[ci].Add(col) != guessformat.Unknown {
				n++
			}
		}
		if n == len(colNames) { // All the cols have concrete types
			break
		}
	}

	columns := make([]TSVColumn, len(colNames))
	for ci, colName := range colNames {
		columns[ci].Name = colName
		guess := guesses[ci].BestGuess()
		switch guess {
		case guessformat.String:
			columns[ci].Type = StringType
		case guessformat.Int:
			columns[ci].Type = IntType
		case guessformat.Float:
			columns[ci].Type = FloatType
		case guessformat.Bool:
			columns[ci].Type = BoolType
		default:
			log.Debug.Printf("tsv1 %v: illegal format '%v' (guess: %+v} for column %v; maybe the file is empty", path, guess, guesses[ci], colName)
			columns[ci].Type = StringType
		}
	}
	return TSVFormat{1, columns}
}

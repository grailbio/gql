package symbol

import "github.com/grailbio/hts/sam"

// AnonRowName is a variable used to store a row that's not a struct.  For a
// struct, each field becomes a separate variable.
const AnonRowName = "_"

// AnonAccName and AnonValName are passed to the reduce combiner.
const AnonAccName = "_acc"
const AnonValName = "_val"

var (
	// List of frequently used symbols.
	Chrom          = Intern("chrom")
	Date           = Intern("date")
	Default        = Intern("default")
	End            = Intern("end")
	Feat           = Intern("feat")
	Filter         = Intern("filter")
	Key            = Intern("key")
	Length         = Intern("length")
	Map            = Intern("map")
	Name           = Intern("name")
	Path           = Intern("path")
	Pos            = Intern("pos")
	Row            = Intern("row")
	Shards         = Intern("shards")
	Start          = Intern("start")
	Type           = Intern("type")
	Value          = Intern("value")
	Subshard       = Intern("subshard")
	Depth          = Intern("depth")
	Mode           = Intern("mode")
	GZIP           = Intern("gzip")

	// Fragment table field names.
	Reference                     = Intern("reference")

	// BAM table field names.
	MapQ  = Intern("mapq")
	Cigar = Intern("cigar")
	Flags = Intern("flags")
	RNext = Intern("rnext")
	PNext = Intern("pnext")
	TLen  = Intern("tlen")
	Seq   = Intern("seq")
	Qual  = Intern("qual")
	// AuxTags contains common aux tags.
	AuxTags      = map[sam.Tag]ID{}
	AuxTagsIndex = map[ID]sam.Tag{}

	AnonRow = Intern(AnonRowName)
	AnonAcc = Intern(AnonAccName)
	AnonVal = Intern(AnonValName)
)

func init() {
	commonTags := []sam.Tag{
		{'H', 'D'},
		{'V', 'N'},
		{'S', 'O'},
		{'G', 'O'},
		{'S', 'Q'},
		{'S', 'N'},
		{'L', 'N'},
		{'A', 'H'},
		{'A', 'S'},
		{'M', '5'},
		{'S', 'P'},
		{'U', 'R'},
		{'R', 'G'},
		{'C', 'N'},
		{'D', 'S'},
		{'D', 'T'},
		{'F', 'O'},
		{'K', 'S'},
		{'L', 'B'},
		{'P', 'I'},
		{'P', 'L'},
		{'P', 'U'},
		{'S', 'M'},
		{'P', 'G'},
		{'I', 'D'},
		{'P', 'N'},
		{'C', 'L'},
		{'P', 'P'},
		{'D', 'S'},
		{'C', 'O'},
	}
	for _, tag := range commonTags {
		AuxTags[tag] = Intern(tag.String())
		AuxTagsIndex[AuxTags[tag]] = tag
	}
}

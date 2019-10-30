package gql

// simple 64bit bitmap.
type bitmap64 uint64

// create a bitmap. the least significant n bits are set to 1.
func newbitmap64(n int) bitmap64 {
	return (bitmap64(1) << bitmap64(n)) - 1
}

// If b[pos] is set, this method clears the bit and returns true.  Else, it
// keeps *b unchanged and returns false.
func (b *bitmap64) tryClear(pos int) bool {
	p := bitmap64(1) << bitmap64(pos)
	if *b&p == 0 {
		return false
	}
	*b = *b & ^p
	return true
}

// test checks if the pos'th bit is set
func (b *bitmap64) test(pos int) bool {
	p := bitmap64(1) << bitmap64(pos)
	return *b&p != 0
}

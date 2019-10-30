// Package marshal implements binary encoder and decoder for GQL values.
package marshal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"runtime/debug"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/unsafe"
	"github.com/grailbio/gql/hash"
)

// EncodeGOB is a convenience function for encoding a value using gob.  It
// crashes the process on error.
func EncodeGOB(enc *gob.Encoder, val interface{}) {
	if err := enc.Encode(val); err != nil {
		log.Panicf("gob: failed to encode %v: %v", val, err)
	}
}

// DecodeGOB is a convenience function for decoding a value using gob.  It
// crashes the process on error.
func DecodeGOB(dec *gob.Decoder, val interface{}) {
	if err := dec.Decode(val); err != nil {
		log.Panicf("gob: failed to decode %v: %v: %v", val, err, string(debug.Stack()))
	}
}

// Encoder is used to encode GQL values.
type Encoder struct {
	buf  []byte
	syms map[string]int64
	tmp  [binary.MaxVarintLen64]byte
}

var encoderPool = sync.Pool{New: func() interface{} { return &Encoder{} }}

// NewEncoder creates a new empty encoder. If buf!=nil, the encoder takes
// ownership of the buffer and appends to the buffer (the buffer will be
// reallocated if it turns out to be too small for the encode data). If buf=nil,
// the encoder allocates a new buffer.
//
// The caller should call ReleaseEncoder() after use. The ReleaseEncoder call is
// optional, but it will save memory allocation.
//
// Example:
//   buf := make([]byte, 128)
//   for {
//      enc := marshal.NewEncoder(buf[:0])
//      enc.Write([]byte("foohah"))
//      serialized := marshal.ReleaseDecoder(enc)
//      ... use serialized ...
//   }
func NewEncoder(buf []byte) *Encoder {
	enc := encoderPool.Get().(*Encoder)
	enc.Reset(buf)
	return enc
}

// ReleaseEncoder releases the encoder into the freepool. It returns value of
// enc.Bytes().
func ReleaseEncoder(enc *Encoder) []byte {
	data := enc.Bytes()
	enc.Reset(nil)
	encoderPool.Put(enc)
	return data
}

func (e *Encoder) Reset(buf []byte) {
	for k := range e.syms {
		delete(e.syms, k)
	}
	e.buf = buf[:0]
}

// Reallocate e.buf so that it can store at least delta more bytes of data.
func (e *Encoder) reserve(delta int) []byte {
	curLen := len(e.buf)
	newLen := delta + len(e.buf)
	if newLen <= cap(e.buf) {
		e.buf = e.buf[:newLen]
		return e.buf[curLen:]
	}
	newCap := cap(e.buf) * 2
	if newCap < 128 {
		newCap = 128
	}
	for newCap < newLen {
		newCap *= 2
	}
	tmp := make([]byte, newCap)
	copy(tmp, e.buf)
	e.buf = tmp[:newLen]
	return e.buf[curLen:]
}

// Write implements io.Writer interface
func (e *Encoder) Write(data []byte) (int, error) {
	n := len(data)
	copy(e.reserve(n), data)
	return n, nil
}

// Append raw bytes to the buffer.
func (e *Encoder) write(data []byte) {
	copy(e.reserve(len(data)), data)
}

// Len returns the size of the encoded data.
func (e *Encoder) Len() int { return len(e.buf) }

// PutGOB encodes an arbitrary value using GOB.
func (e *Encoder) PutGOB(val interface{}) {
	gobe := gob.NewEncoder(e)
	if err := gobe.Encode(val); err != nil {
		log.Panicf("gob: failed to encode %v: %v", val, err)
	}
}

// PutVarint adds an int64.
func (e *Encoder) PutVarint(v int64) {
	n := binary.PutVarint(e.tmp[:], v)
	e.write(e.tmp[0:n])
}

// PutHash adds a Hash object to the buffer.
func (e *Encoder) PutHash(h hash.Hash) {
	e.write(h[:])
}

// PutUint64 adds an uint64 to the buffer.
func (e *Encoder) PutUint64(v uint64) {
	binary.LittleEndian.PutUint64(e.tmp[:8], v)
	e.write(e.tmp[:8])
}

// PutByte adds a byte to the buffer.
func (e *Encoder) PutByte(b byte) {
	buf := e.reserve(1)
	buf[0] = b
}

// PutBool adds a bool to the buffer.
func (e *Encoder) PutBool(b bool) {
	if b {
		e.PutByte(1)
		return
	}
	e.PutByte(0)
}

// PutRawBytes adds the given data to the buffer. It does not encode the data
// length, so the reader must know the data size beforehand.
func (e *Encoder) PutRawBytes(data []byte) {
	e.write(data)
}

// PutBytes adds data to the buffer. It encodes the data length.
func (e *Encoder) PutBytes(data []byte) {
	e.PutVarint(int64(len(data)))
	e.write(data)
}

// PutString adds string to the buffer.
func (e *Encoder) PutString(data string) {
	e.PutBytes(unsafe.StringToBytes(data))
}

// PutSymbol adds a symbol to the stream. Symbols are expected to
// be repeated and are interned as integers into the stream. The
// encoder maintains a table of symbols.
func (e *Encoder) PutSymbol(id string) {
	if e.syms == nil {
		e.syms = make(map[string]int64)
	}
	v, ok := e.syms[id]
	if ok {
		e.PutVarint(v)
	} else {
		v = int64(len(e.syms)) + 1
		e.syms[id] = v
		e.PutVarint(-v)
		e.PutString(id)
	}
}

// Bytes returns the encoded data.
func (e *Encoder) Bytes() []byte { return e.buf }

// Decoder is used to decode GQL values.
type Decoder struct {
	buf  *bytes.Reader
	syms map[int64]string
	tmp  [binary.MaxVarintLen64]byte
}

var decoderPool = sync.Pool{New: func() interface{} { return &Decoder{buf: bytes.NewReader(nil)} }}

// NewDecoder creates a Decoder that reads from the given bytes.  The caller
// should call ReleaseDecoder() after use. The ReleaseDecoder call is optional,
// but it will save memory allocation.
func NewDecoder(data []byte) *Decoder {
	dec := decoderPool.Get().(*Decoder)
	dec.Reset(data)
	return dec
}

// ReleaseDecoder releases the decoder into the freepool.
func ReleaseDecoder(dec *Decoder) {
	if dec.buf.Len() != 0 {
		panic("marshal.Decoder: found trailing junk")
	}
	decoderPool.Put(dec)
}

func (d *Decoder) Reset(data []byte) {
	d.buf.Reset(data)
	for k := range d.syms {
		delete(d.syms, k)
	}
}

// Byte reads a byte from the decoder. Crashes the process on error.
func (d *Decoder) Byte() byte {
	b, err := d.buf.ReadByte()
	if err != nil {
		panic("unmarshalByte")
	}
	return b
}

// Bool reads a bool from the decoder. Crashes the process on error.
func (d *Decoder) Bool() bool {
	b := d.Byte()
	return b != 0
}

// Varint reads an int64. Crashes the process on error.
func (d *Decoder) Varint() int64 {
	n, err := binary.ReadVarint(d.buf)
	if err != nil {
		panic("unmarshalVarint")
	}
	return n
}

// Uint64 reads an uint64. Crashes the process on error.
func (d *Decoder) Uint64() uint64 {
	data := d.tmp[:8]
	n, err := d.buf.Read(data)
	if n < 8 || err != nil {
		panic("unmarshalUint64")
	}
	return binary.LittleEndian.Uint64(data)
}

// GOB reads an arbitrary value using GOB. The val should be a pointer to an
// object.
func (d *Decoder) GOB(val interface{}) {
	gobd := gob.NewDecoder(d.buf)
	if err := gobd.Decode(val); err != nil {
		log.Panicf("gob: failed to decode: %v", err)
	}
}

// Bytes reads data encoded by Encoder.PutBytes.
func (d *Decoder) Bytes() []byte {
	n := d.Varint()
	if n == 0 {
		return nil
	}
	data := make([]byte, n)
	nn, err := d.buf.Read(data)
	if int64(nn) != n || err != nil {
		log.Panicf("unmarshalBytes: %d %d %v", n, nn, err)
	}
	return data
}

// Bytes reads data encoded by Encoder.PutString.
func (d *Decoder) String() string {
	data := d.Bytes()
	return unsafe.BytesToString(data)
}

// Symbol decodes a symbol encoded by Encoder.PutSymbol.
func (d *Decoder) Symbol() string {
	if d.syms == nil {
		d.syms = make(map[int64]string)
	}
	v := d.Varint()
	if v < 0 {
		v = -v
		d.syms[v] = d.String()
	}
	id, ok := d.syms[v]
	if !ok {
		log.Panicf("name for symbol %d not defined in stream", v)
	}
	return id
}

// Len returns the number of bytes that remains to be read.
func (d *Decoder) Len() int { return d.buf.Len() }

// RawBytes reads a byte slice from the decoder.  It crashes the process if the
// decoder stores less than len(data) bytes.
func (d *Decoder) RawBytes(data []byte) {
	n, err := d.buf.Read(data)
	if n != len(data) || err != nil {
		log.Panicf("Decoder.RawBytes: %d %d %v", n, len(data), err)
	}
}

// Hash reads a Hash object from the decoder.
func (d *Decoder) Hash() hash.Hash {
	var h hash.Hash
	d.RawBytes(h[:])
	return h
}

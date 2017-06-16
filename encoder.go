package voltdb

import (
	"bytes"
	"encoding/binary"
)

//size of bytes
const (
	byteSize    = 1
	shortSize   = 2
	integerSize = 4
	longSize    = 8
)

// We are using big endian t encode the values for voltdb wire protocol
var endian = binary.BigEndian

// Encoder defines methods for encoding Go values to voltdb wire protocol. This
// struct is reusable, you can call Reset method and start encodeing new fresh
// values.
//
// Values are encoded in Big Endian byte order mark.
//
// To retrieve []byte of the encoded values use Bytes method.
type Encoder struct {
	buf *bytes.Buffer
}

// NewEncoder returns a new Encoder instance
func NewEncoder() *Encoder {
	return &Encoder{buf: &bytes.Buffer{}}
}

//Reset resets the underlying buffer. This will remove any values that were
//encoded before.
//
//Call this to reuse the Encoder and avoid unnecessary allocations.
func (e *Encoder) Reset() {
	e.buf.Reset()
}

// Byte encodes int8 value of voltdb wire protocol Byte. This returns the number
// of bytes written and an error if any.
//
// For a successful encoding the value of number of bytes written is 1
func (e *Encoder) Byte(v int8) (int, error) {
	b := make([]byte, byteSize)
	b[0] = byte(v)
	return e.buf.Write(b)
}

// Bytes returns the buffered voltdb wire protocol encoded bytes
func (e *Encoder) Bytes() []byte {
	return e.buf.Bytes()
}

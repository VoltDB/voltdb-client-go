package voltdb

import (
	"bytes"
	"encoding/binary"
	"math"
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

// Int16 encodes int16 value to voltdb wire protocol Short. For a successful
// encoding the number of bytes written is 2
func (e *Encoder) Int16(v int16) (int, error) {
	return e.uint16(uint16(v))
}

func (e *Encoder) uint16(v uint16) (int, error) {
	b := make([]byte, shortSize)
	endian.PutUint16(b, v)
	return e.buf.Write(b)
}

// Int32 encodes int32 value to voltdb wire protocol Integer. For a successful
// encoding the number of bytes written is 4
func (e *Encoder) Int32(v int32) (int, error) {
	return e.uint32(uint32(v))
}

func (e *Encoder) uint32(v uint32) (int, error) {
	b := make([]byte, integerSize)
	endian.PutUint32(b, v)
	return e.buf.Write(b)
}

// Int64 encodes int64 value into voltdb wire protocol Long. For a successful
// encoding the number of bytes written is 8
func (e *Encoder) Int64(v int64) (int, error) {
	return e.uint64(uint64(v))
}

func (e *Encoder) uint64(v uint64) (int, error) {
	b := make([]byte, longSize)
	endian.PutUint64(b, v)
	return e.buf.Write(b)
}

// Float64 encodes float64 value to voltdb wire protocol float type. This uses
// math.Float64bits to covert v to uint64 which is encoded into []byte of size
// 8.  For a successful encoding the number of bytes written is 8
func (e *Encoder) Float64(v float64) (int, error) {
	return e.uint64(math.Float64bits(v))
}

// Binary encodes []byte to voltdb wire protocol varbinary
//
// This first encodes the size of v as voltdb Short followed by v as raw bytes.
func (e *Encoder) Binary(v []byte) (int, error) {
	s, err := e.Int32(int32(len(v)))
	if err != nil {
		return 0, err
	}
	n, err := e.buf.Write(v)
	if err != nil {
		return 0, err
	}
	return s + n, nil
}

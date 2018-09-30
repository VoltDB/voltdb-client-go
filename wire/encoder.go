package wire

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"hash"
	"math"
	"reflect"
	"time"
)

//size of bytes
const ()

//Column types
const (
	ArrayColumn     int8 = -99 // array (short)(values*)
	NullColumn      int8 = 1   // null
	BoolColumn      int8 = 3   // boolean
	ByteColumn      int8 = 3   // byte
	ShortColumn     int8 = 4   // int16
	IntColumn       int8 = 5   // int32
	LongColumn      int8 = 6   // int64
	FloatColumn     int8 = 8   // float64
	StringColumn    int8 = 9   // string (int32-length-prefix)(utf-8 bytes)
	TimestampColumn int8 = 11  // int64 timestamp microseconds
	Table           int8 = 21  // VoltTable
	DecimalColumn   int8 = 22  // fix-scaled, fix-precision decimal
	VarBinColumn    int8 = 25  // varbinary (int)(bytes)
)

var errUnknownParam = errors.New("voltdbclient: unknown parameter type")

// We are using big endian to encode the values for voltdb wire protocol
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
	tmp *bytes.Buffer
}

// NewEncoder returns a new Encoder instance
func NewEncoder() *Encoder {
	return &Encoder{buf: &bytes.Buffer{}, tmp: &bytes.Buffer{}}
}

//Reset resets the underlying buffer. This will remove any values that were
//encoded before.
//
//Call this to reuse the Encoder and avoid unnecessary allocations.
func (e *Encoder) Reset() {
	e.buf.Reset()
	e.tmp.Reset()
}

// Len retuns the size of the cueent encoded values
func (e *Encoder) Len() int {
	return e.buf.Len()
}

// Byte encodes int8 value to voltdb wire protocol Byte. This returns the number
// of bytes written and an error if any.
//
// For a successful encoding the value of number of bytes written is 1
func (e *Encoder) Byte(v int8) (int, error) {
	b := make([]byte, ByteSize)
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
	b := make([]byte, ShortSize)
	endian.PutUint16(b, v)
	return e.buf.Write(b)
}

// Int32 encodes int32 value to voltdb wire protocol Integer. For a successful
// encoding the number of bytes written is 4
func (e *Encoder) Int32(v int32) (int, error) {
	return e.uint32(uint32(v))
}

func (e *Encoder) uint32(v uint32) (int, error) {
	b := make([]byte, IntegerSize)
	endian.PutUint32(b, v)
	return e.buf.Write(b)
}

// Int64 encodes int64 value into voltdb wire protocol Long. For a successful
// encoding the number of bytes written is 8
func (e *Encoder) Int64(v int64) (int, error) {
	return e.uint64(uint64(v))
}

func (e *Encoder) uint64(v uint64) (int, error) {
	b := make([]byte, LongSize)
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
// This first encodes the size of v as voltdb Integer followed by raw bytes of v.
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

// Bool encodes bool values to voltdb wireprotocol boolean
func (e *Encoder) Bool(v bool) (int, error) {
	if v {
		return e.Byte(0x1)
	}
	return e.Byte(0x0)
}

// String encodes strings to voltdb wire protocol string. A string is treated
// like []byte. We first encode the size of the string, followed by the raw
// bytes of the string.
func (e *Encoder) String(v string) (int, error) {
	e.tmp.Reset()
	e.tmp.WriteString(v)
	return e.Binary(e.tmp.Bytes())
}

// Time encodes time.Time value to voltdb wire protocol time.
func (e *Encoder) Time(v time.Time) (int, error) {
	nano := v.Round(time.Microsecond).UnixNano()
	if v.IsZero() {
		return e.Int64(math.MinInt64)
	}
	return e.Int64(nano / int64(time.Microsecond))
}

// Write implements io.Writer interface
func (e *Encoder) Write(b []byte) (int, error) {
	return e.buf.Write(b)
}

// Read implements io.Reader interface
func (e *Encoder) Read(b []byte) (int, error) {
	return e.buf.Read(b)
}

// Marshal encodes query arguments, these are values passed as driver.Value when
// executing queries
func (e *Encoder) Marshal(v interface{}) (int, error) {
	switch x := v.(type) {
	case bool:
		return e.MarshalBool(x)
	case int8:
		return e.MarshalByte(x)
	case int16:
		return e.MarshalShort(x)
	case int32:
		return e.MarshalInt32(x)
	case int64:
		return e.MarshalInt64(x)
	case float64:
		return e.MarshalFloat64(x)
	case string:
		return e.MarshalString(x)
	case time.Time:
		return e.MarshalTime(x)
	default:
		rv := reflect.ValueOf(v)
		if !rv.IsValid() || rv.IsNil() {
			return e.MarshalNil()
		}
		switch rv.Kind() {
		case reflect.Slice:
			return e.MarshalSlice(rv)
		case reflect.Ptr:
			return e.Marshal(rv.Elem().Interface())
		}
		return 0, errUnknownParam
	}
}

// MarshalNil encodes nil
func (e *Encoder) MarshalNil() (int, error) {
	return e.Byte(NullColumn)
}

// MarshalBool encodes boolean argument
func (e *Encoder) MarshalBool(v bool) (int, error) {
	n, err := e.Byte(BoolColumn)
	if err != nil {
		return 0, err
	}
	i, err := e.Bool(v)
	if err != nil {
		return 0, err
	}
	return n + i, nil
}

// MarshalByte encodes int8 argument
func (e *Encoder) MarshalByte(v int8) (int, error) {
	n, err := e.Byte(ByteColumn)
	if err != nil {
		return 0, err
	}
	i, err := e.Byte(v)
	if err != nil {
		return 0, err
	}
	return n + i, nil
}

// MarshalShort encodes int16 argument
func (e *Encoder) MarshalShort(v int16) (int, error) {
	n, err := e.Byte(ShortColumn)
	if err != nil {
		return 0, err
	}
	i, err := e.Int16(v)
	if err != nil {
		return 0, err
	}
	return n + i, nil
}

// MarshalInt32 encodes int32 argument
func (e *Encoder) MarshalInt32(v int32) (int, error) {
	n, err := e.Byte(IntColumn)
	if err != nil {
		return 0, err
	}
	i, err := e.Int32(v)
	if err != nil {
		return 0, err
	}
	return n + i, nil
}

// MarshalInt64 encodes int64 argument
func (e *Encoder) MarshalInt64(v int64) (int, error) {
	n, err := e.Byte(LongColumn)
	if err != nil {
		return 0, err
	}
	i, err := e.Int64(v)
	if err != nil {
		return 0, err
	}
	return n + i, nil
}

// MarshalFloat64 encodes float64 argument
func (e *Encoder) MarshalFloat64(v float64) (int, error) {
	n, err := e.Byte(FloatColumn)
	if err != nil {
		return 0, err
	}
	i, err := e.Float64(v)
	return n + i, nil
}

// MarshalString encodes string argument
func (e *Encoder) MarshalString(v string) (int, error) {
	n, err := e.Byte(StringColumn)
	if err != nil {
		return 0, err
	}
	i, err := e.String(v)
	if err != nil {
		return 0, err
	}
	return n + i, nil
}

// MarshalSlice encodes slice of arguments
func (e *Encoder) MarshalSlice(v reflect.Value) (int, error) {
	switch v.Type().Elem().Kind() {
	case reflect.Uint8:
		n, err := e.Byte(VarBinColumn)
		if err != nil {
			return 0, err
		}
		i, err := e.Binary(v.Bytes())
		if err != nil {
			return 0, err
		}
		return n + i, nil
	default:
		n, err := e.Byte(ArrayColumn)
		if err != nil {
			return 0, err
		}
		l := v.Len()
		s, err := e.Int16(int16(l))
		if err != nil {
			return 0, err
		}
		size := n + s
		for i := 0; i < l; i++ {
			c, err := e.Marshal(v.Index(i).Interface())
			if err != nil {
				return 0, err
			}
			size += c
		}
		return size, nil
	}
}

// MarshalTime encodes time.Time argument
func (e *Encoder) MarshalTime(v time.Time) (int, error) {
	n, err := e.Byte(TimestampColumn)
	if err != nil {
		return 0, err
	}
	i, err := e.Time(v)
	if err != nil {
		return 0, err
	}
	return n + i, nil
}

// Args a helper to encode driver arguments
func (e *Encoder) Args(v []driver.Value) error {
	_, err := e.Int16(int16(len(v)))
	if err != nil {
		return err
	}
	for i := 0; i < len(v); i++ {
		_, err = e.Marshal(v[i])
		if err != nil {
			return err
		}
	}
	return nil
}

//Login encodes login details. This supports both version 0 and 1 of the wire
//protocol.
//
//The password is hashed using sha1 and sha356 for version 0 and 1 respectively.
//
// For instance if the username is foo and password is bar,  the login message
// will be encoded as follows
//
// version 0
// 	+------------------+--------------+-----------------------+----------+--------------------------------------+
// 	| protocol version | service name | password hash version | username | password                             |
// 	+------------------+--------------+-----------------------+----------+--------------------------------------+
// 	| 0                | database     | 0                     | foo      | sha1 encoded raw bytes of string bar |
// 	+------------------+--------------+-----------------------+----------+--------------------------------------+
//
// version 1
// 	+------------------+--------------+-----------------------+----------+----------------------------------------+
// 	| protocol version | service name | password hash version | username | password                               |
// 	+------------------+--------------+-----------------------+----------+----------------------------------------+
// 	| 1                | database     | 1                     | foo      | sha256 encoded raw bytes of string bar |
// 	+------------------+--------------+-----------------------+----------+----------------------------------------+
func (e *Encoder) Login(version int, user, password string) ([]byte, error) {
	var h hash.Hash
	_, err := e.Byte(int8(version))
	if err != nil {
		return nil, err
	}
	if version == 0 {
		h = sha1.New()
		//password hash version
		_, err = e.Byte(0)
		if err != nil {
			return nil, err
		}
	} else {
		h = sha256.New()

		//password hash version
		_, err = e.Byte(1)
		if err != nil {
			return nil, err
		}
	}
	_, err = h.Write([]byte(password))
	if err != nil {
		return nil, err
	}
	_, err = e.String("database")
	if err != nil {
		return nil, err
	}

	_, err = e.String(user)
	if err != nil {
		return nil, err
	}
	_, err = e.Write(h.Sum(nil))
	if err != nil {
		return nil, err
	}
	return e.Message(e.Bytes()), nil
}

// Message encodes v into a voldb wire protocol. voltdb wire protocol message
// comprizes of int32 encoded size of v followed by v raw bytes.
func (e *Encoder) Message(v []byte) []byte {
	b := make([]byte, IntegerSize)
	endian.PutUint32(b, uint32(len(v)))
	return append(b, v...)
}

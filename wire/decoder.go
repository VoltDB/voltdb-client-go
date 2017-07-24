package wire

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net"
	"time"
)

//integer sizes
const (
	ByteSize    = 1
	ShortSize   = 2
	IntegerSize = 4
	LongSize    = 8
)

// ConnInfo contains information about the database connection. This is returned
// from the database on a successful login.
type ConnInfo struct {

	// HostID is the host ID of the volt node
	HostID int32

	// Connection unique id for the connection in the current volt node
	Connection int64

	// Time  in which the cluster was started
	ClusterStart time.Time

	// IPV4 address of the leader node
	LeaderAddr struct {
		Value int32
		IP    net.IP
	}

	// Representation of the build of the connected node
	Build string
}

// Decoder is voltdb wire protocol decoder
type Decoder struct {
	r io.Reader
}

// NewDecoder returns a new Decoder that decods values read from src
func NewDecoder(src io.Reader) *Decoder {
	return &Decoder{r: src}
}

// SetReader replaces the underlying reader, next call to Decode methods will
// read from this
func (d *Decoder) SetReader(r io.Reader) {
	d.r = r
}

// Reset clears the underlying io.Reader . This sets the reader to nil.
func (d *Decoder) Reset() {
	d.r = nil
}

// Int32 reads and decodes voltdb wire protocol encoded []byte to int32.
func (d *Decoder) Int32() (int32, error) {
	u, err := d.Uint32()
	if err != nil {
		return 0, err
	}
	return int32(u), nil
}

// Uint32 reads and decodes voltdb wire protocol encoded []byte into uint32.
//
// This reads 4 bytes from the the underlying io.Reader, assuming the io.Reader
// is for voltdb wire protocol encoded bytes stream. Then the bytes read are
// decoded as uint32 using big endianess
func (d *Decoder) Uint32() (uint32, error) {
	var a [IntegerSize]byte
	b := a[:]
	_, err := d.r.Read(b)
	if err != nil {
		return 0, err
	}
	v := endian.Uint32(b)
	return v, nil
}

// Int64 reads and decodes voltdb wire protocol encoded []byte to int64.
func (d *Decoder) Int64() (int64, error) {
	u, err := d.Uint64()
	if err != nil {
		return 0, err
	}
	return int64(u), nil
}

// Uint64 reads and decodes voltdb wire protocol encoded []byte into uint64.
//
// This reads 8 bytes from the the underlying io.Reader, assuming the io.Reader
// is for voltdb wire protocol encoded bytes stream. Then the bytes read are
// decoded as uint64 using big endianess
func (d *Decoder) Uint64() (uint64, error) {
	var a [LongSize]byte
	b := a[:]
	_, err := d.r.Read(b)
	if err != nil {
		return 0, err
	}
	v := endian.Uint64(b)
	return v, nil
}

// Time reads and decodes voltdb wire protocol encoded []byte to time.Time.
func (d *Decoder) Time() (time.Time, error) {
	v, err := d.Int64()
	if err != nil {
		return time.Time{}, err
	}
	if v != math.MinInt64 {
		ts := time.Unix(0, v*int64(time.Microsecond))
		return ts.Round(time.Microsecond), err
	}
	return time.Time{}, nil
}

// Float64 reads and decodes voltdb wire protocol encoded []byte to float64.
func (d *Decoder) Float64() (float64, error) {
	v, err := d.Uint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

// String reads and decodes voltdb wire protocol encoded []byte to string.
func (d *Decoder) String() (string, error) {
	length, err := d.Int32()
	if err != nil {
		return "", err
	}
	if length == -1 {
		return "", nil
	}
	b := make([]byte, length)
	_, err = d.r.Read(b)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Uint16 reads and decodes voltdb wire protocol encoded []byte into uint16.
//
// This reads 2 bytes from the the underlying io.Reader, assuming the io.Reader
// is for voltdb wire protocol encoded bytes stream. Then the bytes read are
// decoded as uint16 using big endianess
func (d *Decoder) Uint16() (uint16, error) {
	var a [ShortSize]byte
	b := a[:]
	_, err := d.r.Read(b)
	if err != nil {
		return 0, err
	}
	v := endian.Uint16(b)
	return v, nil
}

// Int16 reads and decodes voltdb wire protocol encoded []byte to int16.
func (d *Decoder) Int16() (int16, error) {
	v, err := d.Uint16()
	if err != nil {
		return 0, err
	}
	return int16(v), nil
}

// StringSlice reads and decodes voltdb wire protocol encoded []byte to []string.
func (d *Decoder) StringSlice() ([]string, error) {
	size, err := d.Int16()
	if err != nil {
		return nil, err
	}
	a := make([]string, size)
	for i := range a {
		v, err := d.String()
		if err != nil {
			return nil, err
		}
		a[i] = v
	}
	return a, nil
}

// Read implements io.Reader
func (d *Decoder) Read(b []byte) (int, error) {
	return d.r.Read(b)
}

// Message reads a voltdb wire protocol encoded message block from the
// underlying io.Reader.
//
// A message is represented as, a message header followed by the message body.
// The message header is an int32 value dictacting the size of the message i.e
// how many bytes  the message body occupies. The message body is the next n
// bytes after the message header where n is the value of the message header.
func (d *Decoder) Message() ([]byte, error) {
	size, err := d.MessageHeader()
	if err != nil {
		return nil, err
	}
	b := make([]byte, size)
	_, err = io.ReadFull(d.r, b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// MessageHeader reads an int32 value representing the size of the encoded
// message body.
func (d *Decoder) MessageHeader() (int32, error) {
	return d.Int32()
}

// Byte reads and decodes voltdb wire protocol encoded []byte to int8.
func (d *Decoder) Byte() (int8, error) {
	var a [ByteSize]byte
	b := a[:]
	_, err := d.r.Read(b)
	if err != nil {
		return 0, err
	}
	v := a[0]
	return int8(v), nil
}

// Login decodes response message received after successful logging to a voltdb
// database.
//
// The response is first decoded for voltdb wire protocol message i.e first we
// read an int32 value to know the size of the encoded response, then we read
// the next n bytes where n is the size of the message.
//
// The message is then decoded  to *ConnInfo by calling (*Decoder).LoginInfo.
func (d *Decoder) Login() (*ConnInfo, error) {
	msg, err := d.Message()
	if err != nil {
		return nil, err
	}
	return NewDecoder(bytes.NewReader(msg)).LoginInfo()
}

//LoginInfo decodes login message.
func (d *Decoder) LoginInfo() (*ConnInfo, error) {
	c := &ConnInfo{}

	// version
	_, err := d.Byte()
	if err != nil {
		return nil, err
	}
	// auth code
	code, err := d.Byte()
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, fmt.Errorf("voltdbclient:authorization failed with code %d", code)
	}

	host, err := d.Int32()
	if err != nil {
		return nil, err
	}
	c.HostID = host

	conn, err := d.Int64()
	if err != nil {
		return nil, err
	}
	c.Connection = conn

	start, err := d.Time()
	if err != nil {
		return nil, err
	}
	c.ClusterStart = start

	leader, err := d.Int32()
	if err != nil {
		return nil, err
	}
	c.LeaderAddr.Value = leader
	c.LeaderAddr.IP = make(net.IP, IntegerSize)
	endian.PutUint32(c.LeaderAddr.IP, uint32(leader))

	build, err := d.String()
	if err != nil {
		return nil, err
	}
	c.Build = build
	return c, nil
}

// DecoderAt is like Decoder but accepts additional offset that is used to
// determine where to read on the underlying io.ReaderAt
type DecoderAt struct {
	r io.ReaderAt
}

// NewDecoderAt retruns new DecoderAt instance that is reading from r.
func NewDecoderAt(r io.ReaderAt) *DecoderAt {
	return &DecoderAt{r: r}
}

// SetReaderAt changes the underlying io.ReaderAt to r. Any next call on the
// *Decoder methods will read from r.
func (d *DecoderAt) SetReaderAt(r io.ReaderAt) {
	d.r = r
}

func (d *DecoderAt) readAt(size int, offset int64) ([]byte, error) {
	b := make([]byte, size)
	_, err := d.r.ReadAt(b, offset)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// ByteAt decodes voltdb wire protocol encoded []byte read from the given offset
// to int8.
func (d *DecoderAt) ByteAt(offset int64) (byte, error) {
	b, err := d.readAt(ByteSize, offset)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

// Uint32At decodes voltdb wire protocol encoded []byte read from the given
// offset to uint32.
func (d *DecoderAt) Uint32At(offset int64) (uint32, error) {
	b, err := d.readAt(IntegerSize, offset)
	if err != nil {
		return 0, err
	}
	return endian.Uint32(b), nil
}

// Int32At decodes voltdb wire protocol encoded []byte read from the given offset
// to int32.
func (d *DecoderAt) Int32At(offset int64) (int32, error) {
	v, err := d.Uint32At(offset)
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

// ByteSliceAt decodes voltdb wire protocol encoded []byte read from the given
// offset to []byte.
func (d *DecoderAt) ByteSliceAt(offset int64) ([]byte, error) {
	size, err := d.Int32At(offset)
	if err != nil {
		return nil, err
	}
	if size == -1 {
		return nil, nil
	}
	return d.readAt(int(size), offset+IntegerSize)
}

// Uint64At decodes voltdb wire protocol encoded []byte read from the given
// offset to uint64.
func (d *DecoderAt) Uint64At(offset int64) (uint64, error) {
	b, err := d.readAt(LongSize, offset)
	if err != nil {
		return 0, err
	}
	return endian.Uint64(b), nil
}

// Int64At decodes voltdb wire protocol encoded []byte read from the given
// offset to int64.
func (d *DecoderAt) Int64At(offset int64) (int64, error) {
	v, err := d.Uint64At(offset)
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

// Float64At decodes voltdb wire protocol encoded []byte read from the given
// offset to float64.
func (d *DecoderAt) Float64At(offset int64) (float64, error) {
	v, err := d.Uint64At(offset)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

// StringAt decodes voltdb wire protocol encoded []byte read from the given
// offset to string.
func (d *DecoderAt) StringAt(offset int64) (string, error) {
	size, err := d.Int32At(offset)
	if err != nil {
		return "", err
	}
	b, err := d.readAt(int(size), offset+IntegerSize)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Uint16At decodes voltdb wire protocol encoded []byte read from the given
// offset to uint16.
func (d *DecoderAt) Uint16At(offset int64) (uint16, error) {
	b, err := d.readAt(ShortSize, offset)
	if err != nil {
		return 0, err
	}
	return endian.Uint16(b), nil
}

// Int16At decodes voltdb wire protocol encoded []byte read from the given
// offset to int16.
func (d *DecoderAt) Int16At(offset int64) (int16, error) {
	v, err := d.Uint16At(offset)
	if err != nil {
		return 0, err
	}
	return int16(v), nil
}

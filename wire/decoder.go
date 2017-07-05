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
	ShortSizer  = 2
	IntegerSize = 4
	LongSize    = 8
)

// ConnInfo contains information about the database connection. This is returned
// from the database on a successful login.
type ConnInfo struct {

	// Host is the host ID of the volt node
	Host int32

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

func (d *Decoder) Reset() {
	d.r = nil
}

func (d *Decoder) Int32() (int32, error) {
	u, err := d.Uint32()
	if err != nil {
		return 0, err
	}
	return int32(u), nil
}

func (d *Decoder) Uint32() (uint32, error) {
	b, err := d.read(IntegerSize)
	if err != nil {
		return 0, err
	}
	return endian.Uint32(b), nil
}

func (d *Decoder) Int64() (int64, error) {
	u, err := d.Uint64()
	if err != nil {
		return 0, err
	}
	return int64(u), nil
}

func (d *Decoder) Uint64() (uint64, error) {
	b, err := d.read(LongSize)
	if err != nil {
		return 0, err
	}
	return endian.Uint64(b), nil
}

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

func (d *Decoder) Float64() (float64, error) {
	v, err := d.Uint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

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

func (d *Decoder) Uint16() (uint16, error) {
	b, err := d.read(ShortSizer)
	if err != nil {
		return 0, err
	}
	return endian.Uint16(b), nil
}

func (d *Decoder) Int16() (int16, error) {
	v, err := d.Uint16()
	if err != nil {
		return 0, err
	}
	return int16(v), nil
}

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

func (d *Decoder) read(size int) ([]byte, error) {
	b := make([]byte, size)
	_, err := d.r.Read(b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Read implements io.Reader
func (d *Decoder) Read(b []byte) (int, error) {
	return d.r.Read(b)
}

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

func (d *Decoder) MessageHeader() (int32, error) {
	return d.Int32()
}

func (d *Decoder) Byte() (int8, error) {
	b, err := d.read(ByteSize)
	if err != nil {
		return 0, err
	}
	return int8(b[0]), nil
}

func (d *Decoder) Login() (*ConnInfo, error) {
	msg, err := d.Message()
	if err != nil {
		return nil, err
	}
	return NewDecoder(bytes.NewReader(msg)).LoginInfo()
}

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
	c.Host = host

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

type DecoderAt struct {
	r io.ReaderAt
}

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

func (d *DecoderAt) ByteAt(offset int64) (byte, error) {
	b, err := d.readAt(ByteSize, offset)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (d *DecoderAt) Uint32At(offset int64) (uint32, error) {
	b, err := d.readAt(IntegerSize, offset)
	if err != nil {
		return 0, err
	}
	return endian.Uint32(b), nil
}

func (d *DecoderAt) Int32At(offset int64) (int32, error) {
	v, err := d.Uint32At(offset)
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

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

func (d *DecoderAt) Uint64At(offset int64) (uint64, error) {
	b, err := d.readAt(LongSize, offset)
	if err != nil {
		return 0, err
	}
	return endian.Uint64(b), nil
}

func (d *DecoderAt) Int64At(offset int64) (int64, error) {
	v, err := d.Uint64At(offset)
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

func (d *DecoderAt) Float64At(offset int64) (float64, error) {
	v, err := d.Uint64At(offset)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

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

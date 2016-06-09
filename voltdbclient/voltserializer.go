/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package voltdbclient

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"time"
)

// package private methods that perform voltdb compatible
// de/serialization on the base wire protocol types.
// See: http://community.voltdb.com/docs/WireProtocol/index

const (
	vt_ARRAY     int8 = -99 // array (short)(values*)
	vt_NULL      int8 = 1   // null
	VT_BOOL      int8 = 3   // boolean, byte
	VT_SHORT     int8 = 4   // int16
	VT_INT       int8 = 5   // int32
	VT_LONG      int8 = 6   // int64
	VT_FLOAT     int8 = 8   // float64
	VT_STRING    int8 = 9   // string (int32-length-prefix)(utf-8 bytes)
	VT_TIMESTAMP int8 = 11  // int64 timestamp microseconds
	vt_TABLE     int8 = 21  // VoltTable
	VT_DECIMAL   int8 = 22  // fix-scaled, fix-precision decimal
	VT_VARBIN    int8 = 25  // varbinary (int)(bytes)
)

var order = binary.BigEndian

// protoVersion is the implemented VoltDB wireprotocol version.
const protoVersion = 1

// reads and deserializes a procedure call response from the server.
func readResponse(r io.Reader) (*Response, error) {
	buf, err := readMessage(r)
	if err != nil {
		return nil, err
	}
	return deserializeCallResponse(buf)
}

// reads a message
func readMessage(r io.Reader) (*bytes.Buffer, error) {
	size, err := readMessageHdr(r)
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err = io.ReadFull(r, data); err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)

	// Version Byte 1
	// TODO: error on incorrect version.
	if _, err = readByte(buf); err != nil {
		return nil, err
	}

	return buf, nil
}

// readMessageHdr reads the standard wireprotocol header.
func readMessageHdr(r io.Reader) (size int32, err error) {
	// Total message length Integer  4
	size, err = readInt(r)
	if err != nil {
		return
	}
	return (size), nil
}

func writeProtoVersion(w io.Writer) error {
	var b [1]byte
	b[0] = protoVersion
	_, err := w.Write(b[:1])
	return err
}

func writePasswordHashVersion(w io.Writer) error {
	var b [1]byte
	b[0] = 1
	_, err := w.Write(b[:1])
	return err
}

func writeBoolean(w io.Writer, d bool) (err error) {
	if d {
		err = writeByte(w, 0x1)
	} else {
		err = writeByte(w, 0x0)
	}
	return
}

func readBoolean(r io.Reader) (bool, error) {
	val, err := readByte(r)
	if err != nil {
		return false, err
	}
	result := val != 0
	return result, nil
}

func writeByte(w io.Writer, d int8) error {
	var b [1]byte
	b[0] = byte(d)
	_, err := w.Write(b[:1])
	return err
}

func writeBytes(w io.Writer, d []byte) error {
	_, err := w.Write(d)
	return err
}

func readByte(r io.Reader) (int8, error) {
	var b [1]byte
	bs := b[:1]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	return int8(b[0]), nil
}

func readByteArray(r io.Reader) ([]byte, error) {
	// byte arrays have 4 byte length prefixes.
	len, err := readInt(r)
	if err != nil {
		return nil, err
	}
	if len == -1 {
		return nil, nil
	}
	bs := make([]byte, len)
	_, err = r.Read(bs)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func writeShort(w io.Writer, d int16) error {
	var b [2]byte
	bs := b[:2]
	order.PutUint16(bs, uint16(d))
	_, err := w.Write(bs)
	return err
}

func readShort(r io.Reader) (int16, error) {
	var b [2]byte
	bs := b[:2]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	result := order.Uint16(bs)
	return int16(result), nil
}

func writeInt(w io.Writer, d int32) error {
	var b [4]byte
	bs := b[:4]
	order.PutUint32(bs, uint32(d))
	_, err := w.Write(bs)
	return err
}

func readInt(r io.Reader) (int32, error) {
	var b [4]byte
	bs := b[:4]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	result := order.Uint32(bs)
	return int32(result), nil
}

func writeLong(w io.Writer, d int64) error {
	var b [8]byte
	bs := b[:8]
	order.PutUint64(bs, uint64(d))
	_, err := w.Write(bs)
	return err
}

func readLong(r io.Reader) (int64, error) {
	var b [8]byte
	bs := b[:8]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	result := order.Uint64(bs)
	return int64(result), nil
}

func readTimestamp(r io.Reader) (time.Time, error) {
	us, err := readLong(r)
	if us != math.MinInt64 {
		ts := time.Unix(0, us*int64(time.Microsecond))
		return ts.Round(time.Microsecond), err
	}
	return time.Time{}, err
}

func writeTimestamp(w io.Writer, t time.Time) (err error) {
	nanoSeconds := t.Round(time.Microsecond).UnixNano()
	if t.IsZero() {
		return writeLong(w, math.MinInt64)
	}
	return writeLong(w, nanoSeconds/int64(time.Microsecond))
}

func writeFloat(w io.Writer, d float64) error {
	var b [8]byte
	bs := b[:8]
	order.PutUint64(bs, math.Float64bits(d))
	_, err := w.Write(bs)
	return err
}

func readFloat(r io.Reader) (float64, error) {
	var b [8]byte
	bs := b[:8]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	result := order.Uint64(bs)
	return math.Float64frombits(result), nil
}

func writeString(w io.Writer, d string) error {
	writeInt(w, int32(len(d)))
	_, err := io.WriteString(w, d)
	return err
}

func readString(r io.Reader) (result string, err error) {
	result = ""
	length, err := readInt(r)
	if err != nil {
		return
	}
	if length == -1 {
		// NULL string not supported, return zero value
		return
	}
	bs := make([]byte, length)
	_, err = r.Read(bs)
	if err != nil {
		return
	}
	return string(bs), nil
}

func readStringArray(r io.Reader) ([]string, error) {
	cnt, err := readShort(r)
	if err != nil {
		return nil, err
	}
	arr := make([]string, cnt)
	for idx := range arr {
		val, err := readString(r)
		if err != nil {
			return nil, err
		}
		arr[idx] = val
	}
	return arr, nil
}

// The login message password is written as a raw 20 bytes
// without a length prefix.
func writePasswordBytes(w io.Writer, d []byte) error {
	_, err := w.Write(d)
	return err
}

func writeVarbinary(w io.Writer, d []byte) error {
	writeInt(w, int32(len(d)))
	_, err := w.Write(d)
	return err
}

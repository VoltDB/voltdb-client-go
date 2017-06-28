/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

// The set of VoltDB column types and their associated golang type.
const (
	VTArray     int8 = -99 // array (short)(values*)
	VTNull      int8 = 1   // null
	VTBool      int8 = 3   // boolean, byte
	VTShort     int8 = 4   // int16
	VTInt       int8 = 5   // int32
	VTLong      int8 = 6   // int64
	VTFloat     int8 = 8   // float64
	VTString    int8 = 9   // string (int32-length-prefix)(utf-8 bytes)
	VTTimestamp int8 = 11  // int64 timestamp microseconds
	VTTable     int8 = 21  // VoltTable
	VTDecimal   int8 = 22  // fix-scaled, fix-precision decimal
	VTVarBin    int8 = 25  // varbinary (int)(bytes)
)

var order = binary.BigEndian

// protoVersion is the implemented VoltDB wireprotocol version.
const protoVersion = 1

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

func readBoolean(r io.Reader) (bool, error) {
	val, err := readByte(r)
	if err != nil {
		return false, err
	}
	result := val != 0
	return result, nil
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

func readUint8(r io.Reader) (uint8, error) {
	var b [1]byte
	bs := b[:1]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	return uint8(b[0]), nil
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

func readInt(r io.Reader) (int32, error) {
	var b [4]byte
	bs := b[:4]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}

	return int32(order.Uint32(bs)), nil
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

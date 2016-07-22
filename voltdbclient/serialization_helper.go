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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"
)

// A helper for protocol-level de/serialization code. For
// example, serialize and write a procedure call to the network.

func serializeLoginMessage(user string, passwd string) (msg bytes.Buffer, err error) {
	h := sha256.New()
	io.WriteString(h, passwd)
	shabytes := h.Sum(nil)

	err = writeString(&msg, "database")
	if err != nil {
		return
	}
	err = writeString(&msg, user)
	if err != nil {
		return
	}
	err = writePasswordBytes(&msg, shabytes)
	if err != nil {
		return
	}
	return
}

// configures conn with server's advertisement.
func deserializeLoginResponse(r io.Reader) (connData *connectionData, err error) {
	// Authentication result code	Byte	 1	 Basic
	// Server Host ID	            Integer	 4	 Basic
	// Connection ID	            Long	 8	 Basic
	// Cluster start timestamp  	Long	 8	 Basic
	// Leader IPV4 address	        Integer	 4	 Basic
	// Build string	 String	        variable	 Basic
	ok, err := readByte(r)
	if err != nil {
		return
	}
	if ok != 0 {
		return nil, errors.New("Authentication failed.")
	}

	hostId, err := readInt(r)
	if err != nil {
		return
	}

	connId, err := readLong(r)
	if err != nil {
		return
	}

	_, err = readLong(r)
	if err != nil {
		return
	}

	leaderAddr, err := readInt(r)
	if err != nil {
		return
	}

	buildString, err := readString(r)
	if err != nil {
		return
	}

	connData = new(connectionData)
	connData.hostId = hostId
	connData.connId = connId
	connData.leaderAddr = leaderAddr
	connData.buildString = buildString
	return connData, nil
}

func marshallParam(buf io.Writer, param interface{}) (err error) {
	if param == nil {
		marshallNil(buf)
		return
	} else {
		v := reflect.ValueOf(param)
		t := reflect.TypeOf(param)
		err = marshallValue(buf, v, t)
		return
	}
}

func marshallNil(buf io.Writer) {
	writeByte(buf, VT_NULL)
}

func marshallValue(buf io.Writer, v reflect.Value, t reflect.Type) (err error) {
	if !v.IsValid() {
		return errors.New("Can not encode value.")
	}
	switch v.Kind() {
	case reflect.Bool:
		marshallBool(buf, v)
	case reflect.Int8:
		marshallInt8(buf, v)
	case reflect.Int16:
		marshallInt16(buf, v)
	case reflect.Int32:
		marshallInt32(buf, v)
	case reflect.Int64:
		marshallInt64(buf, v)
	case reflect.Float64:
		marshallFloat64(buf, v)
	case reflect.String:
		marshallString(buf, v)
	case reflect.Slice:
		l := v.Len()
		x := v.Slice(0, l)
		err = marshallSlice(buf, x, t, l)
	case reflect.Struct:
		if t, ok := v.Interface().(time.Time); ok {
			marshallTimestamp(buf, t)
		} else if nv, ok := v.Interface().(nullValue); ok {
			marshallNullValue(buf, nv)
		} else {
			panic("Can't marshal struct-type parameters")
		}
	case reflect.Ptr:
		deref := v.Elem()
		marshallValue(buf, deref, deref.Type())
	default:
		panic(fmt.Sprintf("Can't marshal %v-type parameters", v.Kind()))
	}
	return
}

func marshallBool(buf io.Writer, v reflect.Value) (err error) {
	x := v.Bool()
	writeByte(buf, VT_BOOL)
	err = writeBoolean(buf, x)
	return
}

func marshallInt8(buf io.Writer, v reflect.Value) (err error) {
	x := v.Int()
	writeByte(buf, VT_BOOL)
	err = writeByte(buf, int8(x))
	return
}

func marshallInt16(buf io.Writer, v reflect.Value) (err error) {
	x := v.Int()
	writeByte(buf, VT_SHORT)
	err = writeShort(buf, int16(x))
	return
}

func marshallInt32(buf io.Writer, v reflect.Value) (err error) {
	x := v.Int()
	writeByte(buf, VT_INT)
	err = writeInt(buf, int32(x))
	return
}

func marshallInt64(buf io.Writer, v reflect.Value) (err error) {
	x := v.Int()
	writeByte(buf, VT_LONG)
	err = writeLong(buf, int64(x))
	return
}

func marshallFloat64(buf io.Writer, v reflect.Value) (err error) {
	x := v.Float()
	writeByte(buf, VT_FLOAT)
	err = writeFloat(buf, float64(x))
	return
}

func marshallString(buf io.Writer, v reflect.Value) (err error) {
	x := v.String()
	writeByte(buf, VT_STRING)
	err = writeString(buf, x)
	return
}

func marshallTimestamp(buf io.Writer, t time.Time) (err error) {
	writeByte(buf, VT_TIMESTAMP)
	writeTimestamp(buf, t)
	return
}

func marshallNullValue(buf io.Writer, nv nullValue) (err error) {
	switch nv.getColType() {
	case VT_BOOL:
		writeByte(buf, VT_BOOL)
		writeByte(buf, math.MinInt8)
	case VT_SHORT:
		writeByte(buf, VT_SHORT)
		writeShort(buf, math.MinInt16)
	case VT_INT:
		writeByte(buf, VT_INT)
		writeInt(buf, math.MinInt32)
	case VT_LONG:
		writeByte(buf, VT_LONG)
		writeLong(buf, math.MinInt64)
	case VT_FLOAT:
		writeByte(buf, VT_FLOAT)
		writeFloat(buf, float64(-1.7E+308))
	case VT_STRING:
		writeByte(buf, VT_STRING)
		writeInt(buf, int32(-1))
	case VT_VARBIN:
		writeByte(buf, VT_VARBIN)
		writeInt(buf, int32(-1))
	case VT_TIMESTAMP:
		writeByte(buf, VT_TIMESTAMP)
		buf.Write(null_timestamp[:])
	default:
		panic(fmt.Sprintf("Unexpected null type %d", nv.getColType()))
	}
	return
}

func marshallSlice(buf io.Writer, v reflect.Value, t reflect.Type, l int) (err error) {
	k := t.Elem().Kind()

	// distinguish between byte arrays and all other slices.
	// byte arrays are handled as VARBINARY, all others are handled as ARRAY.
	if k == reflect.Uint8 {
		bs := v.Bytes()
		writeByte(buf, VT_VARBIN)
		err = writeVarbinary(buf, bs)
	} else {
		writeByte(buf, VT_ARRAY)
		writeShort(buf, int16(l))
		for i := 0; i < l; i++ {
			err = marshallValue(buf, v.Index(i), t)
		}
	}
	return
}

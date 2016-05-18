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
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"time"
	"crypto/sha256"
)

// io.go includes protocol-level de/serialization code. For
// example, serialize and write a procedure call to the network.

// writeMessage prepends a header and writes header and buf to tcpConn
// Table represents a VoltDB table, often as a procedure result set.
// Wrap up some metdata with pointer(s) to row data. Tables are
// relatively cheap to copy (the associated user data is copied
// reference).
func (conn *Conn) writeLoginMessage(buf bytes.Buffer) error {
	// length includes protocol version.
	length := buf.Len() + 2
	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(length))
	writeProtoVersion(&netmsg)
	writePasswordHashVersion(&netmsg)
	// 1 copy + 1 n/w write benchmarks faster than 2 n/w writes.
	io.Copy(&netmsg, &buf)
	io.Copy(conn.tcpConn, &netmsg)
	// TODO: obviously wrong
	return nil
}

// writeProcedureCall serializes a procedure call and writes it to the tcp connection.
func (conn *Conn) writeProcedureCall(procedure string, ud int64, params []interface{}) error {

	var call bytes.Buffer
	var err error

	// Serialize the procedure call and its params.
	// Use 0 for handle; it's not necessary in pure sync client.
	if call, err = serializeCall(procedure, 0, params); err != nil {
		return err
	}

	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(call.Len()))
	io.Copy(&netmsg, &call)
	io.Copy(conn.tcpConn, &netmsg)
	// TODO: obviously wrong
	return nil
}

// readMessageHdr reads the standard wireprotocol header.
func (conn *Conn) readMessageHdr() (size int32, err error) {
	// Total message length Integer  4
	size, err = readInt(conn.tcpConn)
	if err != nil {
		return
	}
	return (size), nil
}

// readLoginResponse parses the login response message.
func (conn *Conn) readMessage() (*bytes.Buffer, error) {
	size, err := conn.readMessageHdr()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err = io.ReadFull(conn.tcpConn, data); err != nil {
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

func (conn *Conn) readLoginResponse() (*connectionData, error) {
	buf, err := conn.readMessage()
	if err != nil {
		return nil, err
	}
	connData, err := deserializeLoginResponse(buf)
	return connData, err
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

func serializeCall(proc string, ud int64, params []interface{}) (msg bytes.Buffer, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()

	// batch timeout type
	if err = writeByte(&msg, 0); err != nil {
		return
	}
	if err = writeString(&msg, proc); err != nil {
		return
	}
	if err = writeLong(&msg, ud); err != nil {
		return
	}
	serializedParams, err := serializeParams(params)
	if err != nil {
		return
	}
	io.Copy(&msg, &serializedParams)
	return
}

func serializeParams(params []interface{}) (msg bytes.Buffer, err error) {
	// parameter_count short
	// (type byte, parameter)*
	if err = writeShort(&msg, int16(len(params))); err != nil {
		return
	}
	for _, val := range params {
		if err = marshalParam(&msg, val); err != nil {
			return
		}
	}
	return
}

func marshalParam(buf io.Writer, param interface{}) (err error) {
	v := reflect.ValueOf(param)
	if !v.IsValid() {
		return errors.New("Can not encode value.")
	}
	switch v.Kind() {
	case reflect.Bool:
		x := v.Bool()
		writeByte(buf, vt_BOOL)
		err = writeBoolean(buf, x)
	case reflect.Int8:
		x := v.Int()
		writeByte(buf, vt_BOOL)
		err = writeByte(buf, int8(x))
	case reflect.Int16:
		x := v.Int()
		writeByte(buf, vt_SHORT)
		err = writeShort(buf, int16(x))
	case reflect.Int32:
		x := v.Int()
		writeByte(buf, vt_INT)
		err = writeInt(buf, int32(x))
	case reflect.Int, reflect.Int64:
		x := v.Int()
		writeByte(buf, vt_LONG)
		err = writeLong(buf, int64(x))
	case reflect.Float64:
		x := v.Float()
		writeByte(buf, vt_FLOAT)
		err = writeFloat(buf, float64(x))
	case reflect.String:
		x := v.String()
		writeByte(buf, vt_STRING)
		err = writeString(buf, x)
	case reflect.Struct:
		if t, ok := v.Interface().(time.Time); ok {
			writeByte(buf, vt_TIMESTAMP)
			writeTimestamp(buf, t)
		} else {
			panic("Can't marshal struct-type parameters")
		}
	default:
		panic(fmt.Sprintf("Can't marshal %v-type parameters", v.Kind()))
	}
	return
}

// readCallResponse reads a stored procedure invocation response.
func deserializeCallResponse(r io.Reader) (response *Response, err error) {
	response = new(Response)
	if response.clientData, err = readLong(r); err != nil {
		return nil, err
	}

	fields, err := readByte(r)
	if err != nil {
		return nil, err
	} else {
		response.fieldsPresent = uint8(fields)
	}

	if response.status, err = readByte(r); err != nil {
		return nil, err
	}
	if response.fieldsPresent&(1<<5) != 0 {
		if response.statusString, err = readString(r); err != nil {
			return nil, err
		}
	}
	if response.appStatus, err = readByte(r); err != nil {
		return nil, err
	}
	if response.fieldsPresent&(1<<7) != 0 {
		if response.appStatusString, err = readString(r); err != nil {
			return nil, err
		}
	}
	if response.clusterLatency, err = readInt(r); err != nil {
		return nil, err
	}
	if response.fieldsPresent&(1<<6) != 0 {
		if response.exceptionLength, err = readInt(r); err != nil {
			return nil, err
		}
		if response.exceptionLength > 0 {
			// TODO: implement exception deserialization.
			ignored := make([]byte, response.exceptionLength)
			if _, err = io.ReadFull(r, ignored); err != nil {
				return nil, err
			}
		}
	}
	if response.resultCount, err = readShort(r); err != nil {
		return nil, err
	}

	response.tables = make([]Table, response.resultCount)
	for idx, _ := range response.tables {
		if response.tables[idx], err = deserializeTable(r); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func deserializeTable(r io.Reader) (t Table, err error) {
	var errTable Table

	ttlLength, err := readInt(r) // ttlLength
	if err != nil {
		return errTable, err
	}
	metaLength, err := readInt(r) // metaLength
	if err != nil {
		return errTable, err
	}

	t.statusCode, err = readByte(r)
	if err != nil {
		return errTable, err
	}

	t.columnCount, err = readShort(r)
	if err != nil {
		return errTable, err
	}

	// column type "array" and column name "array" are not
	// length prefixed arrays. they are really just columnCount
	// len sequences of bytes (types) and strings (names).
	var i int16
	for i = 0; i < t.columnCount; i++ {
		ct, err := readByte(r)
		if err != nil {
			return errTable, err
		}
		t.columnTypes = append(t.columnTypes, ct)
	}

	for i = 0; i < t.columnCount; i++ {
		cn, err := readString(r)
		if err != nil {
			return errTable, err
		}
		t.columnNames = append(t.columnNames, cn)
	}

	t.rowCount, err = readInt(r)
	if err != nil {
		return errTable, err
	}

	// the total row data byte count is:
	//    ttlLength
	//  - 4 byte metaLength field
	//  - metaLength
	//  - 4 byte row count field
	var tableByteCount int64 = int64(ttlLength - metaLength - 8)

	// OPTIMIZE? Could avoid a possibly large copy here by
	// initializing buf to r[Pos():tableByteCount]. Unsure
	// if that way lies madness or cleverness. For now, suck
	// up the copy. Maybe in the future change this method
	// to take a buffer instead of a reader?
	io.CopyN(&t.rows, r, tableByteCount)
	return t, nil
}

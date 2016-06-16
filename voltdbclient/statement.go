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
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
)

var handle int64 = 0

type VoltStatement struct {
	closed   bool
	numInput int
	stmt     string

	conn        *VoltConn // the connection thot owns this statement.
	netListener *NetworkListener
	writer      *io.Writer
}

func newVoltStatement(conn *VoltConn, writer *io.Writer, netListener *NetworkListener, stmt string) *VoltStatement {
	var vs = new(VoltStatement)
	vs.conn = conn
	vs.writer = writer
	vs.netListener = netListener

	vs.stmt = stmt
	vs.closed = false
	return vs
}

func (vs VoltStatement) Close() error {
	if vs.closed {
		return errors.New("Statement is already closed")
	}
	vs.closed = true
	return nil
}

func (vs VoltStatement) NumInput() int {
	return 1
}

func (vs VoltStatement) Exec(args []driver.Value) (driver.Result, error) {
	if vs.closed {
		return nil, errors.New("Can't Exec, statement is closed")
	}
	return NewVoltResult(), nil
}

func (vs VoltStatement) Query(args []driver.Value) (driver.Rows, error) {
	if vs.closed {
		return nil, errors.New("Can't invoke Query, statement is closed")
	}
	stHandle := atomic.AddInt64(&handle, 1)
	c := vs.netListener.registerQuery(handle)
	if err := vs.serializeStatement(vs.writer, vs.stmt, stHandle, args); err != nil {
		vs.netListener.removeQuery(handle)
		return VoltRows{}, err
	}
	return <-c, nil
}

func (vs VoltStatement) QueryAsync(args []driver.Value) error {
	if vs.closed {
		return errors.New("Can't invoke QueryAsync, statement is closed")
	}
	stHandle := atomic.AddInt64(&handle, 1)
	c := vs.netListener.registerQuery(handle)
	vs.conn.registerQuery(stHandle, c)
	if err := vs.serializeStatement(vs.writer, vs.stmt, stHandle, args); err != nil {
		vs.netListener.removeQuery(handle)
		return err
	}
	return nil
}

// I have two methods with this name, one top scope and one here.
// I want to make this one top scope so that the async methods can call it.
func (vs VoltStatement) serializeStatement(writer *io.Writer, procedure string, handle int64, args []driver.Value) error {

	var call bytes.Buffer
	var err error

	// Serialize the procedure call and its params.
	// Use 0 for handle; it's not necessary in pure sync client.
	if call, err = serializeStatement(procedure, handle, args); err != nil {
		return err
	}

	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(call.Len()))
	io.Copy(&netmsg, &call)
	io.Copy(*writer, &netmsg)
	return nil
}

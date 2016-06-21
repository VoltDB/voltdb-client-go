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
	"database/sql/driver"
	"errors"
)

type VoltStatement struct {
	closed bool
	query  string
	conn   *VoltConn // the connection thot owns this statement.
}

func newVoltStatement(conn *VoltConn, query string) *VoltStatement {
	var vs = new(VoltStatement)
	vs.conn = conn
	vs.query = query
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
	return -1
}

func (vs VoltStatement) Exec(args []driver.Value) (driver.Result, error) {
	if vs.closed {
		return nil, errors.New("Can't invoke Exec, statement is closed")
	}
	return vs.conn.Exec(vs.query, args)
}

func (vs VoltStatement) ExecAsync(args []driver.Value) (*VoltAsyncResponse, error) {
	if vs.closed {
		return nil, errors.New("Can't invoke ExecAsync, statement is closed")
	}
	return vs.conn.ExecAsync(vs.query, args)
}

func (vs VoltStatement) Query(args []driver.Value) (driver.Rows, error) {
	if vs.closed {
		return nil, errors.New("Can't invoke Query, statement is closed")
	}
	return vs.conn.Query(vs.query, args)
}

func (vs VoltStatement) QueryAsync(args []driver.Value) (*VoltAsyncResponse, error) {
	if vs.closed {
		return nil, errors.New("Can't invoke QueryAsync, statement is closed")
	}
	return vs.conn.QueryAsync(vs.query, args)
}

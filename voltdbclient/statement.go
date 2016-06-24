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
	"regexp"
)

var (
	inputFinder, _ = regexp.Compile("[?]")
)

type VoltStatement struct {
	closed   bool
	query    string
	numInput int
	conn     *VoltConn // the connection thot owns this statement.
}

func newVoltStatement(conn *VoltConn, query string) *VoltStatement {
	var vs = new(VoltStatement)
	vs.conn = conn
	vs.query = query
	idx := inputFinder.FindAllStringIndex(query, -1)
	vs.numInput = len(idx)
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
	return vs.numInput
}

func (vs VoltStatement) Exec(args []driver.Value) (driver.Result, error) {
	if vs.closed {
		return nil, errors.New("Can't invoke Exec, statement is closed")
	}
	args = append(args, "")
	copy(args[1:], args[0:])
	args[0] = vs.query
	return vs.conn.Exec("@AdHoc", args)
}

func (vs VoltStatement) ExecAsync(resCons resultConsumer, args []driver.Value) (*VoltAsyncResponse, error) {
	if vs.closed {
		return nil, errors.New("Can't invoke ExecAsync, statement is closed")
	}
	args = append(args, "")
	copy(args[1:], args[0:])
	args[0] = vs.query
	return vs.conn.ExecAsync(resCons, "@AdHoc", args)
}

func (vs VoltStatement) Query(args []driver.Value) (driver.Rows, error) {
	if vs.closed {
		return nil, errors.New("Can't invoke Query, statement is closed")
	}
	args = append(args, "")
	copy(args[1:], args[0:])
	args[0] = vs.query
	return vs.conn.Query("@AdHoc", args)
}

func (vs VoltStatement) QueryAsync(rowsCons rowsConsumer, args []driver.Value) (*VoltAsyncResponse, error) {
	if vs.closed {
		return nil, errors.New("Can't invoke QueryAsync, statement is closed")
	}
	args = append(args, "")
	copy(args[1:], args[0:])
	args[0] = vs.query
	return vs.conn.QueryAsync(rowsCons, "@AdHoc", args)
}

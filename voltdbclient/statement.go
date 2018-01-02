/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
	"regexp"
	"time"
)

var (
	inputFinder, _ = regexp.Compile("[?]")
)

// VoltStatement is an implementation of the database/sql/driver.Stmt interface
type VoltStatement struct {
	query    string
	numInput int
	d        *Conn
}

func newVoltStatement(d *Conn, query string) *VoltStatement {
	return &VoltStatement{
		query:    query,
		numInput: len(inputFinder.FindAllStringIndex(query, -1)),
		d:        d,
	}
}

// Close closes the statement.  Close is a noop for VoltDB as the VoltDB server
// does not directly support prepared statements.
func (vs VoltStatement) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
func (vs VoltStatement) NumInput() int {
	return vs.numInput
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.  Uses DefaultQueryTimeout.
func (vs VoltStatement) Exec(args []driver.Value) (driver.Result, error) {
	return vs.ExecTimeout(args, DefaultQueryTimeout)
}

// ExecTimeout executes a query that doesn't return rows, such as an INSERT or
// UPDATE.  Specifies a duration for timeout.
func (vs VoltStatement) ExecTimeout(args []driver.Value, timeout time.Duration) (driver.Result, error) {
	args = append([]driver.Value{vs.query}, args...)
	return vs.d.ExecTimeout("@AdHoc", args, timeout)
}

// ExecAsync asynchronously runs an Exec.  Uses DefaultQueryTimeout.
func (vs VoltStatement) ExecAsync(resCons AsyncResponseConsumer, args []driver.Value) {
	vs.ExecAsyncTimeout(resCons, args, DefaultQueryTimeout)
}

// ExecAsyncTimeout asynchronously runs an Exec. Specifies a duration for
// timeout.
func (vs VoltStatement) ExecAsyncTimeout(resCons AsyncResponseConsumer, args []driver.Value, timeout time.Duration) {
	args = append([]driver.Value{vs.query}, args...)
	vs.d.ExecAsyncTimeout(resCons, "@AdHoc", args, timeout)
}

// Query executes a query that may return rows, such as a SELECT. Uses
// DefaultQueryTimeout.
func (vs VoltStatement) Query(args []driver.Value) (driver.Rows, error) {
	return vs.QueryTimeout(args, DefaultQueryTimeout)
}

// QueryTimeout executes a query that may return rows, such as a SELECT.
// Specifies a duration for timeout.
func (vs VoltStatement) QueryTimeout(args []driver.Value, timeout time.Duration) (driver.Rows, error) {
	args = append([]driver.Value{vs.query}, args...)
	return vs.d.QueryTimeout("@AdHoc", args, timeout)
}

// QueryAsync asynchronously runs a Query.  Uses DefaultQueryTimeout.
func (vs VoltStatement) QueryAsync(rowsCons AsyncResponseConsumer, args []driver.Value) {
	vs.QueryAsyncTimeout(rowsCons, args, DefaultQueryTimeout)
}

// QueryAsyncTimeout asynchronously runs a Query. Specifies a duration for
// timeout.
func (vs VoltStatement) QueryAsyncTimeout(rowsCons AsyncResponseConsumer, args []driver.Value, timeout time.Duration) {
	args = append([]driver.Value{vs.query}, args...)
	vs.d.QueryAsyncTimeout(rowsCons, "@AdHoc", args, timeout)
}

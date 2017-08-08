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
	"database/sql/driver"
	"errors"
	"time"
)

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// Exec is available on both VoltConn and on VoltStatement.
// Uses DefaultQueryTimeout.
func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecTimeout(query, args, DefaultQueryTimeout)
}

// ExecTimeout executes a query that doesn't return rows, such as an INSERT or
// UPDATE. ExecTimeout is available on both VoltConn and on VoltStatement.
// Specifies a duration for timeout.
func (c *Conn) ExecTimeout(query string, args []driver.Value, timeout time.Duration) (driver.Result, error) {
	responseCh := make(chan voltResponse, 1)
	pi := newSyncProcedureInvocation(c.getNextHandle(), false, query, args, responseCh, timeout)
	c.inPiCh <- pi
	tm := time.NewTimer(pi.timeout)
	defer tm.Stop()
	select {
	case resp := <-pi.responseCh:
		switch resp.(type) {
		case VoltResult:
			return resp.(VoltResult), nil
		case VoltError:
			return nil, resp.(VoltError)
		default:
			panic("unexpected response type")
		}
	case <-tm.C:
		return nil, VoltError{voltResponse: voltResponseInfo{status: ConnectionTimeout, clusterRoundTripTime: -1}, error: errors.New("timeout")}
	}
}

// ExecAsync is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.  Uses DefaultQueryTimeout.
func (c *Conn) ExecAsync(resCons AsyncResponseConsumer, query string, args []driver.Value) {
	c.ExecAsyncTimeout(resCons, query, args, DefaultQueryTimeout)
}

// ExecAsyncTimeout is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.  Specifies a duration for timeout.
func (c *Conn) ExecAsyncTimeout(resCons AsyncResponseConsumer, query string, args []driver.Value, timeout time.Duration) {
	pi := newAsyncProcedureInvocation(c.getNextHandle(), false, query, args, timeout, resCons)
	c.inPiCh <- pi
}

// Prepare creates a prepared statement for later queries or executions.
// The Statement returned by Prepare is bound to this VoltConn.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	stmt := newVoltStatement(c, query)
	return *stmt, nil
}

// Query executes a query that returns rows, typically a SELECT. The args are
// for any placeholder parameters in the query.
// Uses DefaultQueryTimeout.
func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryTimeout(query, args, DefaultQueryTimeout)
}

// QueryTimeout executes a query that returns rows, typically a SELECT. The args
// are for any placeholder parameters in the query.
// Specifies a duration for timeout.
func (c *Conn) QueryTimeout(query string, args []driver.Value, timeout time.Duration) (driver.Rows, error) {
	responseCh := make(chan voltResponse, 1)
	pi := newSyncProcedureInvocation(c.getNextHandle(), true, query, args, responseCh, timeout)
	c.inPiCh <- pi
	tm := time.NewTimer(pi.timeout)
	defer tm.Stop()
	select {
	case resp := <-pi.responseCh:
		switch resp.(type) {
		case VoltRows:
			return resp.(VoltRows), nil
		case VoltError:
			return nil, resp.(VoltError)
		default:
			panic("unexpected response type")
		}
	case <-tm.C:
		return nil, VoltError{voltResponse: voltResponseInfo{status: ConnectionTimeout, clusterRoundTripTime: -1}, error: errors.New("timeout")}
	}
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.  Uses DefaultQueryTimeout.
func (c *Conn) QueryAsync(rowsCons AsyncResponseConsumer, query string, args []driver.Value) {
	c.QueryAsyncTimeout(rowsCons, query, args, DefaultQueryTimeout)
}

// QueryAsyncTimeout executes a query asynchronously.  The invoking thread will
// block until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.  Specifies a duration for timeout.
func (c *Conn) QueryAsyncTimeout(rowsCons AsyncResponseConsumer, query string, args []driver.Value, timeout time.Duration) {
	pi := newAsyncProcedureInvocation(c.getNextHandle(), true, query, args, timeout, rowsCons)
	c.inPiCh <- pi
}

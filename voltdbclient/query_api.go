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
	"errors"
	"fmt"
	"time"
)

var errTimeoutExecutingQuery = errors.New("voltdbclient: Timed out executing query")

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
	// Works similar to QueryTimeout but send exec queries instead.
	responseCh := make(chan voltResponse, 1)
	pi := newSyncProcedureInvocation(c.getNextHandle(), false, query, args, responseCh, timeout)
	_, err := c.submit(pi)
	if err != nil {
		return nil, err
	}
	tm := time.NewTimer(pi.timeout)
	defer tm.Stop()
	sec := time.NewTicker(time.Second)
	defer sec.Stop()
	for {
		if pi.conn.isClosed() {
			return nil, VoltError{voltResponse: voltResponseInfo{status: ConnectionTimeout, clusterRoundTripTime: -1},
				error: fmt.Errorf("%s: writing on a closed node connection",
					pi.conn.connInfo)}
		}
		select {
		case <-sec.C:
			if err := pi.conn.Ping(); err != nil {
				pi.conn.markClosed()
				return nil, VoltError{voltResponse: voltResponseInfo{status: ConnectionTimeout,
					clusterRoundTripTime: -1}, error: err}
			}
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
func (c *Conn) ExecAsyncTimeout(resCons AsyncResponseConsumer, query string, args []driver.Value, timeout time.Duration) error {
	pi := newAsyncProcedureInvocation(c.getNextHandle(), false, query, args, timeout, resCons)
	_, err := c.submit(pi)
	return err
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
	// This is sync sql query to voltdb. In reality there is no sync implementation
	// of querying voltdb in this client.
	//
	// What happens here is, we invoke the procedure invocation and wait until we
	// receive a response on the responseCh channel.
	// The call to c.submit ensures that the procedure invocation request did hit a
	// voltdb host.
	//
	// To account for the case the current connection that is serving the procedure
	// invocation is lost. We send ping request every 1 second.If we detect the
	// connection is down, we mark the connection as closed so that it will no
	// longer be used(NOTE: This is useful in cluster mode).
	responseCh := make(chan voltResponse, 1)
	pi := newSyncProcedureInvocation(c.getNextHandle(), true, query, args, responseCh, timeout)
	_, err := c.submit(pi)
	if err != nil {
		return nil, err
	}
	tm := time.NewTimer(pi.timeout)
	defer tm.Stop()
	sec := time.NewTicker(time.Second)
	defer sec.Stop()
	for {
		if pi.conn.isClosed() {
			return nil, VoltError{voltResponse: voltResponseInfo{status: ConnectionTimeout, clusterRoundTripTime: -1},
				error: fmt.Errorf("%s: writing on a closed node connection",
					pi.conn.connInfo)}
		}
		select {
		case <-sec.C:
			if err := pi.conn.Ping(); err != nil {
				pi.conn.markClosed()
				return nil, VoltError{voltResponse: voltResponseInfo{status: ConnectionTimeout, clusterRoundTripTime: -1},
					error: err}
			}
		case resp := <-pi.responseCh:
			switch e := resp.(type) {
			case VoltRows:
				return &e, nil
			case VoltError:
				return nil, e
			default:
				panic("unexpected response type")
			}
		case <-tm.C:
			return nil, VoltError{voltResponse: voltResponseInfo{status: ConnectionTimeout, clusterRoundTripTime: -1}, error: errTimeoutExecutingQuery}
		}
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
func (c *Conn) QueryAsyncTimeout(rowsCons AsyncResponseConsumer, query string, args []driver.Value, timeout time.Duration) error {
	pi := newAsyncProcedureInvocation(c.getNextHandle(), true, query, args, timeout, rowsCons)
	_, err := c.submit(pi)
	return err
}

package voltdbclient

import (
	"database/sql/driver"
	"time"
	"errors"
)
// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// Exec is available on both VoltConn and on VoltStatement.  Uses DEFAULT_QUERY_TIMEOUT.
func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecTimeout(query, args, DEFAULT_QUERY_TIMEOUT)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// Exec is available on both VoltConn and on VoltStatement.  Specifies a duration for timeout.
func (c *Conn) ExecTimeout(query string, args []driver.Value, timeout time.Duration) (driver.Result, error) {

	responseCh := make(chan voltResponse, 1)
	pi := newSyncProcedureInvocation(c.getNextHandle(), false, query, args, responseCh, timeout)
	c.submit(pi)

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
	case <-time.After(pi.timeout):
		return nil, VoltError{voltResponse: voltResponseInfo{status: CONNECTION_TIMEOUT, clusterRoundTripTime: -1}, error: errors.New("timeout")}
	}
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// ExecAsync is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.  Uses DEFAULT_QUERY_TIMEOUT.
func (c *Conn) ExecAsync(resCons AsyncResponseConsumer, query string, args []driver.Value) error {
	return c.ExecAsyncTimeout(resCons, query, args, DEFAULT_QUERY_TIMEOUT)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// ExecAsync is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.  Specifies a duration for timeout.
func (c *Conn) ExecAsyncTimeout(resCons AsyncResponseConsumer, query string, args []driver.Value, timeout time.Duration) error {
	responseCh := make(chan voltResponse, 1)
	pi := newAsyncProcedureInvocation(c.getNextHandle(), false, query, args, responseCh, timeout, resCons)
	return c.submit(pi)
}

// Prepare creates a prepared statement for later queries or executions.
// The Statement returned by Prepare is bound to this VoltConn.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	stmt := newVoltStatement(c, query)
	return *stmt, nil
}

// Query executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
// Uses DEFAULT_QUERY_TIMEOUT.
func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryTimeout(query, args, DEFAULT_QUERY_TIMEOUT)
}

// Query executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
// Specifies a duration for timeout.
func (c *Conn) QueryTimeout(query string, args []driver.Value, timeout time.Duration) (driver.Rows, error) {
	responseCh := make(chan voltResponse, 1)
	pi := newSyncProcedureInvocation(c.getNextHandle(), true, query, args, responseCh, timeout)
	err := c.submit(pi)
	if err != nil {
		return nil, err
	}
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
	case <-time.After(pi.timeout):
		return nil, VoltError{voltResponse: voltResponseInfo{status: CONNECTION_TIMEOUT, clusterRoundTripTime: -1}, error: errors.New("timeout")}
	}
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.  Uses DEFAULT_QUERY_TIMEOUT.
func (c *Conn) QueryAsync(rowsCons AsyncResponseConsumer, query string, args []driver.Value) error {
	return c.QueryAsyncTimeout(rowsCons, query, args, DEFAULT_QUERY_TIMEOUT)
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.  Specifies a duration for timeout.
func (c *Conn) QueryAsyncTimeout(rowsCons AsyncResponseConsumer, query string, args []driver.Value, timeout time.Duration) error {
	responseCh := make(chan voltResponse, 1)
	pi := newAsyncProcedureInvocation(c.getNextHandle(), true, query, args, responseCh, timeout, rowsCons)
	return c.submit(pi)
}

func (c *Conn) submit(pi *procedureInvocation) error {
	nc, backpressure, err := c.getConnByCA(pi)
	if err != nil {
		return err
	}
	if !backpressure && nc != nil {
		nc.submit(pi)
	} else {
		c.piCh <- pi
	}
	return nil
}

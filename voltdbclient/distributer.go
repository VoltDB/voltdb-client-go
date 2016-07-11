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
	"fmt"
	"log"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// the set of currently active connections
type distributer struct {
	handle   int64
	hanMutex sync.Mutex
	ncs      []*nodeConn
	// next conn to look at when finding by round robin.
	ncIndex int
	ncLen   int
	ncMutex sync.Mutex
	open    atomic.Value
	h       *hashinater
}

func newDistributer() *distributer {
	var d = new(distributer)
	d.handle = 0
	d.ncIndex = 0
	d.open = atomic.Value{}
	d.open.Store(true)
	d.h = newHashinater()
	return d
}

func (d *distributer) setConns(ncs []*nodeConn) {
	d.ncs = ncs
	d.ncLen = len(ncs)
}

// Begin starts a transaction.  VoltDB runs in auto commit mode, and so Begin
// returns an error.
func (d *distributer) Begin() (driver.Tx, error) {
	d.assertOpen()
	return nil, errors.New("VoltDB does not support transactions, VoltDB autocommits")
}

// Close closes the connection to the VoltDB server.  Connections to the server
// are meant to be long lived; it should not be necessary to continually close
// and reopen connections.  Close would typically be called using a defer.
// Operations using a closed connection cause a panic.
func (d *distributer) Close() (err error) {
	if d.isClosed() {
		return
	}
	d.setClosed()

	// once this is closed there shouldn't be any additional activity against
	// the connections.  They're touched here without getting a lock.
	for _, nc := range d.ncs {
		err := nc.close()
		if err != nil {
			log.Printf("Failed to close connection with %v\n", err)
		}
	}
	d.ncs = nil
	return nil
}

// Drain blocks until all outstanding asynchronous requests have been satisfied.
// Asynchronous requests are processed in a background thread; this call blocks the
// current thread until that background thread has finished with all asynchronous requests.
func (d *distributer) Drain() {
	d.assertOpen()

	wg := sync.WaitGroup{}
	wg.Add(len(d.ncs))

	// drain can't work if the set of connections can change
	// while it's running.  Hold the lock for the duration, some
	// asyncs may time out.  The user thread is blocked on drain.
	for _, nc := range d.ncs {
		d.drainNode(nc, &wg)
	}
	wg.Wait()
}

func (d *distributer) drainNode(nc *nodeConn, wg *sync.WaitGroup) {
	go func(nc *nodeConn, wg *sync.WaitGroup) {
		nc.drain()
		wg.Done()
	}(nc, wg)
}

func (d *distributer) assertOpen() {
	if !(d.open.Load().(bool)) {
		panic("Tried to use closed connection pool")
	}
}

func (d *distributer) isClosed() bool {
	return !(d.open.Load().(bool))
}

func (d *distributer) setClosed() {
	d.open.Store(false)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// Exec is available on both VoltConn and on VoltStatement.  Uses DEFAULT_QUERY_TIMEOUT.
func (d *distributer) Exec(query string, args []driver.Value) (driver.Result, error) {
	return d.ExecTimeout(query, args, DEFAULT_QUERY_TIMEOUT)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// Exec is available on both VoltConn and on VoltStatement.  Specifies a duration for timeout.
func (d *distributer) ExecTimeout(query string, args []driver.Value, timeout time.Duration) (driver.Result, error) {
	pi := newProcedureInvocation(d.getNextHandle(), false, query, args, timeout)
	nc, err := d.getConn(pi)
	if err != nil {
		return nil, err
	}
	return nc.exec(pi)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// ExecAsync is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.  Uses DEFAULT_QUERY_TIMEOUT.
func (d *distributer) ExecAsync(resCons AsyncResponseConsumer, query string, args []driver.Value) error {
	return d.ExecAsyncTimeout(resCons, query, args, DEFAULT_QUERY_TIMEOUT)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// ExecAsync is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.  Specifies a duration for timeout.
func (d *distributer) ExecAsyncTimeout(resCons AsyncResponseConsumer, query string, args []driver.Value, timeout time.Duration) error {
	pi := newProcedureInvocation(d.getNextHandle(), false, query, args, timeout)
	nc, err := d.getConn(pi)
	if err != nil {
		return err
	}
	return nc.execAsync(resCons, pi)
}

// Prepare creates a prepared statement for later queries or executions.
// The Statement returned by Prepare is bound to this VoltConn.
func (d *distributer) Prepare(query string) (driver.Stmt, error) {
	stmt := newVoltStatement(d, query)
	return *stmt, nil
}

// Query executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
// Uses DEFAULT_QUERY_TIMEOUT.
func (d *distributer) Query(query string, args []driver.Value) (driver.Rows, error) {
	return d.QueryTimeout(query, args, DEFAULT_QUERY_TIMEOUT)
}

// Query executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
// Specifies a duration for timeout.
func (d *distributer) QueryTimeout(query string, args []driver.Value, timeout time.Duration) (driver.Rows, error) {
	pi := newProcedureInvocation(d.getNextHandle(), true, query, args, timeout)
	nc, err := d.getConn(pi)
	if err != nil {
		return nil, err
	}
	return nc.query(pi)
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.  Uses DEFAULT_QUERY_TIMEOUT.
func (d *distributer) QueryAsync(rowsCons AsyncResponseConsumer, query string, args []driver.Value) error {
	return d.QueryAsyncTimeout(rowsCons, query, args, DEFAULT_QUERY_TIMEOUT)
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.  Specifies a duration for timeout.
func (d *distributer) QueryAsyncTimeout(rowsCons AsyncResponseConsumer, query string, args []driver.Value, timeout time.Duration) error {
	pi := newProcedureInvocation(d.getNextHandle(), true, query, args, timeout)
	nc, err := d.getConn(pi)
	if err != nil {
		return err
	}
	if nc == nil {
		return errors.New("no valid connection found")
	}
	return nc.queryAsync(rowsCons, pi)
}

// Get a connection from the hashinator.  If not, get one by round robin.  If not return nil.
func (d *distributer) getConn(pi *procedureInvocation) (*nodeConn, error) {

	d.assertOpen()
	nc := d.h.getConn(pi)
	if nc != nil {
		return nc, nil
	} else {
		return d.getConnByRR(pi.timeout)
	}
}

func (d *distributer) getConnByRR(timeout time.Duration) (*nodeConn, error) {
	start := time.Now()
	d.ncMutex.Lock()
	defer d.ncMutex.Unlock()
	for i := 0; i < d.ncLen; i++ {
		d.ncIndex++
		d.ncIndex = d.ncIndex % d.ncLen
		nc := d.ncs[d.ncIndex]
		if nc.isOpen() && !nc.hasBP() {
			if time.Now().Sub(start) > timeout {
				return nil, errors.New("timeout")
			} else {
				return nc, nil
			}
		}
	}
	// if went through the loop without finding an open connection
	// without backpressure then return nil.
	return nil, nil
}

func (d *distributer) getNextHandle() int64 {
	d.hanMutex.Lock()
	defer d.hanMutex.Unlock()
	d.handle++
	if d.handle == math.MaxInt64 {
		d.handle = 0
	}
	return d.handle
}

type procedureInvocation struct {
	handle  int64
	isQuery bool // as opposed to an exec.
	query   string
	params  []driver.Value
	timeout time.Duration
	slen    int // length of pi once serialized
}

func newProcedureInvocation(handle int64, isQuery bool, query string, params []driver.Value, timeout time.Duration) *procedureInvocation {
	var pi = new(procedureInvocation)
	pi.handle = handle
	pi.isQuery = isQuery
	pi.query = query
	pi.params = params
	pi.timeout = timeout
	pi.slen = -1
	return pi
}

func (pi *procedureInvocation) getLen() int {
	if pi.slen == -1 {
		pi.slen = pi.calcLen()
	}
	return pi.slen
}

func (pi *procedureInvocation) calcLen() int {
	// fixed - 1 for batch timeout type, 4 for str length (proc name), 8 for handle, 2 for paramCount
	var slen int = 15
	slen += len(pi.query)
	for _, param := range pi.params {
		slen += pi.calcParamLen(param)
	}
	return slen
}

func (pi *procedureInvocation) calcParamLen(param interface{}) int {
	// add one to each because the type itself takes one byte
	v := reflect.ValueOf(param)
	switch v.Kind() {
	case reflect.Bool:
		return 2
	case reflect.Int8:
		return 2
	case reflect.Int16:
		return 3
	case reflect.Int32:
		return 5
	case reflect.Int64:
		return 9
	case reflect.Float64:
		return 9
	case reflect.String:
		return 5 + v.Len()
	case reflect.Slice:
		return 1 + v.Len()
	case reflect.Struct:
		panic("Can't marshal a struct")
	case reflect.Ptr:
		panic("Can't marshal a pointer")
	default:
		panic(fmt.Sprintf("Can't marshal %v-type parameters", v.Kind()))
	}
}

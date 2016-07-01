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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// the set of currently active connections
type activeConns struct {
	ncs      []*nodeConn
	ncsMutex sync.RWMutex
	r        *rand.Rand
	open     atomic.Value
}

func newActiveConns() *activeConns {
	var acs = new(activeConns)
	acs.ncs = make([]*nodeConn, 0)
	acs.ncsMutex = sync.RWMutex{}
	acs.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	acs.open = atomic.Value{}
	acs.open.Store(true)
	return acs
}

// add a conn that is newly connected
func (acs *activeConns) addConn(nc *nodeConn) {
	acs.assertOpen()
	acs.ncsMutex.Lock()
	acs.ncs = append(acs.ncs, nc)
	acs.ncsMutex.Unlock()
}

// Begin starts a transaction.  VoltDB runs in auto commit mode, and so Begin
// returns an error.
func (acs *activeConns) Begin() (driver.Tx, error) {
	acs.assertOpen()
	return nil, errors.New("VoltDB does not support transactions, VoltDB autocommits")
}

// Close closes the connection to the VoltDB server.  Connections to the server
// are meant to be long lived; it should not be necessary to continually close
// and reopen connections.  Close would typically be called using a defer.
// Operations using a closed connection cause a panic.
func (acs *activeConns) Close() (err error) {
	if acs.isClosed() {
		return
	}
	acs.setClosed()

	// once this is closed there shouldn't be any additional activity against
	// the connections.  They're touched here without getting a lock.
	for _, nc := range acs.ncs {
		err := nc.close()
		if err != nil {
			log.Println("Failed to close connection with %v", err)
		}
	}
	acs.ncs = nil
	return nil
}

func (acs *activeConns) removeConn(ci string) {
	acs.assertOpen()
	acs.ncsMutex.Lock()
	for i, ac := range acs.ncs {
		if ac.cs.connInfo == ci {
			acs.ncs = append(acs.ncs[:i], acs.ncs[i+1:]...)
			break
		}
	}
	acs.ncsMutex.Unlock()
}

// Drain blocks until all outstanding asynchronous requests have been satisfied.
// Asynchronous requests are processed in a background thread; this call blocks the
// current thread until that background thread has finished with all asynchronous requests.
func (acs *activeConns) Drain() {
	acs.assertOpen()

	// drain can't work if the set of connections can change
	// while it's running.  Hold the lock for the duration, some
	// asyncs may time out.  The user thread is blocked on drain.
	acs.ncsMutex.Lock()
	for _, nc := range acs.ncs {
		nc.drain()
	}
	acs.ncsMutex.Unlock()
}

func (acs *activeConns) assertOpen() {
	if !(acs.open.Load().(bool)) {
		panic("Tried to use closed connection pool")
	}
}

func (acs *activeConns) isClosed() bool {
	return !(acs.open.Load().(bool))
}

func (acs *activeConns) setClosed() {
	acs.open.Store(false)
}

func (acs *activeConns) numConns() int {
	acs.assertOpen()
	acs.ncsMutex.RLock()
	num := len(acs.ncs)
	acs.ncsMutex.RUnlock()
	return num
}

func (acs *activeConns) getConn() *nodeConn {
	acs.assertOpen()
	acs.ncsMutex.RLock()
	i := acs.r.Intn(len(acs.ncs))
	nc := acs.ncs[i]
	acs.ncsMutex.RUnlock()
	return nc
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// Exec is available on both VoltConn and on VoltStatement.
func (acs *activeConns) Exec(query string, args []driver.Value) (driver.Result, error) {
	ac := acs.getConn()
	return ac.exec(query, args)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// ExecAsync is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.
func (acs *activeConns) ExecAsync(resCons AsyncResponseConsumer, query string, args []driver.Value) error {
	ac := acs.getConn()
	return ac.execAsync(resCons, query, args)
}

// Prepare creates a prepared statement for later queries or executions.
// The Statement returned by Prepare is bound to this VoltConn.
func (acs *activeConns) Prepare(query string) (driver.Stmt, error) {
	ac := acs.getConn()
	stmt := newVoltStatement(ac, query)
	return *stmt, nil
}

// Query executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
func (acs *activeConns) Query(query string, args []driver.Value) (driver.Rows, error) {
	nc := acs.getConn()
	return nc.query(query, args)
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.
func (acs *activeConns) QueryAsync(rowsCons AsyncResponseConsumer, query string, args []driver.Value) error {
	nc := acs.getConn()
	return nc.queryAsync(rowsCons, query, args)
}

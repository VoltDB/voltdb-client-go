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
	"math/rand"
	"time"
)

// the set of currently active connections
type activeConns struct {
	ncs  []*nodeConn
	r    *rand.Rand
	open *bool
}

func newActiveConns() *activeConns {
	var acs = new(activeConns)
	acs.ncs = make([]*nodeConn, 0)
	acs.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	acs.open = new(bool)
	*(acs.open) = true
	return acs
}

// add a conn that is newly connected
func (acs *activeConns) addConn(nc *nodeConn) {
	acs.ncs = append(acs.ncs, nc)
}

// Begin starts a transaction.  VoltDB runs in auto commit mode, and so Begin
// returns an error.
func (acs *activeConns) Begin() (driver.Tx, error) {
	return nil, errors.New("VoltDB does not support transactions, VoltDB autocommits")
}

// Close closes the connection to the VoltDB server.  Connections to the server
// are meant to be long lived; it should not be necessary to continually close
// and reopen connections.  Close would typically be called using a defer.
func (acs *activeConns) Close() (err error) {
	if !acs.isOpen() {
		return
	}
	for _, nc := range acs.ncs {
		err := nc.close()
		if err != nil {
			fmt.Println("Failed to close connection with %v", err)
		}
	}
	return nil
}

// Drain blocks until all outstanding asynchronous requests have been satisfied.
// Asynchronous requests are processed in a background thread; this call blocks the
// current thread until that background thread has finished with all asynchronous requests.
func (acs *activeConns) Drain() {
	for _, nc := range acs.ncs {
		nc.drain()
	}
}

func (acs *activeConns) isOpen() bool {
	return *(acs.open)
}

func (acs *activeConns) numConns() int {
	return len(acs.ncs)
}

// remove a conn that we lost connection to
func (acs *activeConns) removeConn() {

}

func (acs *activeConns) getAC() *nodeConn {
	i := acs.r.Intn(len(acs.ncs))
	return acs.ncs[i]
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// Exec is available on both VoltConn and on VoltStatement.
func (acs *activeConns) Exec(query string, args []driver.Value) (driver.Result, error) {
	ac := acs.getAC()
	return ac.exec(query, args)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// ExecAsync is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.
func (acs *activeConns) ExecAsync(resCons AsyncResponseConsumer, query string, args []driver.Value) error {
	ac := acs.getAC()
	return ac.execAsync(resCons, query, args)
}

// Prepare creates a prepared statement for later queries or executions.
// The Statement returned by Prepare is bound to this VoltConn.
func (acs *activeConns) Prepare(query string) (driver.Stmt, error) {
	ac := acs.getAC()
	stmt := newVoltStatement(ac, query)
	return *stmt, nil
}

// Query executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
func (acs *activeConns) Query(query string, args []driver.Value) (driver.Rows, error) {
	nc := acs.getAC()
	return nc.query(query, args)
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.
func (acs *activeConns) QueryAsync(rowsCons AsyncResponseConsumer, query string, args []driver.Value) error {
	nc := acs.getAC()
	return nc.queryAsync(rowsCons, query, args)
}

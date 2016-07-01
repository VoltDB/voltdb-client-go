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
type distributer struct {
	acs      []*nodeConn  // the set of active connections
	acsMutex sync.RWMutex
	r        *rand.Rand
	open     atomic.Value
}

func newDistributer() *distributer {
	var d = new(distributer)
	d.acs = make([]*nodeConn, 0)
	d.acsMutex = sync.RWMutex{}
	d.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	d.open = atomic.Value{}
	d.open.Store(true)
	return d
}

// add a conn that is newly connected
func (d *distributer) addConn(ac *nodeConn) {
	d.assertOpen()
	d.acsMutex.Lock()
	d.acs = append(d.acs, ac)
	d.acsMutex.Unlock()
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
	for _, ac := range d.acs {
		err := ac.close()
		if err != nil {
			log.Println("Failed to close connection with %v", err)
		}
	}
	d.acs = nil
	return nil
}

func (d *distributer) removeConn(ci string) {
	d.assertOpen()
	d.acsMutex.Lock()
	for i, ac := range d.acs {
		if ac.cs.connInfo == ci {
			d.acs = append(d.acs[:i], d.acs[i+1:]...)
			break
		}
	}
	d.acsMutex.Unlock()
}

// Drain blocks until all outstanding asynchronous requests have been satisfied.
// Asynchronous requests are processed in a background thread; this call blocks the
// current thread until that background thread has finished with all asynchronous requests.
func (d *distributer) Drain() {
	d.assertOpen()

	// drain can't work if the set of connections can change
	// while it's running.  Hold the lock for the duration, some
	// asyncs may time out.  The user thread is blocked on drain.
	d.acsMutex.Lock()
	for _, ac := range d.acs {
		ac.drain()
	}
	d.acsMutex.Unlock()
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

func (d *distributer) numConns() int {
	d.assertOpen()
	d.acsMutex.RLock()
	num := len(d.acs)
	d.acsMutex.RUnlock()
	return num
}

func (d *distributer) getConn() *nodeConn {
	d.assertOpen()
	d.acsMutex.RLock()
	i := d.r.Intn(len(d.acs))
	ac := d.acs[i]
	d.acsMutex.RUnlock()
	return ac
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// Exec is available on both VoltConn and on VoltStatement.
func (d *distributer) Exec(query string, args []driver.Value) (driver.Result, error) {
	ac := d.getConn()
	return ac.exec(query, args)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// ExecAsync is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.
func (d *distributer) ExecAsync(resCons AsyncResponseConsumer, query string, args []driver.Value) error {
	ac := d.getConn()
	return ac.execAsync(resCons, query, args)
}

// Prepare creates a prepared statement for later queries or executions.
// The Statement returned by Prepare is bound to this VoltConn.
func (d *distributer) Prepare(query string) (driver.Stmt, error) {
	ac := d.getConn()
	stmt := newVoltStatement(ac, query)
	return *stmt, nil
}

// Query executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
func (d *distributer) Query(query string, args []driver.Value) (driver.Rows, error) {
	ac := d.getConn()
	return ac.query(query, args)
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.
func (d *distributer) QueryAsync(rowsCons AsyncResponseConsumer, query string, args []driver.Value) error {
	ac := d.getConn()
	return ac.queryAsync(rowsCons, query, args)
}

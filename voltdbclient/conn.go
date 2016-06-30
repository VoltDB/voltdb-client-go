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

// voltdbclient provides a go client for VoltDB.  voltdbclient
// implements the interfaces specified in the database/sql/driver
// package.  The interfaces specified by database/sql/driver are
// typically used to implement a driver to be used by the
// database/sql package.
//
// In this case, though, voltdbclient can also be used as a
// standalone client.  That is, the code in voltdbclient
// can be used both to implement a voltdb driver for database/sql
// and as a standalone VoltDB client.
//
// voltdbclient supports an asynchronous api as well as the
// standard database/sql/driver api.  The asynchronous api is supported
// by the VoltConn and VoltStatement types.
package voltdbclient

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"time"
)

type VoltConn struct {
	ncs  []*nodeConn
	r    *rand.Rand
	open *bool
}

func newVoltConn(ncs []*nodeConn) *VoltConn {
	var vc = new(VoltConn)
	vc.ncs = ncs
	vc.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	vc.open = new(bool)
	*(vc.open) = true
	return vc
}

// OpenConn returns a new connection to the VoltDB server.  The name is a
// string in a driver-specific format.  The returned connection can be used by
// only one goroutine at a time.
func OpenConn(connInfos []string) (*VoltConn, error) {

	ncs := make([]*nodeConn, 0, len(connInfos))
	for _, connInfo := range connInfos {
		nc, err := openNodeConn(connInfo)
		if err == nil {
			ncs = append(ncs, nc)
		} else {
			fmt.Println(err)
		}
	}
	if len(ncs) == 0 {
		return nil, errors.New("Failed to connect to a VoltDB server")
	}
	return newVoltConn(ncs), nil
}

// Begin starts a transaction.  VoltDB runs in auto commit mode, and so Begin
// returns an error.
func (vc VoltConn) Begin() (driver.Tx, error) {
	return nil, errors.New("VoltDB does not support transactions, VoltDB autocommits")
}

// Close closes the connection to the VoltDB server.  Connections to the server
// are meant to be long lived; it should not be necessary to continually close
// and reopen connections.  Close would typically be called using a defer.
func (vc VoltConn) Close() (err error) {
	if !vc.isOpen() {
		return
	}
	for _, nc := range vc.ncs {
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
func (vc VoltConn) Drain() {
	for _, nc := range vc.ncs {
		nc.drain()
	}
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// Exec is available on both VoltConn and on VoltStatement.
func (vc VoltConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	nc := vc.nextNC()
	return nc.exec(query, args)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// ExecAsync is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.
func (vc VoltConn) ExecAsync(resCons AsyncResponseConsumer, query string, args []driver.Value) error {
	nc := vc.nextNC()
	return nc.execAsync(resCons, query, args)
}

// Prepare creates a prepared statement for later queries or executions.
// The Statement returned by Prepare is bound to this VoltConn.
func (vc VoltConn) Prepare(query string) (driver.Stmt, error) {
	stmt := newVoltStatement(&vc, query)
	return *stmt, nil
}

// Query executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
func (vc VoltConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	nc := vc.nextNC()
	return nc.query(query, args)
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.
func (vc VoltConn) QueryAsync(rowsCons AsyncResponseConsumer, query string, args []driver.Value) error {
	nc := vc.nextNC()
	return nc.queryAsync(rowsCons, query, args)
}

func (vc VoltConn) isOpen() bool {
	return *(vc.open)
}

func (vc VoltConn) nextNC() *nodeConn {
	i := vc.r.Intn(len(vc.ncs))
	return vc.ncs[i]
}

func openNodeConn(connInfo string) (*nodeConn, error) {
	// for now, at least, connInfo is host and port.
	raddr, err := net.ResolveTCPAddr("tcp", connInfo)
	if err != nil {
		return nil, fmt.Errorf("Error resolving %v.", connInfo)
	}
	var tcpConn *net.TCPConn
	if tcpConn, err = net.DialTCP("tcp", nil, raddr); err != nil {
		return nil, err
	}
	login, err := serializeLoginMessage("", "")
	if err != nil {
		return nil, err
	}
	writeLoginMessage(tcpConn, &login)
	connData, err := readLoginResponse(tcpConn)
	if err != nil {
		return nil, err
	}
	return newNodeConn(connInfo, tcpConn, tcpConn, *connData), nil
}

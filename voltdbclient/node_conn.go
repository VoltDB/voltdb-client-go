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
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// qHandle is a var
var qHandle int64 = 0 // each query has a unique handle.

// connectionData are the values returned by a successful login.
type connectionData struct {
	hostId      int32
	connId      int64
	leaderAddr  int32
	buildString string
}

type connectionState struct {
	connInfo      string
	reader        io.Reader
	writer        io.Writer
	connData      connectionData
	asyncsChannel chan voltResponse
	asyncs        map[int64]*voltAsyncResponse
	asyncsMutex   sync.Mutex
	nl            *networkListener
	nlwg          *sync.WaitGroup
	isOpen        bool
}

// VoltConn represents a connection to VoltDB that can be used to execute
// queries and other statements.  A VoltConn is initially created with a call
// to OpenConn.
//
// A VoltConn is a driver.Conn; VoltConn also supports an asynchronous api.
//
// The creation of a VoltConn represents the creation of two associated
// goroutines.  One of these is used to listen for responses from the VoltDB
// server.  The other is used to process responses to asynchronous requests.
//
// A VoltConn should not be shared among goroutines; this is true of
// driver.Conn as well.  But a client can create many instances of a VoltConn.
type nodeConn struct {
	cs *connectionState
}

func newNodeConn(connInfo string, reader io.Reader, writer io.Writer, connectionData connectionData) *nodeConn {
	var nc = new(nodeConn)

	asyncsChannel := make(chan voltResponse)
	asyncs := make(map[int64]*voltAsyncResponse)
	asyncsMutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	nl := newListener(nc, reader, &wg)
	cs := connectionState{connInfo, reader, writer, connectionData, asyncsChannel, asyncs, asyncsMutex, nl, &wg, true}
	nc.cs = &cs
	nl.start()
	go nc.processAsyncs()
	return nc
}

func (nc nodeConn) close() (err error) {
	if !nc.isOpen() {
		return
	}

	// stop the network listener
	nc.nl().stop()

	// close the tcp conn, will unblock the listener.
	if nc.reader() != nil {
		tcpConn := nc.reader().(*net.TCPConn)
		err = tcpConn.Close()
	}

	// network thread should return.
	nc.nlwg().Wait()

	nc.cs.isOpen = false
	return err
}

func (nc nodeConn) reconnect() {
	var first bool = true
	for {
		if first {
			first = false
		} else {
			time.Sleep(10 * time.Microsecond)
		}
		raddr, err := net.ResolveTCPAddr("tcp", nc.cs.connInfo)
		if err != nil {
			fmt.Printf("Failed to resolve tcp address of server %s\n", err)
			continue
		}
		tcpConn, err := net.DialTCP("tcp", nil, raddr)
		if err != nil {
			fmt.Printf("Failed to connect to server %s\n", err)
			continue
		}
		login, err := serializeLoginMessage("", "")
		if err != nil {
			fmt.Printf("Failed to serialize login message %s\n", err)
			continue
		}
		writeLoginMessage(tcpConn, &login)
		if err != nil {
			fmt.Printf("Failed to writing login message to server %s\n", err)
			continue
		}
		connectionData, err := readLoginResponse(tcpConn)
		if err != nil {
			fmt.Printf("Did not receive response to login request to server%s\n", err)
			continue
		}

		asyncs := make(map[int64]*voltAsyncResponse)
		wg := sync.WaitGroup{}
		nl := newListener(&nc, tcpConn, &wg)

		nc.cs.reader = tcpConn
		nc.cs.writer = tcpConn
		nc.cs.connData = *connectionData
		nc.cs.asyncsChannel = make(chan voltResponse)
		nc.cs.asyncs = asyncs
		nc.cs.asyncsMutex = sync.Mutex{}
		nc.cs.nl = nl
		nc.cs.nlwg = &wg
		nc.cs.isOpen = true
		nl.start()
		break
	}
}

func (nc nodeConn) exec(query string, args []driver.Value) (driver.Result, error) {
	if !nc.isOpen() {
		return nil, errors.New("Connection is closed")
	}
	handle := atomic.AddInt64(&qHandle, 1)
	c := nc.nl().registerRequest(handle, false)
	if err := nc.serializeQuery(nc.writer(), query, handle, args); err != nil {
		nc.nl().removeRequest(handle)
		return VoltResult{}, err
	}
	resp := <-c
	rslt := resp.(VoltResult)
	if err := rslt.getError(); err != nil {
		return nil, err
	}
	return rslt, nil
}

func (nc nodeConn) execAsync(resCons AsyncResponseConsumer, query string, args []driver.Value) error {
	if !nc.isOpen() {
		return errors.New("Connection is closed")
	}
	handle := atomic.AddInt64(&qHandle, 1)
	c := nc.nl().registerRequest(handle, false)
	vasr := newVoltAsyncResponse(nc, handle, c, false, resCons)
	nc.registerAsync(handle, vasr)
	if err := nc.serializeQuery(nc.writer(), query, handle, args); err != nil {
		nc.nl().removeRequest(handle)
		return err
	}
	return nil
}

func (nc nodeConn) query(query string, args []driver.Value) (driver.Rows, error) {
	if !nc.isOpen() {
		return nil, errors.New("Connection is closed")
	}
	handle := atomic.AddInt64(&qHandle, 1)
	c := nc.nl().registerRequest(handle, true)
	if err := nc.serializeQuery(nc.writer(), query, handle, args); err != nil {
		nc.nl().removeRequest(handle)
		return VoltRows{}, err
	}
	resp := <-c
	rows := resp.(VoltRows)
	if err := rows.getError(); err != nil {
		return nil, err
	}
	return rows, nil
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.
func (nc nodeConn) queryAsync(rowsCons AsyncResponseConsumer, query string, args []driver.Value) error {
	if !nc.isOpen() {
		return errors.New("Connection is closed")
	}
	handle := atomic.AddInt64(&qHandle, 1)
	c := nc.nl().registerRequest(handle, true)
	vasr := newVoltAsyncResponse(nc, handle, c, true, rowsCons)
	nc.registerAsync(handle, vasr)
	if err := nc.serializeQuery(nc.writer(), query, handle, args); err != nil {
		nc.nl().removeRequest(handle)
		return err
	}
	return nil
}

func (nc nodeConn) drain() {
	var numAsyncs int
	for {
		nc.asyncsMutex().Lock()
		numAsyncs = len(nc.asyncs())
		nc.asyncsMutex().Unlock()
		if numAsyncs == 0 {
			break
		}
		runtime.Gosched()
	}
}

func (nc nodeConn) processAsyncs() {
	for {
		resp := <-nc.cs.asyncsChannel
		handle := resp.getHandle()
		nc.asyncsMutex().Lock()
		async := nc.asyncs()[handle]
		nc.asyncsMutex().Unlock()

		if async.isQuery() {
			vrows := resp.(VoltRows)
			if err := vrows.getError(); err != nil {
				async.getArc().ConsumeError(err)
			} else {
				async.getArc().ConsumeRows(vrows)

			}
			nc.removeAsync(handle)
			continue
		} else {
			vrslt := resp.(VoltResult)
			if err := vrslt.getError(); err != nil {
				async.getArc().ConsumeError(err)
			} else {
				async.getArc().ConsumeResult(vrslt)
			}
			nc.removeAsync(handle)
			continue
		}
	}
}

func (nc nodeConn) asyncs() map[int64]*voltAsyncResponse {
	return nc.cs.asyncs
}

func (nc nodeConn) asyncsMutex() *sync.Mutex {
	return &nc.cs.asyncsMutex
}

func (nc nodeConn) isOpen() bool {
	return nc.cs.isOpen
}

func (nc nodeConn) nl() *networkListener {
	return nc.cs.nl
}

func (nc nodeConn) nlwg() *sync.WaitGroup {
	return nc.cs.nlwg
}

func (nc nodeConn) reader() io.Reader {
	return nc.cs.reader
}

func (nc nodeConn) writer() io.Writer {
	return nc.cs.writer
}

func (nc nodeConn) registerAsync(handle int64, vasr *voltAsyncResponse) {
	nc.asyncsMutex().Lock()
	nc.asyncs()[handle] = vasr
	nc.asyncsMutex().Unlock()
	go func() {
		nc.cs.asyncsChannel <- <-vasr.channel()
	}()
}

func (nc nodeConn) removeAsync(han int64) {
	nc.asyncsMutex().Lock()
	delete(nc.asyncs(), han)
	nc.asyncsMutex().Unlock()
}

func writeLoginMessage(writer io.Writer, buf *bytes.Buffer) {
	// length includes protocol version.
	length := buf.Len() + 2
	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(length))
	writeProtoVersion(&netmsg)
	writePasswordHashVersion(&netmsg)
	// 1 copy + 1 n/w write benchmarks faster than 2 n/w writes.
	io.Copy(&netmsg, buf)
	io.Copy(writer, &netmsg)
}

func readLoginResponse(reader io.Reader) (*connectionData, error) {
	buf, err := readMessage(reader)
	if err != nil {
		return nil, err
	}
	connData, err := deserializeLoginResponse(buf)
	return connData, err
}

// AsyncResponseConsumer is a type that consumes responses from asynchronous
// Queries and Execs.
// In the VoltDB go client, asynchronous requests are continuously processed by
// one or more goroutines executing in the background.  When a response from
// the server is received for an asynchronous request, one of the methods in
// this interface is invoked.  An instance of AyncResponseConsumer is passed
// when an asynchronous request is made, this instance will process the
// response for that request.
type AsyncResponseConsumer interface {

	// This method is invoked when an error is returned by an async Query
	// or an Exec.
	ConsumeError(error)
	// This method is invoked when a Result is returned by an async Exec.
	ConsumeResult(driver.Result)
	// This method is invoked when Rows is returned by an async Query.
	ConsumeRows(driver.Rows)
}

type voltAsyncResponse struct {
	conn nodeConn
	han  int64
	ch   <-chan voltResponse
	isQ  bool
	arc  AsyncResponseConsumer
}

func newVoltAsyncResponse(conn nodeConn, han int64, ch <-chan voltResponse, isQuery bool, arc AsyncResponseConsumer) *voltAsyncResponse {
	var vasr = new(voltAsyncResponse)
	vasr.conn = conn
	vasr.han = han
	vasr.ch = ch
	vasr.isQ = isQuery
	vasr.arc = arc
	return vasr
}

func (vasr *voltAsyncResponse) getArc() AsyncResponseConsumer {
	return vasr.arc
}

func (vasr *voltAsyncResponse) channel() <-chan voltResponse {
	return vasr.ch
}

func (vasr *voltAsyncResponse) handle() int64 {
	return vasr.han
}

func (vasr *voltAsyncResponse) isQuery() bool {
	return vasr.isQ
}

func (nc nodeConn) serializeQuery(writer io.Writer, procedure string, handle int64, args []driver.Value) error {

	var call bytes.Buffer
	var err error

	// Serialize the procedure call and its params.
	// Use 0 for handle; it's not necessary in pure sync client.
	if call, err = serializeStatement(procedure, handle, args); err != nil {
		return err
	}

	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(call.Len()))
	io.Copy(&netmsg, &call)
	io.Copy(writer, &netmsg)
	return nil
}

// Null Value type
type nullValue struct {
	colType int8
}

func (nv *nullValue) getColType() int8 {
	return nv.colType
}

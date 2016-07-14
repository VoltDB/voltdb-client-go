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
	"log"
	"net"
	"sync"
	"time"
)

// start back pressure when this many bytes are queued for write
const maxQueuedBytes = 262144

// connectionData are the values returned by a successful login.
type connectionData struct {
	hostId      int32
	connId      int64
	leaderAddr  int32
	buildString string
}

type nodeConn struct {
	dist          *distributer
	connInfo      string
	connData      *connectionData
	tcpConn       *net.TCPConn
	asyncsChannel chan voltResponse
	asyncs        map[int64]*voltAsyncResponse
	asyncsMutex   *sync.Mutex

	nl        *networkListener
	nlCloseCh chan chan bool

	nw   *networkWriter
	nwCh chan<- *procedureInvocation
	nwwg *sync.WaitGroup

	// queued bytes will be read/written by the main client thread and also
	// by the network listener thread.
	queuedBytes int
	qbMutex     sync.Mutex

	open      bool
	openMutex sync.RWMutex
}

func newNodeConn(ci string, dist *distributer) *nodeConn {
	var nc = new(nodeConn)

	nc.connInfo = ci
	nc.asyncsChannel = make(chan voltResponse)
	nc.asyncs = make(map[int64]*voltAsyncResponse)
	nc.asyncsMutex = &sync.Mutex{}
	nc.dist = dist
	return nc
}

func (nc *nodeConn) close() (err error) {
	nc.openMutex.Lock()
	if !nc.open {
		nc.openMutex.Unlock()
		return nil
	} else {
		nc.open = false
		nc.openMutex.Unlock()
	}

	close(nc.asyncsChannel)
	nc.stopNW()

	// stop the network listener.  Send a response channel over the
	// close channel so that the listener knows its being
	// closed and won't try to reconnect.
	respCh := make(chan bool)
	nc.nlCloseCh <- respCh
	// close the tcp connection.  If the listener is blocked
	// on a read this will unblock it.
	if nc.tcpConn != nil {
		err = nc.tcpConn.Close()
		nc.tcpConn = nil
	}
	// wait for the listener to respond that it's been closed.
	<-respCh

	return err
}
func (nc *nodeConn) isOpen() bool {
	var open bool
	nc.openMutex.RLock()
	open = nc.open
	nc.openMutex.RUnlock()
	return open
}

func (nc *nodeConn) setOpen(open bool) {
	nc.openMutex.RLock()
	nc.open = open
	nc.openMutex.RUnlock()
}

func (nc *nodeConn) connect() error {
	tcpConn, connData, err := nc.networkConnect()
	if err != nil {
		return err
	}

	nc.connData = connData
	nc.tcpConn = tcpConn

	nc.nl = newListener(nc, nc.connInfo)
	nc.nlCloseCh = nc.startNL(nc.nl, tcpConn)
	nc.startNW(tcpConn)

	nc.openMutex.Lock()
	nc.open = true
	nc.openMutex.Unlock()
	return nil
}

// called when the network listener loses connection.
// the 'processAsyncs' goroutine and channel stay in place over
// a reconnect, they're not affected.
func (nc *nodeConn) reconnectNL() {

	// lost connection, stop the network writer and close the connection.
	nc.stopNW()
	if nc.tcpConn != nil {
		_ = nc.tcpConn.Close()
		nc.tcpConn = nil
	}
	nc.queuedBytes = 0

	for {
		tcpConn, connData, err := nc.networkConnect()
		if err != nil {
			log.Println(fmt.Printf("Failed to reconnect to server with %s, retrying\n", err))
			time.Sleep(5 * time.Second)
			continue
		}
		nc.tcpConn = tcpConn
		nc.connData = connData

		nc.nlCloseCh = nc.startNL(nc.nl, nc.tcpConn)
		nc.startNW(nc.tcpConn)

		nc.openMutex.Lock()
		nc.open = true
		nc.openMutex.Unlock()
		break
	}
	nc.setOpen(true)
}

func (nc *nodeConn) startNL(nl *networkListener, tcpConn *net.TCPConn) chan chan bool {
	go nc.processAsyncs()
	nlCloseCh := nc.nl.start(tcpConn)
	return nlCloseCh
}

// start the network writer.
func (nc *nodeConn) startNW(tcpConn *net.TCPConn) {
	piCh := make(chan *procedureInvocation, 1000)
	nc.nwCh = piCh
	nwwg := sync.WaitGroup{}
	nc.nwwg = &nwwg
	nc.nw = newNetworkWriter()
	nwwg.Add(1)
	nc.nw.start(tcpConn, piCh, &nwwg)
}

func (nc *nodeConn) stopNW() {
	close(nc.nwCh)
	nc.nwwg.Wait()
}

func (nc *nodeConn) networkConnect() (*net.TCPConn, *connectionData, error) {
	raddr, err := net.ResolveTCPAddr("tcp", nc.connInfo)
	if err != nil {
		return nil, nil, fmt.Errorf("Error resolving %v.", nc.connInfo)
	}
	tcpConn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return nil, nil, err
	}
	login, err := serializeLoginMessage("", "")
	if err != nil {
		tcpConn.Close()
		return nil, nil, err
	}
	writeLoginMessage(tcpConn, &login)
	if err != nil {
		tcpConn.Close()
		return nil, nil, err
	}
	connData, err := readLoginResponse(tcpConn)
	if err != nil {
		tcpConn.Close()
		return nil, nil, err
	}
	return tcpConn, connData, nil
}

func (nc *nodeConn) exec(pi *procedureInvocation) (driver.Result, error) {
	if !nc.open {
		return nil, errors.New("Connection is closed")
	}
	c := nc.nl.registerRequest(nc, pi)
	nc.incrementQueuedBytes(pi.getLen())
	nc.nwCh <- pi

	select {
	case resp := <-c:
		switch resp.(type) {
		case VoltResult:
			return resp.(VoltResult), nil
		case VoltError:
			return nil, resp.(VoltError)
		default:
			return nil, VoltError{error: errors.New("unexpected response type")}
		}
	case <-time.After(pi.timeout):
		return nil, VoltError{voltResponse: voltResponseInfo{status: int8(CONNECTION_TIMEOUT)}, error: errors.New("timeout")}
	}
}

func (nc *nodeConn) execAsync(resCons AsyncResponseConsumer, pi *procedureInvocation) error {
	if !nc.open {
		return errors.New("Connection is closed")
	}
	c := nc.nl.registerRequest(nc, pi)
	vasr := newVoltAsyncResponse(nc, pi.handle, c, false, resCons)
	nc.registerAsync(pi.handle, vasr)
	nc.incrementQueuedBytes(pi.getLen())
	nc.nwCh <- pi
	return nil
}

func (nc *nodeConn) query(pi *procedureInvocation) (driver.Rows, error) {
	if !nc.open {
		return nil, errors.New("Connection is closed")
	}
	c := nc.nl.registerRequest(nc, pi)
	nc.incrementQueuedBytes(pi.getLen())
	nc.nwCh <- pi
	select {
	case resp := <-c:
		switch resp.(type) {
		case VoltRows:
			return resp.(VoltRows), nil
		case VoltError:
			return nil, resp.(VoltError)
		default:
			return nil, VoltError{error: errors.New("unexpected response type")}
		}
	case <-time.After(pi.timeout):
		// TODO: make an error type for timeout
		return nil, VoltError{voltResponse: voltResponseInfo{status: int8(CONNECTION_TIMEOUT)}, error: errors.New("timeout")}
	}
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.
func (nc *nodeConn) queryAsync(rowsCons AsyncResponseConsumer, pi *procedureInvocation) error {
	if !nc.open {
		return errors.New("Connection is closed")
	}
	c := nc.nl.registerRequest(nc, pi)
	vasr := newVoltAsyncResponse(nc, pi.handle, c, true, rowsCons)
	nc.registerAsync(pi.handle, vasr)
	nc.incrementQueuedBytes(pi.getLen())
	nc.nwCh <- pi
	return nil
}

func (nc *nodeConn) incrementQueuedBytes(numBytes int) {
	nc.qbMutex.Lock()
	nc.queuedBytes += numBytes
	nc.qbMutex.Unlock()
}

func (nc *nodeConn) decrementQueuedBytes(numBytes int) {
	nc.qbMutex.Lock()
	nc.queuedBytes -= numBytes
	nc.qbMutex.Unlock()
}

func (nc *nodeConn) getQueuedBytes() int {
	var qb int
	nc.qbMutex.Lock()
	qb = nc.queuedBytes
	nc.qbMutex.Unlock()
	return qb
}

func (nc *nodeConn) drain() {
	var numAsyncs int
	for {
		nc.asyncsMutex.Lock()
		numAsyncs = len(nc.asyncs)
		nc.asyncsMutex.Unlock()
		numQueuedWrites := len(nc.nwCh)
		if numAsyncs == 0 && numQueuedWrites == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (nc *nodeConn) processAsyncs() {
	for resp := range nc.asyncsChannel {
		handle := resp.getHandle()
		nc.asyncsMutex.Lock()
		async := nc.asyncs[handle]
		nc.asyncsMutex.Unlock()
		// can happen on reconnect...
		if async == nil {
			log.Panic("Didn't find async with handle", handle)
			return
		}

		switch resp.(type) {
		case VoltRows:
			async.getArc().ConsumeRows(resp.(VoltRows))
		case VoltResult:
			async.getArc().ConsumeResult(resp.(VoltResult))
		case VoltError:
			async.getArc().ConsumeError(resp.(VoltError))
		default:
			log.Panic("unexpected response type", resp)
		}
		nc.asyncsMutex.Lock()
		delete(nc.asyncs, handle)
		nc.asyncsMutex.Unlock()
	}
}

func (nc *nodeConn) hasBP() bool {
	return nc.getQueuedBytes() >= maxQueuedBytes
}

func (nc *nodeConn) registerAsync(handle int64, vasr *voltAsyncResponse) {
	nc.asyncsMutex.Lock()
	nc.asyncs[handle] = vasr
	nc.asyncsMutex.Unlock()
	go func(ch <-chan voltResponse) {
		select {
		case vr := <-ch:
			nc.asyncsChannel <- vr
		case <-time.After(2 * time.Minute):
			req := nc.nl.removeRequest(vasr.handle())
			if req != nil {
				err := errors.New("timeout")
				nc.asyncsChannel <- err.(voltResponse)
			}
		}
	}(vasr.channel())
}

func (nc *nodeConn) removeAsync(han int64) {
	nc.asyncsMutex.Lock()
	delete(nc.asyncs, han)
	nc.asyncsMutex.Unlock()
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
	conn *nodeConn
	han  int64
	ch   <-chan voltResponse
	isQ  bool
	arc  AsyncResponseConsumer
}

func newVoltAsyncResponse(conn *nodeConn, han int64, ch <-chan voltResponse, isQuery bool, arc AsyncResponseConsumer) *voltAsyncResponse {
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

// Null Value type
type nullValue struct {
	colType int8
}

func (nv *nullValue) getColType() int8 {
	return nv.colType
}

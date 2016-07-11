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
	"runtime"
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
	connInfo      string
	reader        io.Reader
	connData      connectionData
	asyncsChannel chan voltResponse
	asyncs        map[int64]*voltAsyncResponse
	asyncsMutex   *sync.Mutex
	nl            *networkListener
	nlCloseCh     chan bool
	nlwg          *sync.WaitGroup

	nw        *networkWriter
	nwCh      chan<- *procedureInvocation
	nwCloseCh chan bool
	nwwg      *sync.WaitGroup

	// queued bytes will be read/written by the main client thread and also
	// by the network listener thread.
	queuedBytes int
	qbMutex     sync.Mutex

	open      bool
	openMutex sync.RWMutex
}

func newNodeConn(ci string) *nodeConn {
	var nc = new(nodeConn)

	nc.connInfo = ci
	nc.asyncsChannel = make(chan voltResponse)
	nc.asyncs = make(map[int64]*voltAsyncResponse)
	nc.asyncsMutex = &sync.Mutex{}
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

	close(nc.nwCloseCh)
	nc.nlCloseCh <- true

	if nc.reader != nil {
		tcpConn := nc.reader.(*net.TCPConn)
		err = tcpConn.Close()
	}
	nc.nwwg.Wait()
	nc.nlwg.Wait()
	return err
}

func (nc *nodeConn) isOpen() bool {
	var open bool
	nc.openMutex.RLock()
	open = nc.open
	nc.openMutex.RUnlock()
	return open
}

func (nc *nodeConn) connect() error {
	raddr, err := net.ResolveTCPAddr("tcp", nc.connInfo)
	if err != nil {
		return fmt.Errorf("Error resolving %v.", nc.connInfo)
	}
	var tcpConn *net.TCPConn
	if tcpConn, err = net.DialTCP("tcp", nil, raddr); err != nil {
		return err
	}
	login, err := serializeLoginMessage("", "")
	if err != nil {
		return err
	}
	writeLoginMessage(tcpConn, &login)
	if err != nil {
		return err
	}
	connData, err := readLoginResponse(tcpConn)
	if err != nil {
		return err
	}

	nc.reader = tcpConn
	nc.connData = *connData

	nc.nlCloseCh = make(chan bool, 1)
	wg := sync.WaitGroup{}
	nc.nlwg = &wg
	nc.nl = newListener(nc, nc.connInfo, tcpConn, &nc.nwCloseCh, nc.nlwg)

	// The buffer won't be allocated up front, so it's ok to make this big.
	// In practice the buffer will be limited by back pressure
	ch := make(chan *procedureInvocation, 1000)
	nc.nwCh = ch
	nc.nwCloseCh = make(chan bool)
	wg = sync.WaitGroup{}
	nc.nwwg = &wg
	nc.nw = newNetworkWriter(tcpConn, ch, &nc.nwCloseCh, nc.nwwg)

	nc.queuedBytes = 0

	nc.nlwg.Add(1)
	nc.nl.start()
	nc.nwwg.Add(1)
	nc.nw.start()
	go nc.processAsyncs()

	nc.openMutex.Lock()
	nc.open = true
	nc.openMutex.Unlock()
	return nil
}

func (nc *nodeConn) reconnect() {
	for {
		nc.close()
		err := nc.connect()
		if err == nil {
			log.Printf("reconnected %v\n", nc.connInfo)
			break
		} else {
			log.Printf("Failed to connect to host %v with %v retrying...\n", nc.connInfo, err)
			time.Sleep(1 * time.Second)
		}
	}
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
		rslt := resp.(VoltResult)
		if err := rslt.getError(); err != nil {
			return nil, err
		}
		return rslt, nil
	case <-time.After(pi.timeout):
		return nil, errors.New("timeout")
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
		rows := resp.(VoltRows)
		if err := rows.getError(); err != nil {
			return nil, err
		}
		return rows, nil
	case <-time.After(pi.timeout):
		// TODO: make an error type for timeout
		return nil, errors.New("timeout")
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
		if numAsyncs == 0 {
			break
		}
		runtime.Gosched()
	}
}

func (nc *nodeConn) processAsyncs() {
	for {
		resp := <-nc.asyncsChannel
		handle := resp.getHandle()
		nc.asyncsMutex.Lock()
		async := nc.asyncs[handle]
		nc.asyncsMutex.Unlock()
		// can happen on reconnect...
		if async == nil {
			continue
		}

		if async.isQuery() {
			vrows := resp.(VoltRows)
			if err := vrows.getError(); err != nil {
				async.getArc().ConsumeError(err)
			} else {
				async.getArc().ConsumeRows(vrows)

			}
		} else {
			vrslt := resp.(VoltResult)
			if err := vrslt.getError(); err != nil {
				async.getArc().ConsumeError(err)
			} else {
				async.getArc().ConsumeResult(vrslt)
			}
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
	go func() {
		nc.asyncsChannel <- <-vasr.channel()
	}()
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

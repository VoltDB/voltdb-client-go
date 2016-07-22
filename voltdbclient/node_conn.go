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
	dist     *distributer
	connInfo string
	connData *connectionData
	tcpConn  *net.TCPConn

	nl        *networkListener
	nlCloseCh chan chan bool

	nw   *networkWriter
	piCh chan *procedureInvocation
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
	nc.dist = dist
	nc.nl = newNetworkListener(nc, ci)
	nc.nw = newNetworkWriter()
	return nc
}

// when the node conn is closed by its owning distributer
func (nc *nodeConn) close() error {
	if !nc.setClosed() {
		return nil
	}
	// stop the network listener.  Send a response channel over the
	// close channel so that the listener knows its being
	// closed and won't try to reconnect.
	respCh := make(chan bool)
	nc.nlCloseCh <- respCh
	// close the tcp connection.  If the listener is blocked
	// on a read this will unblock it.
	if nc.tcpConn != nil {
		_ = nc.tcpConn.Close()
		nc.tcpConn = nil
	}
	// wait for the listener to respond that it's been closed.
	<-respCh
	nc.disconnect()
	return nil
}

func (nc *nodeConn) handleDisconnect() {
	nc.setClosed()
	nc.tcpConn = nil
	nc.disconnect()
	nc.reconnect()
}

// when the node conn is closing because it lost connection.  determined by
// its nl.
func (nc *nodeConn) disconnect() {
	nc.nw.disconnect(nc.piCh)
	nc.nwwg.Wait()
	nc.nl.disconnect()
}

func (nc *nodeConn) isOpen() bool {
	var open bool
	nc.openMutex.RLock()
	open = nc.open
	nc.openMutex.RUnlock()
	return open
}

// returns true if the nc is currently open,
// false if it wasn't open.
func (nc *nodeConn) setClosed() bool {
	nc.openMutex.RLock()
	defer nc.openMutex.RUnlock()
	if !nc.open {
		return false
	}
	nc.open = false
	return true
}

func (nc *nodeConn) setOpen() {
	nc.openMutex.RLock()
	nc.open = true
	defer nc.openMutex.RUnlock()

}

func (nc *nodeConn) connect() error {
	tcpConn, connData, err := nc.networkConnect()
	if err != nil {
		return err
	}

	nc.connData = connData
	nc.tcpConn = tcpConn

	nc.nlCloseCh = nc.nl.connect(tcpConn)
	nc.connectNW(tcpConn)

	nc.openMutex.Lock()
	nc.open = true
	nc.openMutex.Unlock()
	return nil
}

// called when the network listener loses connection.
// the 'processAsyncs' goroutine and channel stay in place over
// a reconnect, they're not affected.
func (nc *nodeConn) reconnect() {

	for {
		tcpConn, connData, err := nc.networkConnect()
		if err != nil {
			log.Println(fmt.Printf("Failed to reconnect to server with %s, retrying\n", err))
			time.Sleep(5 * time.Second)
			continue
		}
		nc.tcpConn = tcpConn
		nc.connData = connData

		nc.nlCloseCh = nc.nl.connect(nc.tcpConn)
		nc.connectNW(nc.tcpConn)

		nc.openMutex.Lock()
		nc.open = true
		nc.openMutex.Unlock()
		break
	}
	nc.setOpen()
}

// start the network writer.
func (nc *nodeConn) connectNW(tcpConn *net.TCPConn) {
	piCh := make(chan *procedureInvocation, 1000)
	nc.piCh = piCh
	nwwg := sync.WaitGroup{}
	nc.nwwg = &nwwg
	nwwg.Add(1)
	nc.nw.connect(tcpConn, piCh, &nwwg)
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

func (nc *nodeConn) exec(pi *procedureInvocation, respRcvd func(int32)) (driver.Result, error) {
	if !nc.open {
		return nil, errors.New("Connection is closed")
	}
	c := nc.nl.registerSyncRequest(nc, pi)
	nc.incrementQueuedBytes(pi.getLen())
	nc.piCh <- pi

	select {
	case resp := <-c:
		switch resp.(type) {
		case VoltResult:
			respRcvd(resp.getClusterRoundTripTime())
			return resp.(VoltResult), nil
		case VoltError:
			respRcvd(-1)
			return nil, resp.(VoltError)
		default:
			panic("unexpected response type")
		}
	case <-time.After(pi.timeout):
		if respRcvd != nil {
			respRcvd(int32(pi.timeout.Seconds() * 1000))
		}
		return nil, VoltError{voltResponse: voltResponseInfo{status: CONNECTION_TIMEOUT, clusterRoundTripTime: -1}, error: errors.New("timeout")}
	}
}

func (nc *nodeConn) execAsync(resCons AsyncResponseConsumer, pi *procedureInvocation, respRcvd func(int32)) error {
	if !nc.open {
		return errors.New("Connection is closed")
	}
	nc.nl.registerAsyncRequest(nc, pi, resCons, respRcvd)
	nc.incrementQueuedBytes(pi.getLen())
	nc.piCh <- pi
	return nil
}

func (nc *nodeConn) query(pi *procedureInvocation, respRcvd func(int32)) (driver.Rows, error) {
	if !nc.open {
		return nil, errors.New("Connection is closed")
	}
	c := nc.nl.registerSyncRequest(nc, pi)
	nc.incrementQueuedBytes(pi.getLen())
	nc.piCh <- pi
	select {
	case resp := <-c:
		switch resp.(type) {
		case VoltRows:
			if respRcvd != nil {
				respRcvd(resp.getClusterRoundTripTime())
			}
			return resp.(VoltRows), nil
		case VoltError:
			if respRcvd != nil {
				respRcvd(-1)
			}
			return nil, resp.(VoltError)
		default:
			panic("unexpected response type")
		}
	case <-time.After(pi.timeout):
		if respRcvd != nil {
			respRcvd(int32(pi.timeout.Seconds() * 1000))
		}
		return nil, VoltError{voltResponse: voltResponseInfo{status: CONNECTION_TIMEOUT, clusterRoundTripTime: -1}, error: errors.New("timeout")}
	}
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.
func (nc *nodeConn) queryAsync(rowsCons AsyncResponseConsumer, pi *procedureInvocation, respRcvd func(int32)) error {
	if !nc.open {
		return errors.New("Connection is closed")
	}
	nc.nl.registerAsyncRequest(nc, pi, rowsCons, respRcvd)
	nc.incrementQueuedBytes(pi.getLen())
	nc.piCh <- pi
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
	for {
		numRequests := nc.nl.numOutstandingRequests()
		numQueuedWrites := len(nc.piCh)
		if numRequests == 0 && numQueuedWrites == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (nc *nodeConn) hasBP() bool {
	return nc.getQueuedBytes() >= maxQueuedBytes
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

// Null Value type
type nullValue struct {
	colType int8
}

func (nv *nullValue) getColType() int8 {
	return nv.colType
}

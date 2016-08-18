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
	conn        *Conn
	connInfo    string
	connData    *connectionData
	tcpConn     *net.TCPConn

	nl          *networkListener
	drainCh     chan chan bool
	bpCh        chan chan bool
	nlCloseCh   chan chan bool

	// channel for pi's meant specifically for this connection.
	ncPiCh      chan *procedureInvocation

	open        bool
	openMutex   sync.RWMutex
}

func newNodeConn(ci string, conn *Conn, ncPiCh chan *procedureInvocation) *nodeConn {
	var nc = new(nodeConn)
	nc.connInfo = ci
	nc.conn = conn
	nc.ncPiCh = ncPiCh
	nc.bpCh = make(chan chan bool)
	nc.drainCh = make(chan chan bool)
	nc.nl = newNetworkListener(nc, ci, nc.ncPiCh)
	return nc
}

func (nc *nodeConn) submit(pi *procedureInvocation) {
	nc.ncPiCh <- pi
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

// when the node conn is closing because it lost connection.  determined by
// its nl.
func (nc *nodeConn) disconnect() {
	// nc.nl.disconnect()
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

func (nc *nodeConn) connect(piCh <-chan *procedureInvocation) error {
	tcpConn, connData, err := nc.networkConnect()
	if err != nil {
		return err
	}

	nc.connData = connData
	nc.tcpConn = tcpConn

	responseCh := make(chan *bytes.Buffer, 10)
	go nc.nl.listen(tcpConn, responseCh)

	nc.nlCloseCh = make(chan chan bool, 1)
	nc.drainCh = make(chan chan bool, 1)

	go nc.nl.loop(tcpConn, piCh, responseCh, nc.bpCh, nc.drainCh, nc.nlCloseCh)


	nc.openMutex.Lock()
	nc.open = true
	nc.openMutex.Unlock()
	return nil
}

// called when the network listener loses connection.
// the 'processAsyncs' goroutine and channel stay in place over
// a reconnect, they're not affected.
func (nc *nodeConn) reconnect(piCh <-chan *procedureInvocation) {

	for {
		tcpConn, connData, err := nc.networkConnect()
		if err != nil {
			log.Println(fmt.Printf("Failed to reconnect to server with %s, retrying\n", err))
			time.Sleep(5 * time.Second)
			continue
		}
		nc.tcpConn = tcpConn
		nc.connData = connData

		responseCh := make(chan *bytes.Buffer, 10)
		go nc.nl.listen(tcpConn, responseCh)
		nc.nlCloseCh = make(chan chan bool, 1)
		go nc.nl.loop(tcpConn, piCh, responseCh, nc.bpCh, nc.drainCh, nc.nlCloseCh)

		nc.openMutex.Lock()
		nc.open = true
		nc.openMutex.Unlock()
		break
	}
	nc.setOpen()
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

func (nc *nodeConn) drain(respCh chan bool) {
	nc.drainCh <- respCh
}

func (nc *nodeConn) hasBP() bool {
	respCh := make(chan bool)
	nc.bpCh<- respCh
	return <-respCh
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

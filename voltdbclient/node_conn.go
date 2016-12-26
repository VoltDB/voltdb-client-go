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
	"time"
)

// start back pressure when this many bytes are queued for write
const maxQueuedBytes = 262144

// connectionData are the values returned by a successful login.
type connectionData struct {
	hostID      int32
	connID      int64
	leaderAddr  int32
	buildString string
}

type nodeConn struct {
	connInfo string
	connData *connectionData
	tcpConn  *net.TCPConn

	drainCh chan chan bool
	bpCh    chan chan bool
	closeCh chan chan bool

	// channel for pi's meant specifically for this connection.
	ncPiCh chan *procedureInvocation
}

func newNodeConn(ci string, ncPiCh chan *procedureInvocation) *nodeConn {
	var nc = new(nodeConn)
	nc.connInfo = ci
	nc.ncPiCh = ncPiCh
	nc.bpCh = make(chan chan bool)
	nc.closeCh = make(chan chan bool)
	nc.drainCh = make(chan chan bool)
	return nc
}

func (nc *nodeConn) submit(pi *procedureInvocation) {
	nc.ncPiCh <- pi
}

// when the node conn is closed by its owning distributer
func (nc *nodeConn) close() chan bool {
	respCh := make(chan bool, 1)
	nc.closeCh <- respCh
	return respCh
}

func (nc *nodeConn) connect(protocolVersion int, piCh <-chan *procedureInvocation) error {
	tcpConn, connData, err := nc.networkConnect(protocolVersion)
	if err != nil {
		return err
	}

	nc.connData = connData
	nc.tcpConn = tcpConn

	responseCh := make(chan *bytes.Buffer, 10)
	go nc.listen(tcpConn, responseCh)

	nc.drainCh = make(chan chan bool, 1)

	go nc.loop(tcpConn, piCh, responseCh, nc.bpCh, nc.drainCh)
	return nil
}

// called when the network listener loses connection.
// the 'processAsyncs' goroutine and channel stay in place over
// a reconnect, they're not affected.
func (nc *nodeConn) reconnect(protocolVersion int, piCh <-chan *procedureInvocation) {

	for {
		tcpConn, connData, err := nc.networkConnect(protocolVersion)
		if err != nil {
			log.Println(fmt.Printf("Failed to reconnect to server with %s, retrying\n", err))
			time.Sleep(5 * time.Second)
			continue
		}
		nc.tcpConn = tcpConn
		nc.connData = connData

		responseCh := make(chan *bytes.Buffer, 10)
		go nc.listen(tcpConn, responseCh)
		go nc.loop(tcpConn, piCh, responseCh, nc.bpCh, nc.drainCh)
		break
	}
}

func (nc *nodeConn) networkConnect(protocolVersion int) (*net.TCPConn, *connectionData, error) {
	raddr, err := net.ResolveTCPAddr("tcp", nc.connInfo)
	if err != nil {
		return nil, nil, fmt.Errorf("Error resolving %v.", nc.connInfo)
	}
	tcpConn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to connect to server %v.", nc.connInfo)
	}
	login, err := serializeLoginMessage(protocolVersion, "", "")
	if err != nil {
		tcpConn.Close()
		return nil, nil, fmt.Errorf("Failed to serialize login message %v.", nc.connInfo)
	}

	writeLoginMessage(protocolVersion, tcpConn, &login)
	if err != nil {
		tcpConn.Close()
		return nil, nil, fmt.Errorf("Failed to login to server %v.", nc.connInfo)
	}
	connData, err := readLoginResponse(tcpConn)
	if err != nil {
		tcpConn.Close()
		return nil, nil, fmt.Errorf("Failed to login to server %v.", nc.connInfo)
	}
	return tcpConn, connData, nil
}

func (nc *nodeConn) drain(respCh chan bool) {
	nc.drainCh <- respCh
}

func (nc *nodeConn) hasBP() bool {
	respCh := make(chan bool)
	nc.bpCh <- respCh
	return <-respCh
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (nc *nodeConn) listen(reader io.Reader, responseCh chan<- *bytes.Buffer) {
	for {
		buf, err := readMessage(reader)
		if err != nil {
			if responseCh == nil {
				// exiting
				return
			}
			// TODO: put the error on the channel
			// the owner needs to reconnect
			return
		}
		responseCh <- buf
	}
}

func (nc *nodeConn) loop(writer io.Writer, piCh <-chan *procedureInvocation, responseCh <-chan *bytes.Buffer, bpCh <-chan chan bool, drainCh chan chan bool) {
	// declare mutable state
	requests := make(map[int64]*networkRequest)
	ncPiCh := nc.ncPiCh
	var draining bool
	var drainRespCh chan bool
	var queuedBytes int
	var bp bool

	var tci = int64(DefaultQueryTimeout / 10)                    // timeout check interval
	tcc := time.NewTimer(time.Duration(tci) * time.Nanosecond).C // timeout check timer channel

	// for ping
	var pingTimeout = 2 * time.Minute
	pingSentTime := time.Now()
	var pingOutstanding bool

	for {
		// setup select cases
		if draining {
			if queuedBytes == 0 && len(nc.ncPiCh) == 0 && len(piCh) == 0 {
				drainRespCh <- true
				drainRespCh = nil
				draining = false
			}
		}

		if queuedBytes > maxQueuedBytes && ncPiCh != nil {
			ncPiCh = nil
			bp = true
		} else if ncPiCh == nil {
			ncPiCh = nc.ncPiCh
			bp = false
		}

		// ping
		pingSinceSent := time.Now().Sub(pingSentTime)
		if pingOutstanding {
			if pingSinceSent > pingTimeout {
				// TODO: should disconnect
			}
		} else if pingSinceSent > pingTimeout/3 {
			nc.sendPing(writer)
			pingOutstanding = true
			pingSentTime = time.Now()
		}

		select {
		case respCh := <-nc.closeCh:
			respCh <- true
			return
		case pi := <-ncPiCh:
			nc.handleProcedureInvocation(writer, pi, &requests, &queuedBytes)
		case pi := <-piCh:
			nc.handleProcedureInvocation(writer, pi, &requests, &queuedBytes)
		case resp := <-responseCh:
			handle, err := readLong(resp)
			// can't do anything without a handle.  If reading the handle fails,
			// then log and drop the message.
			if err != nil {
				continue
			}
			if handle == PingHandle {
				pingOutstanding = false
				continue
			}
			req := requests[handle]
			if req == nil {
				// there's a race here with timeout.  A request can be timed out and
				// then a response received.  In this case drop the response.
				continue
			}
			queuedBytes -= req.numBytes
			delete(requests, handle)
			if req.isSync() {
				nc.handleSyncResponse(handle, resp, req)
			} else {
				go nc.handleAsyncResponse(handle, resp, req)
			}
		case respBPCh := <-bpCh:
			respBPCh <- bp
		case drainRespCh = <-drainCh:
			draining = true
		// check for timed out procedure invocations
		case <-tcc:
			for _, req := range requests {
				if time.Now().After(req.submitted.Add(req.timeout)) {
					queuedBytes -= req.numBytes
					nc.handleTimeout(req)
					delete(requests, req.handle)
				}
			}
			tcc = time.NewTimer(time.Duration(tci) * time.Nanosecond).C
		}
	}
}

func (nc *nodeConn) handleProcedureInvocation(writer io.Writer, pi *procedureInvocation, requests *map[int64]*networkRequest, queuedBytes *int) {
	var nr *networkRequest
	if pi.isAsync() {
		nr = newAsyncRequest(pi.handle, pi.responseCh, pi.isQuery, pi.arc, pi.getLen(), pi.timeout, time.Now())
	} else {
		nr = newSyncRequest(pi.handle, pi.responseCh, pi.isQuery, pi.getLen(), pi.timeout, time.Now())
	}
	(*requests)[pi.handle] = nr
	*queuedBytes += pi.slen
	serializePI(writer, pi)
}

func (nc *nodeConn) handleSyncResponse(handle int64, r io.Reader, req *networkRequest) {
	respCh := req.getChan()
	rsp, err := deserializeResponse(r, handle)
	if err != nil {
		respCh <- err.(voltResponse)
	} else if req.isQuery() {
		if rows, err := deserializeRows(r, rsp); err != nil {
			respCh <- err.(voltResponse)
		} else {
			respCh <- rows
		}
	} else {
		if result, err := deserializeResult(r, rsp); err != nil {
			respCh <- err.(voltResponse)
		} else {
			respCh <- result
		}
	}

}

func (nc *nodeConn) handleAsyncResponse(handle int64, r io.Reader, req *networkRequest) {
	rsp, err := deserializeResponse(r, handle)
	if err != nil {
		req.arc.ConsumeError(err)
	} else if req.isQuery() {
		if rows, err := deserializeRows(r, rsp); err != nil {
			req.arc.ConsumeError(err)
		} else {
			req.arc.ConsumeRows(rows)
		}
	} else {
		if result, err := deserializeResult(r, rsp); err != nil {
			req.arc.ConsumeError(err)
		} else {
			req.arc.ConsumeResult(result)
		}
	}
}

func (nc *nodeConn) handleTimeout(req *networkRequest) {
	err := errors.New("timeout")
	verr := VoltError{voltResponse: emptyVoltResponseInfo(), error: err}
	if req.isSync() {
		respCh := req.getChan()
		respCh <- verr
	} else {
		req.arc.ConsumeError(verr)
	}
}

func (nc *nodeConn) sendPing(writer io.Writer) {
	pi := newProcedureInvocationByHandle(PingHandle, true, "@Ping", []driver.Value{})
	serializePI(writer, pi)
}

func writeLoginMessage(protocolVersion int, writer io.Writer, buf *bytes.Buffer) {
	var netmsg bytes.Buffer
	if protocolVersion == 0 {
		length := buf.Len() + 1
		writeInt(&netmsg, int32(length))
		writeProtoVersion(&netmsg)
	} else {
		length := buf.Len() + 2
		writeInt(&netmsg, int32(length))
		writeProtoVersion(&netmsg)
		writePasswordHashVersion(&netmsg)
	}
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

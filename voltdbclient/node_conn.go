/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

	"github.com/VoltDB/voltdb-client-go/wire"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

// start back pressure when this many bytes are queued for write
const maxQueuedBytes = 262144
const maxResponseBuffer = 10000

type nodeConn struct {
	connInfo string
	connData *wire.ConnInfo
	tcpConn  *net.TCPConn

	drainCh      chan chan bool
	bpCh         chan chan bool
	closeCh      chan chan bool
	responseCh   chan *bytes.Buffer
	requests     map[int64]*networkRequest
	queuedBytes  int
	bp           bool
	disconnected bool
}

func newNodeConn(ci string) *nodeConn {
	return &nodeConn{
		connInfo:   ci,
		bpCh:       make(chan chan bool),
		closeCh:    make(chan chan bool),
		drainCh:    make(chan chan bool),
		responseCh: make(chan *bytes.Buffer, maxResponseBuffer),
		requests:   make(map[int64]*networkRequest),
	}
}

func (nc *nodeConn) submit(pi *procedureInvocation) (int, error) {
	return nc.handleProcedureInvocation(pi)
}

// when the node conn is closed by its owning distributer
func (nc *nodeConn) close() chan bool {
	respCh := make(chan bool, 1)
	nc.closeCh <- respCh
	return respCh
}

func (nc *nodeConn) connect(protocolVersion int) error {
	tcpConn, connData, err := nc.networkConnect(protocolVersion)
	if err != nil {
		return err
	}
	nc.connData = connData
	nc.tcpConn = tcpConn

	go nc.listen()

	nc.drainCh = make(chan chan bool, 1)

	go nc.loop(nc.bpCh, nc.drainCh)
	return nil
}

// called when the network listener loses connection.
// the 'processAsyncs' goroutine and channel stay in place over
// a reconnect, they're not affected.
func (nc *nodeConn) reconnect(protocolVersion int) {
	for {
		tcpConn, connData, err := nc.networkConnect(protocolVersion)
		if err != nil {
			log.Println(fmt.Printf("Failed to reconnect to server with %s, retrying\n", err))
			time.Sleep(5 * time.Second)
			continue
		}
		nc.tcpConn = tcpConn
		nc.connData = connData
		go nc.listen()
		go nc.loop(nc.bpCh, nc.drainCh)
		break
	}
}

func (nc *nodeConn) networkConnect(protocolVersion int) (*net.TCPConn, *wire.ConnInfo, error) {
	u, err := parseURL(nc.connInfo)
	if err != nil {
		return nil, nil, err
	}
	raddr, err := net.ResolveTCPAddr("tcp", u.Host)
	if err != nil {
		return nil, nil, fmt.Errorf("error resolving %v", nc.connInfo)
	}
	tcpConn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to server %v", nc.connInfo)
	}
	pass, _ := u.User.Password()
	encoder := wire.NewEncoder()
	login, err := encoder.Login(protocolVersion, u.User.Username(), pass)
	if err != nil {
		tcpConn.Close()
		return nil, nil, fmt.Errorf("failed to serialize login message %v", nc.connInfo)
	}
	_, err = tcpConn.Write(login)
	if err != nil {
		return nil, nil, err
	}
	decoder := wire.NewDecoder(tcpConn)
	i, err := decoder.Login()
	if err != nil {
		tcpConn.Close()
		return nil, nil, fmt.Errorf("failed to login to server %v", nc.connInfo)
	}
	return tcpConn, i, nil
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
func (nc *nodeConn) listen() {
	d := wire.NewDecoder(nc.tcpConn)
	s := &wire.Decoder{}
	for {
		b, err := d.Message()
		if err != nil {
			if nc.responseCh == nil {
				// exiting
				return
			}
			// TODO: put the error on the channel
			// the owner needs to reconnect
			return
		}
		buf := bytes.NewBuffer(b)
		s.SetReader(buf)
		_, err = s.Byte()
		if err != nil {
			if nc.responseCh == nil {
				return
			}
			return
		}
		nc.responseCh <- buf
	}
}

func (nc *nodeConn) loop(bpCh <-chan chan bool, drainCh chan chan bool) {
	// declare mutable state
	// requests := make(map[int64]*networkRequest)
	// ncPiCh := nc.ncPiCh
	var draining bool
	var drainRespCh chan bool
	// var queuedBytes int
	// var bp bool

	var tci = int64(DefaultQueryTimeout / 10)                    // timeout check interval
	tcc := time.NewTimer(time.Duration(tci) * time.Nanosecond).C // timeout check timer channel

	// for ping
	var pingTimeout = 2 * time.Minute
	pingSentTime := time.Now()
	var pingOutstanding bool
	for {
		// setup select cases
		if draining {
			if nc.queuedBytes == 0 {
				drainRespCh <- true
				drainRespCh = nil
				draining = false
			}
		}

		// if nc.queuedBytes > maxQueuedBytes && ncPiCh != nil {
		// 	ncPiCh = nil
		// 	nc.bp = true
		// } else if ncPiCh == nil {
		// 	ncPiCh = nc.ncPiCh
		// 	nc.bp = false
		// }

		// ping
		pingSinceSent := time.Now().Sub(pingSentTime)
		if pingOutstanding {
			if pingSinceSent > pingTimeout {
				// TODO: should disconnect
			}
		} else if pingSinceSent > pingTimeout/3 {
			nc.sendPing(nc.tcpConn)
			pingOutstanding = true
			pingSentTime = time.Now()
		}

		select {
		case respCh := <-nc.closeCh:
			nc.tcpConn.Close()
			respCh <- true
			return
		// case pi := <-ncPiCh:
		// 	nc.handleProcedureInvocation(pi)
		case resp := <-nc.responseCh:
			decoder := wire.NewDecoder(resp)
			handle, err := decoder.Int64()
			// can't do anything without a handle.  If reading the handle fails,
			// then log and drop the message.
			if err != nil {
				continue
			}
			if handle == PingHandle {
				pingOutstanding = false
				continue
			}
			req := nc.requests[handle]
			if req == nil {
				// there's a race here with timeout.  A request can be timed out and
				// then a response received.  In this case drop the response.
				continue
			}
			nc.queuedBytes -= req.numBytes

			delete(nc.requests, handle)
			if req.isSync() {
				nc.handleSyncResponse(handle, resp, req)
			} else {
				nc.handleAsyncResponse(handle, resp, req)
			}

		case respBPCh := <-bpCh:
			respBPCh <- nc.bp
		case drainRespCh = <-drainCh:
			draining = true
		// check for timed out procedure invocations
		case <-tcc:
			for _, req := range nc.requests {
				if time.Now().After(req.submitted.Add(req.timeout)) {
					nc.queuedBytes -= req.numBytes
					nc.handleTimeout(req)
					delete(nc.requests, req.handle)
				}
			}
			tcc = time.NewTimer(time.Duration(tci) * time.Nanosecond).C
		}
	}
}

func (nc *nodeConn) handleProcedureInvocation(pi *procedureInvocation) (int, error) {
	var nr *networkRequest
	if pi.isAsync() {
		nr = newAsyncRequest(pi.handle, pi.responseCh, pi.isQuery, pi.arc, pi.getLen(), pi.timeout, time.Now())
	} else {
		nr = newSyncRequest(pi.handle, pi.responseCh, pi.isQuery, pi.getLen(), pi.timeout, time.Now())
	}
	nc.requests[pi.handle] = nr
	nc.queuedBytes += pi.slen
	encoder := wire.NewEncoder()
	EncodePI(encoder, pi)
	n, err := nc.tcpConn.Write(encoder.Bytes())
	if err != nil {
		return n, fmt.Errorf("%s: %v", nc.connInfo, err)
	}
	return 0, nil
}

func (nc *nodeConn) handleSyncResponse(handle int64, r io.Reader, req *networkRequest) {
	respCh := req.getChan()
	decoder := wire.NewDecoder(r)
	rsp, err := decodeResponse(decoder, handle)
	if err != nil {
		e := err.(VoltError)
		e.error = fmt.Errorf("%s: %v", nc.connInfo, e.error)
		respCh <- e
	} else if req.isQuery() {

		if rows, err := decodeRows(decoder, rsp); err != nil {
			respCh <- err.(voltResponse)
		} else {
			respCh <- rows
		}
	} else {
		if result, err := decodeResult(decoder, rsp); err != nil {
			respCh <- err.(voltResponse)
		} else {
			respCh <- result
		}
	}

}

func (nc *nodeConn) handleAsyncResponse(handle int64, r io.Reader, req *networkRequest) {
	d := wire.NewDecoder(r)
	rsp, err := decodeResponse(d, handle)
	if err != nil {
		req.arc.ConsumeError(err)
	} else if req.isQuery() {
		if rows, err := decodeRows(d, rsp); err != nil {
			req.arc.ConsumeError(err)
		} else {
			req.arc.ConsumeRows(rows)
		}
	} else {
		if result, err := decodeResult(d, rsp); err != nil {
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
	encoder := wire.NewEncoder()
	EncodePI(encoder, pi)
	writer.Write(encoder.Bytes())
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

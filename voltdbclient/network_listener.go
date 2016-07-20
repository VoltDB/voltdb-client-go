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
	"errors"
	"io"
	"log"
	"sync"
	"time"
)

// NetworkListener listens for responses for asynchronous procedure calls from
// the server.  If a callback (channel) is registered for the procedure, the
// listener puts the response on the channel (calls back).
type networkListener struct {
	nc           *nodeConn
	ci           string
	requests     map[int64]*networkRequest
	requestMutex sync.Mutex
	asyncsCh     chan asyncVoltResponse
}

func newNetworkListener(nc *nodeConn, ci string) *networkListener {
	var nl = new(networkListener)
	nl.nc = nc
	nl.ci = ci
	return nl
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (nl *networkListener) listen(reader io.Reader, closeCh chan chan bool) {
	for {
		// the owning node conn told nl to disconnect
		respCh := nl.readClose(closeCh)
		if respCh != nil {
			respCh <- true
			return
		}
		// can't do anything without a handle.  If reading the handle fails,
		// then log and drop the message.
		buf, err := readMessage(reader)
		if err != nil {
			respCh := nl.readClose(closeCh)
			if respCh != nil {
				// node conn told nl to stop
				respCh <- true
				return
			} else {
				// have lost connection.  tell the owning node conn to disconnect
				// and to reconnect.
				nl.nc.handleDisconnect()
				return
			}
		}
		handle, err := readLong(buf)
		// can't do anything without a handle.  If reading the handle fails,
		// then log and drop the message.
		if err != nil {
			log.Printf("For %v error reading handle %v\n", nl.ci, err)
			continue
		}
		nl.readResponse(buf, handle)
	}
}

func (nl *networkListener) readClose(closeCh chan chan bool) chan bool {
	select {
	case respCh := <-closeCh:
		return respCh
	case <-time.After(5 * time.Millisecond):
		return nil
	}
}

func (nl *networkListener) disconnect() {
	// if connection to server is lost then there won't be a response
	// for any of the remaining requests.  Time them out.
	err := errors.New("connection lost")
	nl.requestMutex.Lock()
	defer nl.requestMutex.Unlock()
	for _, req := range nl.requests {
		verr := VoltError{voltResponse: nil, error: err}
		req.getChan() <- voltResponse(verr)
	}
	nl.requests = nil
	close(nl.asyncsCh)
	nl.asyncsCh = nil
}

// blocks until there are no outstanding asyncs to process
func (nl *networkListener) numOutstandingRequests() int {
	var numOutstanding int
	nl.requestMutex.Lock()
	numOutstanding = len(nl.requests)
	nl.requestMutex.Unlock()
	return numOutstanding
}

// reads and deserializes a response from the server.
func (nl *networkListener) readResponse(r io.Reader, handle int64) {
	var err error
	rsp, err := deserializeResponse(r, handle)

	// handling special response
	// TODO add sentPing() for checke liveness
	if handle == PING_HANDLE {
		// nl.outstandingping = false
		return
	}
	if handle == ASYNC_TOPO_HANDLE {
		if err != nil {
			nl.nc.dist.handleSubscribeError(err)
		} else {
			if rows, err := deserializeRows(r, rsp); err != nil {
				nl.nc.dist.handleSubscribeError(err)
			} else {
				nl.nc.dist.handleSubscribeRow(rows)
			}

		}
		return
	}

	// remove the associated request as soon as the handle is found.
	req := nl.removeRequest(handle)
	if req == nil {
		// there's a race here with timeout.  A request can be timed out and
		// then a response received.  In this case drop the response.
		return
	}
	if err != nil {
		req.getChan() <- err.(voltResponse)
	} else if req.isQuery() {
		if rows, err := deserializeRows(r, rsp); err != nil {
			req.getChan() <- err.(voltResponse)
		} else {
			req.getChan() <- rows
		}
	} else {
		if result, err := deserializeResult(r, rsp); err != nil {
			req.getChan() <- err.(voltResponse)
		} else {
			req.getChan() <- result
		}
	}
}

func (nl *networkListener) registerSyncRequest(nc *nodeConn, pi *procedureInvocation) <-chan voltResponse {
	ch := make(chan voltResponse, 1)
	nr := newSyncRequest(nc, ch, pi.isQuery, pi.getLen())
	nl.requestMutex.Lock()
	nl.requests[pi.handle] = nr
	nl.requestMutex.Unlock()
	return ch
}

func (nl *networkListener) registerAsyncRequest(nc *nodeConn, pi *procedureInvocation, arc AsyncResponseConsumer, respRcvd func(int32)) {
	handle := pi.handle
	ch := make(chan voltResponse, 1)
	nr := newAsyncRequest(nc, ch, pi.isQuery, arc, pi.getLen())
	nl.requestMutex.Lock()
	nl.requests[handle] = nr
	nl.requestMutex.Unlock()

	go func(ch <-chan voltResponse) {
		select {
		case vr := <-ch:
			if respRcvd != nil {
				respRcvd(vr.getClusterRoundTripTime())
			}
			avr := asyncVoltResponse{vr, arc}
			nl.asyncsCh <- avr
		case <-time.After(2 * time.Minute):
			if respRcvd != nil {
				// timeout in milliseconds
				respRcvd(int32(2 * time.Minute.Seconds() * 1000))
			}
			req := nl.removeRequest(handle)

			if req != nil {
				err := errors.New("timeout")
				verr := VoltError{voltResponse: nil, error: err}
				avr := asyncVoltResponse{verr, arc}
				nl.asyncsCh <- avr
			}
		}
	}(ch)
}

// need to have a lock on the map when this is invoked.
func (nl *networkListener) removeRequest(handle int64) *networkRequest {
	var req *networkRequest

	nl.requestMutex.Lock()
	defer nl.requestMutex.Unlock()
	req = nl.requests[handle]
	if req != nil {
		req.getNodeConn().decrementQueuedBytes(req.numBytes)
		delete(nl.requests, handle)
	}
	return req
}

func (nl *networkListener) connect(reader io.Reader) chan chan bool {
	nl.requests = make(map[int64]*networkRequest)
	nl.asyncsCh = make(chan asyncVoltResponse)
	go nl.processAsyncs()
	closeCh := make(chan chan bool, 1)
	go nl.listen(reader, closeCh)
	return closeCh
}

func (nl *networkListener) processAsyncs() {
	for avr := range nl.asyncsCh {
		vr := avr.vr
		arc := avr.arc
		switch vr.(type) {
		case VoltRows:
			arc.ConsumeRows(vr.(VoltRows))
		case VoltResult:
			arc.ConsumeResult(vr.(VoltResult))
		case VoltError:
			arc.ConsumeError(vr.(VoltError))
		default:
			log.Panic("unexpected response type", vr)
		}
	}
}

type networkRequest struct {
	nc    *nodeConn
	query bool
	ch    chan voltResponse
	sync  bool
	arc   AsyncResponseConsumer
	// the size of the serialized request written to the server.
	numBytes int
}

func newSyncRequest(nc *nodeConn, ch chan voltResponse, isQuery bool, numBytes int) *networkRequest {
	var nr = new(networkRequest)
	nr.nc = nc
	nr.query = isQuery
	nr.ch = ch
	nr.sync = true
	nr.arc = nil
	nr.numBytes = numBytes
	return nr
}

func newAsyncRequest(nc *nodeConn, ch chan voltResponse, isQuery bool, arc AsyncResponseConsumer, numBytes int) *networkRequest {
	var nr = new(networkRequest)
	nr.nc = nc
	nr.query = isQuery
	nr.ch = ch
	nr.sync = false
	nr.arc = arc
	nr.numBytes = numBytes
	return nr
}

func (nr *networkRequest) getArc() AsyncResponseConsumer {
	return nr.arc
}

func (nr *networkRequest) getNodeConn() *nodeConn {
	return nr.nc
}

func (nr *networkRequest) isSync() bool {
	return nr.sync
}

func (nr *networkRequest) isQuery() bool {
	return nr.query
}

func (nr *networkRequest) getChan() chan voltResponse {
	return nr.ch
}

func (nr *networkRequest) getNumBytes() int {
	return nr.numBytes
}

// wrap these two things together so that they can go on the asyncs channel.
type asyncVoltResponse struct {
	vr  voltResponse
	arc AsyncResponseConsumer
}

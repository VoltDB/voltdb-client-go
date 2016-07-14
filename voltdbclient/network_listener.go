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
}

func newListener(nc *nodeConn, ci string) *networkListener {
	var nl = new(networkListener)
	nl.nc = nc
	nl.ci = ci
	nl.requests = make(map[int64]*networkRequest)
	return nl
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (nl *networkListener) listen(reader io.Reader, closeCh *chan bool, wg *sync.WaitGroup) {
	for {
		if nl.readClose(closeCh) {
			nl.close(wg)
			return
		}
		// can't do anything without a handle.  If reading the handle fails,
		// then log and drop the message.
		buf, err := readMessage(reader)
		if err != nil {
			if nl.readClose(closeCh) {
				// close() was called on the connection.
				nl.close(wg)
				return
			} else {
				// have lost connection.  reestablish connection here and let this thread exit.
				// close the connection so nore more requests.
				nl.nc.setOpen(false)
				nl.close(wg)
				nl.nc.reconnectNL()
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

func (nl *networkListener) readClose(closeCh *chan bool) bool {
	select {
	case <-*closeCh:
		return true
	case <-time.After(5 * time.Millisecond):
		return false
	}
}

func (nl *networkListener) close(wg *sync.WaitGroup) {
	defer wg.Done()
	// if connection to server is lost then there won't be a response
	// for any of the remaining requests.  Time them out.
	err := errors.New("connection lost")
	nl.requestMutex.Lock()
	defer nl.requestMutex.Unlock()
	for _, req := range nl.requests {
		req.getChan() <- err.(voltResponse)
	}
	nl.requests = make(map[int64]*networkRequest)
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
			nl.nc.dist.handleSubscribe(err.(voltResponse))
		} else {
			if rows, err := deserializeRows(r, rsp); err != nil {
				nl.nc.dist.handleSubscribe(err.(voltResponse))
			} else {
				nl.nc.dist.handleSubscribe(rows)
			}

		}
		return
	}

	// remove the associated request as soon as the handle is found.
	req := nl.removeRequest(handle)
	if req == nil {
		log.Panic("Unexpected handle", handle)
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

func (nl *networkListener) registerRequest(nc *nodeConn, pi *procedureInvocation) <-chan voltResponse {
	ch := make(chan voltResponse, 1)
	nr := newNetworkRequest(nc, ch, pi.isQuery, pi.getLen())
	nl.requestMutex.Lock()
	nl.requests[pi.handle] = nr
	nl.requestMutex.Unlock()
	return ch
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

func (nl *networkListener) start(reader io.Reader, closeCh *chan bool, wg *sync.WaitGroup) {
	go nl.listen(reader, closeCh, wg)
}

type networkRequest struct {
	nc    *nodeConn
	query bool
	ch    chan voltResponse
	// the size of the serialized request written to the server.
	numBytes int
}

func newNetworkRequest(nc *nodeConn, ch chan voltResponse, isQuery bool, numBytes int) *networkRequest {
	var nr = new(networkRequest)
	nr.nc = nc
	nr.query = isQuery
	nr.ch = ch
	nr.numBytes = numBytes
	return nr
}

func (nr *networkRequest) getNodeConn() *nodeConn {
	return nr.nc
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

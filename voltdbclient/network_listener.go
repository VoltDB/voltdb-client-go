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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// NetworkListener listens for responses for asynchronous procedure calls from
// the server.  If a callback (channel) is registered for the procedure, the
// listener puts the response on the channel (calls back).
type networkListener struct {
	nc             *nodeConn
	reader         io.Reader
	requests       map[int64]*networkRequest
	requestMutex   sync.Mutex
	wg             *sync.WaitGroup
	hasBeenStopped int32
}

func newListener(nc *nodeConn, reader io.Reader, wg *sync.WaitGroup) *networkListener {
	var nl = new(networkListener)
	nl.nc = nc
	nl.reader = reader
	nl.requests = make(map[int64]*networkRequest)
	nl.requestMutex = sync.Mutex{}
	nl.wg = wg
	nl.hasBeenStopped = 0
	return nl
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (nl *networkListener) listen() {
	for {
		if !atomic.CompareAndSwapInt32(&nl.hasBeenStopped, 0, 0) {
			nl.wg.Done()
			break
		}
		// can't do anything without a handle.  If reading the handle fails,
		// then log and drop the message.
		buf, err := readMessage(nl.reader)
		if err != nil {
			if nl.hasBeenStopped == 1 {
				// notify the stopping thread that this thread is unblocked and is exiting.
				nl.wg.Done()
			} else {
				// have lost connection.  reestablish connection here and let this thread exit.
				nl.nc.reconnect()
			}
			return
		}
		handle, err := readLong(buf)
		// can't do anything without a handle.  If reading the handle fails,
		// then log and drop the message.
		if err != nil {
			fmt.Printf("Error reading handle %v\n", err)
			continue
		}
		nl.readResponse(buf, handle)
	}
}

// reads and deserializes a response from the server.
func (nl *networkListener) readResponse(r io.Reader, handle int64) {

	rsp := deserializeResponse(r, handle)

	nl.requestMutex.Lock()
	req := nl.requests[handle]
	delete(nl.requests, handle)
	nl.requestMutex.Unlock()

	if req.isQuery() {
		rows := deserializeRows(r, rsp)
		req.getChan() <- rows
	} else {
		result := deserializeResult(r, rsp)
		req.getChan() <- result
	}
}

func (nl *networkListener) registerRequest(handle int64, isQuery bool) <-chan voltResponse {
	c := make(chan voltResponse, 1)
	nr := newNetworkRequest(c, isQuery)
	nl.requestMutex.Lock()
	nl.requests[handle] = nr
	nl.requestMutex.Unlock()
	return c
}

// need to have a lock on the map when this is invoked.
func (nl *networkListener) removeRequest(handle int64) {
	nl.requestMutex.Lock()
	delete(nl.requests, handle)
	nl.requestMutex.Unlock()
}

func (nl *networkListener) start() {
	nl.wg.Add(1)
	go nl.listen()
}

func (nl *networkListener) stop() {
	for {
		if atomic.CompareAndSwapInt32(&nl.hasBeenStopped, 0, 1) {
			break
		}
	}
}

type networkRequest struct {
	query bool
	ch    chan voltResponse
}

func newNetworkRequest(ch chan voltResponse, isQuery bool) *networkRequest {
	var nr = new(networkRequest)
	nr.query = isQuery
	nr.ch = ch
	return nr
}

func (nr networkRequest) isQuery() bool {
	return nr.query
}

func (nr networkRequest) getChan() chan voltResponse {
	return nr.ch
}

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
type NetworkListener struct {
	reader         io.Reader
	requests       map[int64]*NetworkRequest
	requestMutex   sync.Mutex
	wg             sync.WaitGroup
	hasBeenStopped int32
}

func newListener(reader io.Reader, wg sync.WaitGroup) *NetworkListener {
	var l = new(NetworkListener)
	l.reader = reader
	l.requests = make(map[int64]*NetworkRequest)
	l.requestMutex = sync.Mutex{}
	l.wg = wg
	l.hasBeenStopped = 0
	return l
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (l *NetworkListener) listen() {
	for {
		if !atomic.CompareAndSwapInt32(&l.hasBeenStopped, 0, 0) {
			l.wg.Done()
			break
		}
		// can't do anything without a handle.  If reading the handle fails,
		// then log and drop the message.
		buf, err := readMessage(l.reader)
		if err != nil {
			// TODO: log
			fmt.Printf("Error reading response %v\n", err)
			return
		}
		handle, err := readLong(buf)
		if err != nil {
			// TODO: log
			fmt.Printf("Error reading handle %v\n", err)
			continue
		}
		l.readResponse(buf, handle)
	}
}

// reads and deserializes a response from the server.
func (l *NetworkListener) readResponse(r io.Reader, handle int64) {

	rsp := deserializeResponse(r, handle)

	l.requestMutex.Lock()
	req := l.requests[handle]
	l.removeRequest(handle)
	l.requestMutex.Unlock()

	if req.isQuery() {
		rows := deserializeRows(r, rsp)
		req.getChan() <- rows
	} else {
		result := newVoltResult(rsp)
		req.getChan() <- *result
	}
}

func (l *NetworkListener) registerRequest(handle int64, isQuery bool) <-chan VoltResponse {
	c := make(chan VoltResponse, 1)
	nr := newNetworkRequest(c, isQuery)
	l.requestMutex.Lock()
	l.requests[handle] = nr
	l.requestMutex.Unlock()
	return c
}

// need to have a lock on the map when this is invoked.
func (l *NetworkListener) removeRequest(handle int64) {
	delete(l.requests, handle)
}

func (l *NetworkListener) start() {
	l.wg.Add(1)
	go l.listen()
}

func (l *NetworkListener) stop() {
	for {
		if atomic.CompareAndSwapInt32(&l.hasBeenStopped, 0, 1) {
			break
		}
	}
}

type NetworkRequest struct {
	query bool
	ch    chan VoltResponse
}

func newNetworkRequest(ch chan VoltResponse, isQuery bool) *NetworkRequest {
	var nr = new(NetworkRequest)
	nr.query = isQuery
	nr.ch = ch
	return nr
}

func (nr NetworkRequest) isQuery() bool {
	return nr.query
}

func (nr NetworkRequest) getChan() chan VoltResponse {
	return nr.ch
}

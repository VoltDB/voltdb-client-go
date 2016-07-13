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
	reader       io.Reader
	requests     map[int64]*networkRequest
	requestMutex sync.Mutex
	closeCh      *chan bool
	wg           *sync.WaitGroup
}

func newListener(nc *nodeConn, ci string, reader io.Reader, closeCh *chan bool, wg *sync.WaitGroup) *networkListener {
	var nl = new(networkListener)
	nl.nc = nc
	nl.ci = ci
	nl.reader = reader
	nl.requests = make(map[int64]*networkRequest)
	nl.closeCh = closeCh
	nl.wg = wg
	return nl
}

// the state on the writer that gets reset when the connection is lost and then re-established.
func (nl *networkListener) onReconnect(reader io.Reader, closeCh *chan bool) {
	nl.reader = reader
	nl.closeCh = closeCh
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (nl *networkListener) listen() {
	for {
		if nl.readClose() {
			nl.close()
			return
		}
		// can't do anything without a handle.  If reading the handle fails,
		// then log and drop the message.
		err := nl.readResponse(nl.reader)
		if err != nil {
			if nl.readClose() {
				nl.close()
				return
			} else {
				// have lost connection.  reestablish connection here and let this thread exit.
				nl.close()
				nl.nc.reconnectNL()
				return
			}
		}
	}
}

func (nl *networkListener) readClose() bool {
	select {
	case <-*nl.closeCh:
		return true
	case <-time.After(5 * time.Millisecond):
		return false
	}
}

func (nl *networkListener) close() {
	defer nl.wg.Done()
	// if connection to server is lost then there won't be a response
	// for any of the remaining requests.  Time them out.
	err := errors.New("connection lost")
	nl.requestMutex.Lock()
	defer nl.requestMutex.Unlock()
	for handle, req := range nl.requests {
		vr := newVoltResponseInfoError(handle, err)
		req.getChan() <- vr
	}
	nl.requests = make(map[int64]*networkRequest)
}

// reads and deserializes a response from the server.
func (nl *networkListener) readResponse(r io.Reader) error {

	// return an error and reconnect to the server for the
	// first two cases - failed to read the message.
	size, err := readMessageHdr(r)
	if err != nil {
		return err
	}
	data := make([]byte, size)
	if _, err = io.ReadFull(r, data); err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)

	// for malformed messages just drop the message, not the
	// connection
	if _, err = readByte(buf); err != nil {
		log.Printf("Dropped malformed message, bad version\n")
		return nil
	}

	handle, err := readLong(buf)
	if err != nil {
		log.Printf("Dropped malformed message, bad handle\n")
		return nil
	}

	// remove the associated request as soon as the handle is found.
	req := nl.removeRequest(handle)

	if req != nil {
		rsp := deserializeResponse(buf, handle)
		if req.isQuery() {
			rows := deserializeRows(buf, rsp)
			req.getChan() <- rows
		} else {
			result := deserializeResult(buf, rsp)
			req.getChan() <- result
		}
	}
	return nil
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

func (nl *networkListener) start() {
	go nl.listen()
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

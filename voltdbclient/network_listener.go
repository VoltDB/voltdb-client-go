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
	"time"
)

// NetworkListener listens for responses for asynchronous procedure calls from
// the server.  If a callback (channel) is registered for the procedure, the
// listener puts the response on the channel (calls back).
type networkListener struct {
	nc     *nodeConn
	ci     string
	ncPiCh <-chan *procedureInvocation
}

func newNetworkListener(nc *nodeConn, ci string, ncPiCh <-chan *procedureInvocation) *networkListener {
	var nl = new(networkListener)
	nl.nc = nc
	nl.ci = ci
	nl.ncPiCh = ncPiCh
	return nl
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (nl *networkListener) listen(reader io.Reader, responseCh chan<- *bytes.Buffer) {
	for {
		buf, err := readMessage(reader)
		if err != nil {
			if responseCh == nil {
				// exiting
				return
			} else {
				// put the error on the channel
				// the owner needs to reconnect
				return
			}
		}
		responseCh <- buf
	}
}

func (nl *networkListener) loop(writer io.Writer, piCh <-chan *procedureInvocation, responseCh <-chan *bytes.Buffer, bpCh <-chan chan bool, drainCh chan chan bool, closeCh chan chan bool) {
	// declare mutable state
	requests := make(map[int64]*networkRequest)
	ncPiCh := nl.ncPiCh
	var draining bool = false
	var drainRespCh chan bool
	var queuedBytes int = 0
	var bp bool = false

	var tci int64 = int64(DEFAULT_QUERY_TIMEOUT / 10)            // timeout check interval
	tcc := time.NewTimer(time.Duration(tci) * time.Nanosecond).C // timeout check timer channel
	for {
		// setup select cases
		if draining {
			if queuedBytes == 0 && len(nl.ncPiCh) == 0 && len(piCh) == 0 {
				drainRespCh <- true
				drainRespCh = nil
				draining = false
			}
		}
		if queuedBytes > maxQueuedBytes && ncPiCh != nil {
			ncPiCh = nil
			bp = true
		} else if ncPiCh == nil {
			ncPiCh = nl.ncPiCh
			bp = false
		}
		select {
		case respCh := <-closeCh:
			respCh <- true
			return
		case pi := <-ncPiCh:
			nl.handleProcedureInvocation(writer, pi, &requests, &queuedBytes)
		case pi := <-piCh:
			nl.handleProcedureInvocation(writer, pi, &requests, &queuedBytes)
		case resp := <-responseCh:
			handle, err := readLong(resp)
			// can't do anything without a handle.  If reading the handle fails,
			// then log and drop the message.
			if err != nil {
				continue
			}
			if handle == PING_HANDLE {
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
				nl.handleSyncResponse(handle, resp, req)
			} else {
				go nl.handleAsyncResponse(handle, resp, req)
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
					nl.handleAsyncTimeout(req)
					delete(requests, req.handle)
				}
			}
			tcc = time.NewTimer(time.Duration(tci) * time.Nanosecond).C
		}
	}
}

func (nl *networkListener) handleProcedureInvocation(writer io.Writer, pi *procedureInvocation, requests *map[int64]*networkRequest, queuedBytes *int) {
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

func (nl *networkListener) handleSyncResponse(handle int64, r io.Reader, req *networkRequest) {
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

func (nl *networkListener) handleAsyncResponse(handle int64, r io.Reader, req *networkRequest) {
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

func (nl *networkListener) handleAsyncTimeout(req *networkRequest) {
	err := errors.New("timeout")
	verr := VoltError{voltResponse: emptyVoltResponseInfo(), error: err}
	req.arc.ConsumeError(verr)
}

type networkRequest struct {
	handle int64
	query  bool
	ch     chan voltResponse
	sync   bool
	arc    AsyncResponseConsumer
	// the size of the serialized request written to the server.
	numBytes  int
	timeout   time.Duration
	submitted time.Time
}

func newSyncRequest(handle int64, ch chan voltResponse, isQuery bool, numBytes int, timeout time.Duration, submitted time.Time) *networkRequest {
	var nr = new(networkRequest)
	nr.handle = handle
	nr.query = isQuery
	nr.ch = ch
	nr.sync = true
	nr.arc = nil
	nr.numBytes = numBytes
	nr.submitted = submitted
	nr.timeout = timeout
	return nr
}

func newAsyncRequest(handle int64, ch chan voltResponse, isQuery bool, arc AsyncResponseConsumer, numBytes int, timeout time.Duration, submitted time.Time) *networkRequest {
	var nr = new(networkRequest)
	nr.handle = handle
	nr.query = isQuery
	nr.ch = ch
	nr.sync = false
	nr.arc = arc
	nr.numBytes = numBytes
	nr.submitted = submitted
	nr.timeout = timeout
	return nr
}

func (nr *networkRequest) getArc() AsyncResponseConsumer {
	return nr.arc
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

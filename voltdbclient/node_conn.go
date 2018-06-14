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
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VoltDB/voltdb-client-go/wire"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

// start back pressure when this many bytes are queued for write
const maxQueuedBytes = 262144
const maxResponseBuffer = 10000
const defaultMaxRetries = 10
const defaultRetryInterval = time.Second

type nodeConn struct {
	connInfo     string
	connData     *wire.ConnInfo
	tcpConn      *net.TCPConn
	drainCh      chan chan bool
	bpCh         chan chan bool
	responseCh   chan *bytes.Buffer
	requests     *sync.Map
	queuedBytes  int
	bp           bool
	disconnected bool
	closed       atomic.Value

	// This is the duration to wait before the next retry to connect to a node that
	// lost connection attempt
	retryInterval time.Duration

	// If true, this will try to reconnect the moment the connection is closed.
	retry bool

	// The maximum number of retries to reconnect to a disconeected node before
	// giving up.
	maxRetries int
	cancel     func()
}

func newNodeConn(ci string) *nodeConn {
	return &nodeConn{
		connInfo:   ci,
		bpCh:       make(chan chan bool),
		drainCh:    make(chan chan bool),
		responseCh: make(chan *bytes.Buffer, maxResponseBuffer),
		requests:   &sync.Map{},
	}
}

func (nc *nodeConn) submit(ctx context.Context, pi *procedureInvocation) (int, error) {
	if nc.isClosed() {
		return 0, fmt.Errorf("%s:%d writing on a closed node connection",
			nc.connInfo, pi.handle)
	}
	return nc.handleProcedureInvocation(ctx, pi)
}

func (nc *nodeConn) markClosed() error {
	nc.closed.Store(true)
	nc.requests.Range(func(k, v interface{}) bool {
		nc.requests.Delete(k)
		r := v.(*procedureInvocation)
		nc.handleMarkClosed(r)
		if r.cancel != nil {
			r.cancel()
		}
		nc.queuedBytes--
		return true
	})
	nc.cancel()
	return nc.tcpConn.Close()
}

func (nc *nodeConn) isClosed() bool {
	if v := nc.closed.Load(); v != nil {
		return v.(bool)
	}
	return false
}

func (nc *nodeConn) Close() error {
	if nc.isClosed() {
		return nil
	}
	return nc.markClosed()
}

func (nc *nodeConn) connect(ctx context.Context, protocolVersion int) error {
	tcpConn, connData, err := nc.networkConnect(protocolVersion)
	if err != nil {
		return err
	}
	nc.connData = connData
	nc.tcpConn = tcpConn
	nctx, cancel := context.WithCancel(ctx)
	nc.cancel = cancel
	go nc.listen(nctx)
	nc.drainCh = make(chan chan bool, 1)
	go nc.loop(nctx, nc.bpCh)
	return nil
}

// called when the network listener loses connection.
// the 'processAsyncs' goroutine and channel stay in place over
// a reconnect, they're not affected.
func (nc *nodeConn) reconnect(ctx context.Context, protocolVersion int) {
	if nc.retry {
		if !nc.isClosed() {
			return
		}
		maxRetries := nc.maxRetries
		if maxRetries == 0 {
			maxRetries = defaultMaxRetries
		}
		interval := nc.retryInterval
		if interval == 0 {
			interval = defaultRetryInterval
		}
		count := 0
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if count > maxRetries {
					return
				}
				tcpConn, connData, err := nc.networkConnect(protocolVersion)
				if err != nil {
					log.Println(fmt.Printf("Failed to reconnect to server %s with %s, retrying ...%d\n", nc.connInfo, err, count))
					count++
					continue
				}
				nc.tcpConn = tcpConn
				nc.connData = connData
				go nc.listen(ctx)
				go nc.loop(ctx, nc.bpCh)
				nc.closed.Store(false)
				return
			}
		}
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
	query := u.Query()
	retry := query.Get("retry")
	if retry != "" {
		r, err := strconv.ParseBool(retry)
		if err != nil {
			return nil, nil, fmt.Errorf("voltdbclient: failed to parse retry value %v", err)
		}
		nc.retry = r

		interval := query.Get("retry_interval")
		if interval != "" {
			i, err := time.ParseDuration(interval)
			if err != nil {
				return nil, nil, fmt.Errorf("voltdbclient: failed to parse retry_interval value %v", err)
			}
			nc.retryInterval = i
		}
		maxRetries := query.Get("max_retries")
		if maxRetries != "" {
			max, err := strconv.Atoi(maxRetries)
			if err != nil {
				return nil, nil, fmt.Errorf("voltdbclient: failed to parse max_retries value %v", err)
			}
			nc.maxRetries = max
		}
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

func checkConnReset(err error) bool {
	return strings.Contains(err.Error(), "read: connection reset by peer")
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (nc *nodeConn) listen(ctx context.Context) {
	d := wire.NewDecoder(nc.tcpConn)
	s := &wire.Decoder{}
	for {
		if nc.isClosed() {
			return
		}
		select {
		case <-ctx.Done():
			if !nc.isClosed() {
				nc.markClosed()
			}
			return
		default:
			b, err := d.Message()
			if err != nil {
				if checkConnReset(err) {
					if !nc.isClosed() {
						nc.markClosed()
					}
				}
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
}

func (nc *nodeConn) loop(ctx context.Context, bpCh <-chan chan bool) {
	var draining bool
	var drainRespCh chan bool
	// for ping
	for {
		if nc.isClosed() {
			return
		}
		// setup select cases
		if draining {
			if nc.queuedBytes <= 0 {
				drainRespCh <- true
				drainRespCh = nil
				draining = false
			}
		}
		select {
		case <-ctx.Done():
			if !nc.isClosed() {
				nc.markClosed()
			}
			return
		case resp := <-nc.responseCh:
			decoder := wire.NewDecoder(resp)
			handle, err := decoder.Int64()
			// can't do anything without a handle.  If reading the handle fails,
			// then log and drop the message.
			if err != nil {
				continue
			}
			r, ok := nc.requests.Load(handle)
			if !ok || r == nil {
				// there's a race here with timeout.  A request can be timed out and
				// then a response received.  In this case drop the response.
				continue
			}
			req := r.(*procedureInvocation)
			nc.queuedBytes--
			nc.requests.Delete(handle)
			if !req.async {
				nc.handleSyncResponse(handle, resp, req)
			} else {
				nc.handleAsyncResponse(handle, resp, req)
			}
		case respBPCh := <-bpCh:
			respBPCh <- nc.bp
		case drainRespCh = <-nc.drainCh:
			if nc.isClosed() {
				drainRespCh <- true
				return
			}
			draining = true
		}
	}
}

func (nc *nodeConn) handleProcedureInvocation(ctx context.Context, pi *procedureInvocation) (int, error) {
	encoder := wire.NewEncoder()
	EncodePI(encoder, pi)
	n, err := nc.tcpConn.Write(encoder.Bytes())
	if err != nil {
		if strings.Contains(err.Error(), "write: broken pipe") {
			return n, fmt.Errorf("node %s: is down", nc.connInfo)
		}
		return n, fmt.Errorf("%s: %v", nc.connInfo, err)
	}
	nc.requests.Store(pi.handle, pi)
	nc.queuedBytes++
	pi.conn = nc
	go pi.handleTimeoutsAndCancel(ctx)
	return 0, nil
}

func (nc *nodeConn) handleSyncResponse(handle int64, r io.Reader, req *procedureInvocation) {
	decoder := wire.NewDecoder(r)
	rsp, err := decodeResponse(decoder, handle)
	if err != nil {
		e := err.(VoltError)
		e.error = fmt.Errorf("%s: %v", nc.connInfo, e.error)
		req.responseCh <- e
	} else if req.isQuery {

		if rows, err := decodeRows(decoder, rsp); err != nil {
			req.responseCh <- err.(voltResponse)
		} else {
			req.responseCh <- rows
		}
	} else {
		if result, err := decodeResult(decoder, rsp); err != nil {
			req.responseCh <- err.(voltResponse)
		} else {
			req.responseCh <- result
		}
	}
	if req.cancel != nil {
		req.cancel()
	}
}

func (nc *nodeConn) handleAsyncResponse(handle int64, r io.Reader, req *procedureInvocation) {
	d := wire.NewDecoder(r)
	rsp, err := decodeResponse(d, handle)
	if err != nil {
		req.arc.ConsumeError(err)
	} else if req.isQuery {
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
	if req.cancel != nil {
		req.cancel()
	}
}

func (nc *nodeConn) handleTimeout(req *procedureInvocation) {
	err := fmt.Errorf("timeout %s %d", nc.connInfo, req.handle)
	verr := VoltError{voltResponse: emptyVoltResponseInfo(), error: err}
	if !req.async {
		req.responseCh <- verr
	} else {
		req.arc.ConsumeError(verr)
	}
	nc.queuedBytes--
	nc.requests.Delete(req.handle)
	if req.cancel != nil {
		req.cancel()
	}
}

func (nc *nodeConn) handleMarkClosed(req *procedureInvocation) {
	err := fmt.Errorf("node %s was marked as closed clearing up this request ", nc.connInfo)
	verr := VoltError{voltResponse: emptyVoltResponseInfo(), error: err}
	if !req.async {
		req.responseCh <- verr
	} else {
		req.arc.ConsumeError(verr)
	}
}

func (nc *nodeConn) Ping() error {
	return nc.sendPing()
}

func (nc *nodeConn) sendPing() error {
	pi := newProcedureInvocationByHandle(PingHandle, true, "@Ping", []driver.Value{})
	encoder := wire.NewEncoder()
	EncodePI(encoder, pi)
	_, err := nc.tcpConn.Write(encoder.Bytes())
	if err != nil {
		if strings.Contains(err.Error(), "write: broken pipe") {
			return fmt.Errorf("node %s: is down", nc.connInfo)
		}
	}
	return err
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

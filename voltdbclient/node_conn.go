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
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"database/sql/driver"
	"crypto/x509"
	"crypto/tls"
	"sync/atomic"
	"net/url"

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
	pemBytes []byte
	insecureSkipVerify bool
	connInfo     string
	connData     *wire.ConnInfo
	tcpConn      *net.TCPConn
	tlsConn      *tls.Conn
	drainCh      chan chan bool
	bpCh         chan chan bool
	closeCh      chan chan bool
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
}

func newNodeConn(ci string) *nodeConn {
	return &nodeConn{
		connInfo:   ci,
		bpCh:       make(chan chan bool),
		closeCh:    make(chan chan bool),
		drainCh:    make(chan chan bool),
		responseCh: make(chan *bytes.Buffer, maxResponseBuffer),
		requests:   &sync.Map{},
	}
}

func newNodeTLSConn(ci string, insecureSkipVerify bool, pemBytes []byte) *nodeConn {
	return &nodeConn{
		pemBytes: pemBytes,
		insecureSkipVerify: insecureSkipVerify,
		connInfo:   ci,
		bpCh:       make(chan chan bool),
		closeCh:    make(chan chan bool),
		drainCh:    make(chan chan bool),
		responseCh: make(chan *bytes.Buffer, maxResponseBuffer),
		requests:   &sync.Map{},
	}
}

func (nc *nodeConn) submit(pi *procedureInvocation) (int, error) {
	if nc.isClosed() {
		return 0, fmt.Errorf("%s:%d writing on a closed node connection",
			nc.connInfo, pi.handle)
	}
	return nc.handleProcedureInvocation(pi)
}

func (nc *nodeConn) markClosed() {
	nc.closed.Store(true)
	nc.tcpConn.Close()

	// release all stored pending requests. This connection is closed so we can't
	// satisfy the requests.
	//
	// TODO: handle the requests with connection closed error?
	nc.requests = &sync.Map{}
}

func (nc *nodeConn) isClosed() bool {
	if v := nc.closed.Load(); v != nil {
		return v.(bool)
	}
	return false
}

// when the node conn is closed by its owning distributer
func (nc *nodeConn) close() chan bool {
	respCh := make(chan bool, 1)
	nc.closeCh <- respCh
	return respCh
}

func (nc *nodeConn) connect(protocolVersion int) error {
	connInterface, connData, err := nc.networkConnect(protocolVersion)
	if err != nil {
		return err
	}

	nc.connData = connData
	nc.drainCh = make(chan chan bool, 1)

	switch c := connInterface.(type) {
	case *net.TCPConn:
		nc.tcpConn = c
		go nc.listen()
		go nc.loop()
	case *tls.Conn:
		nc.tlsConn = c
		go nc.listenTLS()
		go nc.loopTLS()
	}

	return nil
}

// called when the network listener loses connection.
// the 'processAsyncs' goroutine and channel stay in place over
// a reconnect, they're not affected.
func (nc *nodeConn) reconnect(protocolVersion int) {
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
				if err := nc.connect(protocolVersion); err != nil {
					log.Println(fmt.Printf("Failed to reconnect to server %s with %s, retrying ...%d\n", nc.connInfo, err, count))
					count++
					continue
				}
				nc.closed.Store(false)
				return
			}
		}
	}
}

func (nc *nodeConn) networkConnect(protocolVersion int) (interface{}, *wire.ConnInfo, error) {
	u, err := parseURL(nc.connInfo)
	if err != nil {
		return nil, nil, err
	}
	raddr, err := net.ResolveTCPAddr("tcp", u.Host)
	if err != nil {
		return nil, nil, fmt.Errorf("error resolving %v", nc.connInfo)
	}
	if nc.pemBytes != nil {
		roots := x509.NewCertPool()
		ok := roots.AppendCertsFromPEM(nc.pemBytes)
		if !ok {
			log.Fatal("failed to parse root certificate")
		}
		config := &tls.Config{
			RootCAs: roots,
			ServerName: "localhost",
			InsecureSkipVerify: nc.insecureSkipVerify,
		}
		conn, err := net.DialTCP("tcp", nil, raddr)
		if err != nil {
			return nil, nil, err
		}
		tlsConn := tls.Client(conn, config)
		i, err := nc.setupConn(protocolVersion, u, tlsConn)
		if err != nil {
			tlsConn.Close()
			return nil, nil, err
		}
		return tlsConn, i, nil
	}
	conn, err := net.DialTCP("tcp", nil, raddr)
	i, err := nc.setupConn(protocolVersion, u, conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, i, nil
}

func (nc *nodeConn) setupConn(protocolVersion int, u *url.URL, tcpConn io.ReadWriter) (*wire.ConnInfo, error) {
	pass, _ := u.User.Password()
	encoder := wire.NewEncoder()
	login, err := encoder.Login(protocolVersion, u.User.Username(), pass)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize login message %v", nc.connInfo)
	}
	_, err = tcpConn.Write(login)
	if err != nil {
		return nil, err
	}
	decoder := wire.NewDecoder(tcpConn)
	i, err := decoder.Login()
	if err != nil {
		return nil, fmt.Errorf("failed to login to server %v", nc.connInfo)
	}
	query := u.Query()
	retry := query.Get("retry")
	if retry != "" {
		r, err := strconv.ParseBool(retry)
		if err != nil {
			return nil, fmt.Errorf("voltdbclient: failed to parse retry value %v", err)
		}
		nc.retry = r

		interval := query.Get("retry_interval")
		if interval != "" {
			i, err := time.ParseDuration(interval)
			if err != nil {
				return nil, fmt.Errorf("voltdbclient: failed to parse retry_interval value %v", err)
			}
			nc.retryInterval = i
		}
		maxRetries := query.Get("max_retries")
		if maxRetries != "" {
			max, err := strconv.Atoi(maxRetries)
			if err != nil {
				return nil, fmt.Errorf("voltdbclient: failed to parse max_retries value %v", err)
			}
			nc.maxRetries = max
		}
	}
	return i, nil
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
		if nc.isClosed() {
			return
		}
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

func (nc *nodeConn) loop() {
	var draining bool
	var drainRespCh chan bool

	var tci = int64(DefaultQueryTimeout / 10)                    // timeout check interval
	tcc := time.NewTimer(time.Duration(tci) * time.Nanosecond).C // timeout check timer channel

	// for ping
	var pingTimeout = 2 * time.Minute
	pingSentTime := time.Now()
	var pingOutstanding bool
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

		// ping
		pingSinceSent := time.Now().Sub(pingSentTime)
		if pingOutstanding {
			if pingSinceSent > pingTimeout {
				// TODO: should disconnect
			}
		} else if pingSinceSent > pingTimeout/3 {
			nc.sendPing()
			pingOutstanding = true
			pingSentTime = time.Now()
		}

		select {
		case respCh := <-nc.closeCh:
			nc.tcpConn.Close()
			respCh <- true
			return
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
			r, ok := nc.requests.Load(handle)
			if !ok || r == nil {
				// there's a race here with timeout.  A request can be timed out and
				// then a response received.  In this case drop the response.
				continue
			}
			req := r.(*networkRequest)
			nc.queuedBytes -= req.numBytes
			nc.requests.Delete(handle)
			if req.isSync() {
				nc.handleSyncResponse(handle, resp, req)
			} else {
				nc.handleAsyncResponse(handle, resp, req)
			}

		case respBPCh := <-nc.bpCh:
			respBPCh <- nc.bp
		case drainRespCh = <-nc.drainCh:
			draining = true
		// check for timed out procedure invocations
		case <-tcc:
			nc.requests.Range(func(_, v interface{}) bool {
				req := v.(*networkRequest)
				if time.Now().After(req.submitted.Add(req.timeout)) {
					nc.queuedBytes -= req.numBytes
					nc.handleTimeout(req)
					nc.requests.Delete(req.handle)
				}
				return true
			})
			tcc = time.NewTimer(time.Duration(tci) * time.Nanosecond).C
		}
	}
}

// listenTLS listens for messages from the server and calls back a registered listener.
// listenTLS blocks on input from the server and should be run as a go routine.
func (nc *nodeConn) listenTLS() {
	d := wire.NewDecoder(nc.tlsConn)
	s := &wire.Decoder{}
	for {
		if nc.isClosed() {
			return
		}
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

func (nc *nodeConn) loopTLS() {
	var draining bool
	var drainRespCh chan bool

	var tci = int64(DefaultQueryTimeout / 10)                    // timeout check interval
	tcc := time.NewTimer(time.Duration(tci) * time.Nanosecond).C // timeout check timer channel

	// for ping
	var pingTimeout = 2 * time.Minute
	pingSentTime := time.Now()
	var pingOutstanding bool
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

		// ping
		pingSinceSent := time.Now().Sub(pingSentTime)
		if pingOutstanding {
			if pingSinceSent > pingTimeout {
				// TODO: should disconnect
			}
		} else if pingSinceSent > pingTimeout/3 {
			nc.sendPing()
			pingOutstanding = true
			pingSentTime = time.Now()
		}

		select {
		case respCh := <-nc.closeCh:
			nc.tlsConn.Close()
			respCh <- true
			return
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
			r, ok := nc.requests.Load(handle)
			if !ok || r == nil {
				// there's a race here with timeout.  A request can be timed out and
				// then a response received.  In this case drop the response.
				continue
			}
			req := r.(*networkRequest)
			nc.queuedBytes -= req.numBytes
			nc.requests.Delete(handle)
			if req.isSync() {
				nc.handleSyncResponse(handle, resp, req)
			} else {
				nc.handleAsyncResponse(handle, resp, req)
			}

		case respBPCh := <-nc.bpCh:
			respBPCh <- nc.bp
		case drainRespCh = <-nc.drainCh:
			draining = true
		// check for timed out procedure invocations
		case <-tcc:
			nc.requests.Range(func(_, v interface{}) bool {
				req := v.(*networkRequest)
				if time.Now().After(req.submitted.Add(req.timeout)) {
					nc.queuedBytes -= req.numBytes
					nc.handleTimeout(req)
					nc.requests.Delete(req.handle)
				}
				return true
			})
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
	nc.requests.Store(pi.handle, nr)
	nc.queuedBytes += pi.slen
	encoder := wire.NewEncoder()
	EncodePI(encoder, pi)
	var n int
	var err error
	if nc.tlsConn == nil {
		n, err = nc.tcpConn.Write(encoder.Bytes())
	} else {
		n, err = nc.tlsConn.Write(encoder.Bytes())
	}
	if err != nil {
		if strings.Contains(err.Error(), "write: broken pipe") {
			return n, fmt.Errorf("node %s: is down", nc.connInfo)
		}
		return n, fmt.Errorf("%s: %v", nc.connInfo, err)
	}
	pi.conn = nc
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

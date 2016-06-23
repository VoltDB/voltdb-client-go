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
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

var qHandle int64 = 0 // each query has a unique handle.

// connectionData are the values returned by a successful login.
type connectionData struct {
	hostId      int32
	connId      int64
	leaderAddr  int32
	buildString string
}

type connectionState struct {
	connInfo    string
	reader      io.Reader
	writer      io.Writer
	connData    connectionData
	asyncs      map[int64]*VoltAsyncResponse
	nl          *NetworkListener
	nlwg        *sync.WaitGroup
	isOpen      bool
}

type VoltConn struct {
	cs     *connectionState
}

func newVoltConn(connInfo string, reader io.Reader, writer io.Writer, connectionData connectionData) *VoltConn {
	var vc = new(VoltConn)

	asyncs := make(map[int64]*VoltAsyncResponse)
	wg := sync.WaitGroup{}
	nl := newListener(vc, reader, &wg)
	cs := connectionState{connInfo, reader, writer, connectionData, asyncs, nl, &wg, true}
	vc.cs = &cs
	nl.start()
	return vc
}

func (vc VoltConn) Begin() (driver.Tx, error) {
	return nil, errors.New("VoltDB does not support transactions, VoltDB autocommits")
}

func (vc VoltConn) Close() (err error) {
	if !vc.isOpen() {
		return
	}

	// stop the network listener
	vc.nl().stop()

	// close the tcp conn, will unblock the listener.
	if vc.reader() != nil {
		tcpConn := vc.reader().(*net.TCPConn)
		err = tcpConn.Close()
	}

	// network thread should return.
	vc.nlwg().Wait()

	vc.cs.isOpen = false
	return err
}

func (vc VoltConn) Reconnect() {
	var first bool = true
	for {
		if first {
			first = false
		} else {
			time.Sleep(10 * time.Microsecond)
		}
		raddr, err := net.ResolveTCPAddr("tcp", vc.cs.connInfo)
		if err != nil {
			fmt.Printf("Failed to resolve tcp address of server %s\n", err)
			continue
		}
		tcpConn, err := net.DialTCP("tcp", nil, raddr)
		if err != nil {
			fmt.Printf("Failed to connect to server %s\n", err)
			continue
		}
		login, err := serializeLoginMessage("", "")
		if err != nil {
			fmt.Printf("Failed to serialize login message %s\n", err)
			continue
		}
		writeLoginMessage(tcpConn, &login)
		if err != nil {
			fmt.Printf("Failed to writing login message to server %s\n", err)
			continue
		}
		connectionData, err := readLoginResponse(tcpConn)
		if err != nil {
			fmt.Printf("Did not receive response to login request to server%s\n", err)
			continue
		}

		asyncs := make(map[int64]*VoltAsyncResponse)
		wg := sync.WaitGroup{}
		nl := newListener(&vc, tcpConn, &wg)

		vc.cs.reader = tcpConn
		vc.cs.writer = tcpConn
		vc.cs.connData = *connectionData
		vc.cs.asyncs = asyncs
		vc.cs.nl = nl
		vc.cs.nlwg = &wg
		vc.cs.isOpen = true
		nl.start()
		break
	}
}

func OpenConn(connInfo string) (*VoltConn, error) {
	// for now, at least, connInfo is host and port.
	raddr, err := net.ResolveTCPAddr("tcp", connInfo)
	if err != nil {
		return nil, fmt.Errorf("Error resolving %v.", connInfo)
	}
	var tcpConn *net.TCPConn
	if tcpConn, err = net.DialTCP("tcp", nil, raddr); err != nil {
		return nil, err
	}
	login, err := serializeLoginMessage("", "")
	if err != nil {
		return nil, err
	}
	writeLoginMessage(tcpConn, &login)
	connData, err := readLoginResponse(tcpConn)
	if err != nil {
		return nil, err
	}
	return newVoltConn(connInfo, tcpConn, tcpConn, *connData), nil
}

func (vc VoltConn) Prepare(query string) (driver.Stmt, error) {
	stmt := newVoltStatement(&vc, query)
	return *stmt, nil
}

func (vc VoltConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if !vc.isOpen() {
		return nil, errors.New("Connection is closed")
	}
	handle := atomic.AddInt64(&qHandle, 1)
	c := vc.nl().registerRequest(handle, false)
	if err := vc.serializeQuery(vc.writer(), query, handle, args); err != nil {
		vc.nl().removeRequest(handle)
		return VoltResult{}, err
	}
	resp := <-c
	rslt := resp.(VoltResult)
	if err := rslt.getError(); err != nil {
		return nil, err
	}
	return rslt, nil
}

func (vc VoltConn) ExecAsync(query string, args []driver.Value) (*VoltAsyncResponse, error) {
	if !vc.isOpen() {
		return nil, errors.New("Connection is closed")
	}
	handle := atomic.AddInt64(&qHandle, 1)
	c := vc.nl().registerRequest(handle, false)
	vasr := newVoltAsyncResponse(vc, handle, c, false)
	vc.registerAsync(handle, vasr)
	if err := vc.serializeQuery(vc.writer(), query, handle, args); err != nil {
		vc.nl().removeRequest(handle)
		return nil, err
	}
	return vasr, nil
}

func (vc VoltConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if !vc.isOpen() {
		return nil, errors.New("Connection is closed")
	}
	handle := atomic.AddInt64(&qHandle, 1)
	c := vc.nl().registerRequest(handle, true)
	if err := vc.serializeQuery(vc.writer(), query, handle, args); err != nil {
		vc.nl().removeRequest(handle)
		return VoltRows{}, err
	}
	resp := <-c
	rows := resp.(VoltRows)
	if err := rows.getError(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (vc VoltConn) QueryAsync(query string, args []driver.Value) (*VoltAsyncResponse, error) {
	if !vc.isOpen() {
		return nil, errors.New("Connection is closed")
	}
	handle := atomic.AddInt64(&qHandle, 1)
	c := vc.nl().registerRequest(handle, true)
	vasr := newVoltAsyncResponse(vc, handle, c, true)
	vc.registerAsync(handle, vasr)
	if err := vc.serializeQuery(vc.writer(), query, handle, args); err != nil {
		vc.nl().removeRequest(handle)
		return nil, err
	}
	return vasr, nil
}

func (vc VoltConn) Drain(vasrs []*VoltAsyncResponse) {
	idxs := []int{} // index into the given slice
	cases := []reflect.SelectCase{}

	for {
		for idx, vasr := range vasrs {
			if vasr.IsActive() {
				idxs = append(idxs, idx)
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(vasr.channel())})
				if len(cases) == 100 {  // 100 at a time to limit garbage collection of slices
					break
				}
			}
		}

		if len(cases) == 0 {
			break
		}

		for len(idxs) > 0 {
			chosen, val, ok := reflect.Select(cases)

			// idiom for removing from the middle of a slice
			idx := idxs[chosen]
			idxs[chosen] = idxs[len(idxs) - 1]
			idxs = idxs[:len(idxs) - 1]

			cases[chosen] = cases[len(cases) - 1]
			cases = cases[:len(cases) - 1]

			chosenResponse := vasrs[idx]
			// if not ok, the channel was closed
			if !ok {
				chosenResponse.setError(errors.New("Result was not available, channel was closed"))
			} else {
				// check the returned value
				if val.Kind() != reflect.Interface {
					chosenResponse.setError(errors.New("unexpected return type, not an interface"))
				} else {
					rows, ok := val.Interface().(driver.Rows)
					if ok {
						vrows := rows.(VoltRows)
						if vrows.getError() != nil {
							chosenResponse.setError(vrows.getError())
						} else {
							chosenResponse.setRows(rows)
						}
						continue
					}
					rslt, ok := val.Interface().(driver.Result)
					if ok {
						vrslt := rslt.(VoltResult)
						if vrslt.getError() != nil {
							chosenResponse.setError(vrslt.getError())
						} else {
							chosenResponse.setResult(rslt)
						}
						continue
					}
					chosenResponse.setError(errors.New("unexpected return type, not driver.Rows or driver.Result"))
				}
			}
		}
	}
}

func (vc VoltConn) DrainAll() []*VoltAsyncResponse {
	vasrs := make([]*VoltAsyncResponse, len(vc.asyncs()))
	i := 0
	for _, vasr := range vc.asyncs() {
		vasrs[i] = vasr
		i++
	}
	vc.Drain(vasrs)
	return vasrs
}

func (vc VoltConn) ExecutingAsyncs() []*VoltAsyncResponse {
	// don't copy the queries themselves, but copy the list
	vasrs := make([]*VoltAsyncResponse, len(vc.asyncs()))
	i := 0
	for _, vasr := range vc.asyncs() {
		vasrs[i] = vasr
		i++
	}
	return vasrs
}

func (vc VoltConn) asyncs() map[int64]*VoltAsyncResponse {
	return vc.cs.asyncs
}

func (vc VoltConn) isOpen() bool {
	return vc.cs.isOpen
}

func (vc VoltConn) nl() *NetworkListener {
	return vc.cs.nl
}

func (vc VoltConn) nlwg() *sync.WaitGroup {
	return vc.cs.nlwg
}

func (vc VoltConn) reader() io.Reader {
	return vc.cs.reader
}

func (vc VoltConn) writer() io.Writer {
	return vc.cs.writer
}

func (vc VoltConn) registerAsync(handle int64, vasr *VoltAsyncResponse) {
	vc.asyncs()[handle] = vasr
}

func (vc VoltConn) removeAsync(han int64) {
	delete(vc.asyncs(), han)
}

func writeLoginMessage(writer io.Writer, buf *bytes.Buffer) {
	// length includes protocol version.
	length := buf.Len() + 2
	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(length))
	writeProtoVersion(&netmsg)
	writePasswordHashVersion(&netmsg)
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

type VoltAsyncResponse struct {
	conn    VoltConn
	han     int64
	ch      <-chan VoltResponse
	isQuery bool
	result  driver.Result
	rows    driver.Rows
	err     error
	active  bool
}

func newVoltAsyncResponse(conn VoltConn, han int64, ch <-chan VoltResponse, isQuery bool) *VoltAsyncResponse {
	var vasr = new(VoltAsyncResponse)
	vasr.conn = conn
	vasr.han = han
	vasr.ch = ch
	vasr.isQuery = isQuery
	vasr.active = true
	return vasr
}

func (vasr *VoltAsyncResponse) Result() (driver.Result, error) {
	if vasr.isQuery {
		return nil, errors.New("Response holds driver.Rows rather than driver.Result")
	}
	if !vasr.active {
		if vasr.err != nil {
			return nil, vasr.err
		}
		return vasr.result, nil

	} else {
		resp := <-vasr.ch
		if err := resp.getError(); err != nil {
			resp.setError(err)
			return nil, err
		}
		rslt := resp.(VoltResult)
		vasr.setResult(rslt)
		return rslt, nil
	}
}

func (vasr *VoltAsyncResponse) Rows() (driver.Rows, error) {
	if !vasr.isQuery {
		return nil, errors.New("Response holds driver.Result rather than driver.Rows")
	}
	if !vasr.active {
		if vasr.err != nil {
			return nil, vasr.err
		}
		return vasr.rows, nil

	} else {
		resp := <-vasr.ch
		if err := resp.getError(); err != nil {
			resp.setError(err)
			return nil, err
		}
		rows := resp.(VoltRows)
		vasr.setRows(rows)
		return rows, nil
	}
}

func (vasr *VoltAsyncResponse) channel() <-chan VoltResponse {
	return vasr.ch
}

func (vasr *VoltAsyncResponse) handle() int64 {
	return vasr.han
}

func (vasr *VoltAsyncResponse) IsActive() bool {
	return vasr.active
}

func (vasr *VoltAsyncResponse) IsQuery() bool {
	return vasr.isQuery
}

func (vasr *VoltAsyncResponse) setError(err error) {
	vasr.err = err
	vasr.active = false
	vasr.conn.removeAsync(vasr.han)
}

func (vasr *VoltAsyncResponse) setResult(result driver.Result) {
	vasr.result = result
	vasr.active = false
	vasr.conn.removeAsync(vasr.han)
}

func (vasr *VoltAsyncResponse) setRows(rows driver.Rows) {
	vasr.rows = rows
	vasr.active = false
	vasr.conn.removeAsync(vasr.han)
}

func (vc VoltConn) serializeQuery(writer io.Writer, procedure string, handle int64, args []driver.Value) error {

	var call bytes.Buffer
	var err error

	// Serialize the procedure call and its params.
	// Use 0 for handle; it's not necessary in pure sync client.
	if call, err = serializeStatement(procedure, handle, args); err != nil {
		return err
	}

	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(call.Len()))
	io.Copy(&netmsg, &call)
	io.Copy(writer, &netmsg)
	return nil
}

// Null Value type
type NullValue struct {
	colType int8
}

func NewNullValue(colType int8) *NullValue {
	var nv = new(NullValue)
	nv.colType = colType
	return nv
}

func (nv *NullValue) ColType() int8 {
	return nv.colType
}

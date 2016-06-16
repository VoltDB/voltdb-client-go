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
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
)

// connectionData are the values returned by a successful login.
type connectionData struct {
	hostId      int32
	connId      int64
	leaderAddr  int32
	buildString string
}

type VoltConn struct {
	reader      io.Reader
	writer      io.Writer
	connData    *connectionData
	execs       map[int64]<-chan driver.Result
	queries     map[int64]<-chan driver.Rows
	netListener *NetworkListener
	isOpen      bool
}

func newVoltConn(reader io.Reader, writer io.Writer, connData *connectionData) *VoltConn {
	var vc = new(VoltConn)
	vc.reader = reader
	vc.writer = writer
	vc.execs = make(map[int64]<-chan driver.Result)
	vc.queries = make(map[int64]<-chan driver.Rows)
	vc.netListener = NewListener(reader)
	vc.netListener.start()
	vc.isOpen = true
	return vc
}

func (vc VoltConn) Begin() (driver.Tx, error) {
	return nil, errors.New("VoltDB does not support transactions, VoltDB autocommits")
}

func (vc VoltConn) Close() (err error) {
	if vc.reader != nil {
		tcpConn := vc.reader.(*net.TCPConn)
		err = tcpConn.Close()
	}
	vc.reader = nil
	vc.writer = nil
	vc.connData = nil
	vc.isOpen = false
	return err
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
	return newVoltConn(tcpConn, tcpConn, connData), nil
}

func (vc VoltConn) Prepare(query string) (driver.Stmt, error) {
	if !vc.isOpen {
		return nil, errors.New("Connection is closed")
	}
	vs := newVoltStatement(&vc, &vc.writer, vc.netListener, query)
	return *vs, nil
}

func (vc VoltConn) HasExecutingStatements() bool {
	return len(vc.execs) != 0 || len(vc.queries) != 0
}

func (vc VoltConn) StatementResult() (driver.Rows, error) {
	handles := []int64{}
	chs := []<-chan driver.Rows{}
	for handle, ch := range vc.queries {
		handles = append(handles, handle)
		chs = append(chs, ch)
	}

	cases := make([]reflect.SelectCase, len(chs))
	for i, ch := range chs {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}
	chosen, val, ok := reflect.Select(cases)
	handle := handles[chosen]
	delete(vc.queries, handle)

	// if not ok, the channel was closed
	if !ok {
		return nil, errors.New("Result was not available, channel was closed")
	}

	// check the returned value
	if val.Kind() != reflect.Interface {
		return nil, errors.New("unexpected return type, not an interface")
	}
	rows, ok := val.Interface().(driver.Rows)
	if !ok {
		return nil, errors.New("unexpected return type, not driver.Rows")
	}
	return rows, nil
}

func (vc VoltConn) registerExec(handle int64, c <-chan driver.Result) {
	vc.execs[handle] = c
}

func (vc VoltConn) registerQuery(handle int64, c <-chan driver.Rows) {
	vc.queries[handle] = c
}

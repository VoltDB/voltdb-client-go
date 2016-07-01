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

// voltdbclient provides a go client for VoltDB.  voltdbclient
// implements the interfaces specified in the database/sql/driver
// package.  The interfaces specified by database/sql/driver are
// typically used to implement a driver to be used by the
// database/sql package.
//
// In this case, though, voltdbclient can also be used as a
// standalone client.  That is, the code in voltdbclient
// can be used both to implement a voltdb driver for database/sql
// and as a standalone VoltDB client.
//
// voltdbclient supports an asynchronous api as well as the
// standard database/sql/driver api.  The asynchronous api is supported
// by the VoltConn and VoltStatement types.
package voltdbclient

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"
)

const (
	QUERY_TIMEOUT time.Duration = 120 // seconds
)

// VoltConn represents a connection to VoltDB that can be used to execute
// queries and other statements.  A VoltConn is initially created with a call
// to OpenConn.
//
// A VoltConn is a driver.Conn; VoltConn also supports an asynchronous api.
//
// The creation of a VoltConn represents the creation of two associated
// goroutines.  One of these is used to listen for responses from the VoltDB
// server.  The other is used to process responses to asynchronous requests.
//
// A VoltConn should not be shared among goroutines; this is true of
// driver.Conn as well.  But a client can create many instances of a VoltConn.
type VoltConn struct {
	*activeConns
	cis []string
}

func newVoltConn(cis []string) *VoltConn {
	var vc = new(VoltConn)
	vc.cis = cis
	vc.activeConns = newActiveConns()
	return vc
}

// OpenConn returns a new connection to the VoltDB server.  The name is a
// string in a driver-specific format.  The returned connection can be used by
// only one goroutine at a time.
func OpenConn(cis []string) (*VoltConn, error) {
	vc := newVoltConn(cis)

	for _, ci := range cis {
		err := vc.openNodeConn(ci)
		// if fail to initially open a connection, continue to
		// retry in the background
		if err != nil {
			log.Printf("Failed to connect to host %v with %v\n", ci, err)
			go vc.reconnect(ci) // the goroutine exits when reconnect succeeds.
		}
	}
	if vc.numConns() == 0 {
		return nil, errors.New("Failed to connect to a VoltDB server")
	}
	return vc, nil
}

func (vc VoltConn) openNodeConn(ci string) error {
	// for now, at least, connInfo is host and port.
	raddr, err := net.ResolveTCPAddr("tcp", ci)
	if err != nil {
		return fmt.Errorf("Error resolving %v.", ci)
	}
	var tcpConn *net.TCPConn
	if tcpConn, err = net.DialTCP("tcp", nil, raddr); err != nil {
		return err
	}
	login, err := serializeLoginMessage("", "")
	if err != nil {
		return err
	}
	writeLoginMessage(tcpConn, &login)
	if err != nil {
		return err
	}
	connData, err := readLoginResponse(tcpConn)
	if err != nil {
		return err
	}
	nc := newNodeConn(&vc, ci, tcpConn, tcpConn, *connData)
	vc.addConn(nc)
	return nil
}

func (vc VoltConn) reconnect(ci string) {
	for {
		err := vc.openNodeConn(ci)
		if err == nil {
			log.Printf("reconnected %v\n", ci)
			break
		} else {
			log.Printf("Failed to connect to host %v with %v retrying...\n", ci, err)
			time.Sleep(1 * time.Second)
		}
	}
}

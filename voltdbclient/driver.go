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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
)

// A database/sql/driver for VoltDB.  This driver is registered as 'voltdb'
type VoltDriver struct {
}

// News an instance of a VoltDB driver.
func NewVoltDriver() *VoltDriver {
	var vd = new(VoltDriver)
	return vd
}

// Open a connection to the VoltDB server.
func (vd *VoltDriver) Open(hostAndPort string) (driver.Conn, error) {
	raddr, err := net.ResolveTCPAddr("tcp", hostAndPort)
	if err != nil {
		return nil, fmt.Errorf("Error resolving %v.", hostAndPort)
	}
	var tcpConn *net.TCPConn
	if tcpConn, err = net.DialTCP("tcp", nil, raddr); err != nil {
		return nil, err
	}
	// TODO: Usename and password end up here somehow.
	login, err := serializeLoginMessage("", "")
	if err != nil {
		return nil, err
	}
	writeLoginMessage(tcpConn, &login)

	connData, err := readLoginResponse(tcpConn)
	if err != nil {
		return nil, err
	}
	voltConn := newVoltConn(hostAndPort, tcpConn, tcpConn, *connData)
	return *voltConn, nil
}

func init() {
	sql.Register("voltdb", &VoltDriver{})
}

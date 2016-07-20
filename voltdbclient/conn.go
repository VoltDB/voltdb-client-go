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
	"time"
)

const (
	// Default time out for queries.
	DEFAULT_QUERY_TIMEOUT time.Duration = 2 * time.Minute
	// TODO No timeout for system procedure call?
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
	*distributer
}

func newVoltConn(cis []string) *VoltConn {
	var vc = new(VoltConn)
	vc.distributer = newDistributer()
	return vc
}

func newVoltConnWithLatencyTarget(cis []string, latencyTarget int32) *VoltConn {
	var vc = new(VoltConn)
	vc.distributer = newDistributerWithLatencyTarget(latencyTarget)
	return vc
}

func newVoltConnWithMaxOutstandingTxns(cis []string, maxOutTxns int) *VoltConn {
	var vc = new(VoltConn)
	vc.distributer = newDistributerWithMaxOutstandingTxns(maxOutTxns)
	return vc
}

// OpenConn returns a new connection to the VoltDB server.  The name is a
// string in a driver-specific format.  The returned connection can be used by
// only one goroutine at a time.
func OpenConn(cis []string) (*VoltConn, error) {
	vc := newVoltConn(cis)
	err := vc.makeConns(cis)
	if err != nil {
		return nil, err
	}
	return vc, nil
}

func OpenConnWithLatencyTarget(cis []string, latencyTarget int32) (*VoltConn, error) {
	vc := newVoltConnWithLatencyTarget(cis, latencyTarget)
	err := vc.makeConns(cis)
	if err != nil {
		return nil, err
	}
	return vc, nil
}

func OpenConnWithMaxOutstandingTxns(cis []string, maxOutTxns int) (*VoltConn, error) {
	vc := newVoltConnWithMaxOutstandingTxns(cis, maxOutTxns)
	err := vc.makeConns(cis)
	if err != nil {
		return nil, err
	}
	return vc, nil
}

func (vc *VoltConn) makeConns(cis []string) error {
	ncs := make([]*nodeConn, len(cis))
	for i, ci := range cis {
		nc := newNodeConn(ci, vc.distributer)
		ncs[i] = nc
		err := nc.connect()
		if err != nil {
			return err
		}
		if vc.distributer.useClientAffinity {
			vc.distributer.hostIdToConnection[int(nc.connData.hostId)] = nc
		}
	}
	vc.setConns(ncs)
	if vc.distributer.useClientAffinity {
		vc.distributer.subscribeToNewNode()
	}

	return nil

}

/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
	"errors"
	"io"
)

// A helper for protocol-level de/serialization code. For
// example, serialize and write a procedure call to the network.

// configures conn with server's advertisement.
func deserializeLoginResponse(r io.Reader) (connData *connectionData, err error) {
	// Authentication result code	Byte	 1	 Basic
	// Server Host ID	            Integer	 4	 Basic
	// Connection ID	            Long	 8	 Basic
	// Cluster start timestamp  	Long	 8	 Basic
	// Leader IPV4 address	        Integer	 4	 Basic
	// Build string	 String	        variable	 Basic
	ok, err := readByte(r)
	if err != nil {
		return
	}
	if ok != 0 {
		return nil, errors.New("Authentication failed.")
	}

	hostID, err := readInt(r)
	if err != nil {
		return
	}

	connID, err := readLong(r)
	if err != nil {
		return
	}

	_, err = readLong(r)
	if err != nil {
		return
	}

	leaderAddr, err := readInt(r)
	if err != nil {
		return
	}

	buildString, err := readString(r)
	if err != nil {
		return
	}

	connData = new(connectionData)
	connData.hostID = hostID
	connData.connID = connID
	connData.leaderAddr = leaderAddr
	connData.buildString = buildString
	return connData, nil
}

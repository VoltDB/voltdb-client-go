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

import "math"

const (
	PING_HANDLE       = math.MaxInt64
	ASYNC_TOPO_HANDLE = PING_HANDLE - 1
)

const (
	PARTITIONID_BITS = 14

	// maximum values for the txn id fields
	PARTITIONID_MAX_VALUE = (1 << PARTITIONID_BITS) - 1
	MP_INIT_PID           = PARTITIONID_MAX_VALUE
)

// Hash Type
const (
	ELASTIC = "ELASTIC"
)

// Hash Config Format
const (
	BINARAY_FORMAT = 0
	JSON_FORMAT    = 1
)

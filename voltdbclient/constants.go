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

import "math"

// Handles
const (
	PingHandle      = math.MaxInt64
	AsyncTopoHandle = PingHandle - 1
)

// Partitions
const (
	PartitionIDBits = 14

	// maximum values for the txn id fields
	PartitionIDMaxValue = (1 << PartitionIDBits) - 1
	MPInitPID           = PartitionIDMaxValue
)

// Hash Type
const (
	Elastic = "ELASTIC"
)

// Hash Config Format
const (
	BinArrayFormat = 0
	JSONFormat     = 1
)

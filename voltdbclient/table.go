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

// Table represents a single result set for a stored procedure invocation.
type VoltTable struct {
	statusCode int8
	rows       VoltRows
}

func NewVoltTable(clientHandle int64, appStatus int8, appStatusString string, clusterRoundTripTime int32,
columnCount int16, columnTypes []int8, columnNames []string, rowCount int32, rows [][]byte) *VoltTable {
	var vt = new(VoltTable)
	vt.rows = *NewVoltTableRow(clientHandle, appStatus, appStatusString, clusterRoundTripTime, columnCount, columnTypes, columnNames, rowCount, rows)
	return vt
}

func (vt *VoltTable) Rows() VoltRows {
	return vt.rows
}

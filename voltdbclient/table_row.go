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

type VoltTableRow struct {
	vt *VoltTable
}

func NewVoltTableRow(table *VoltTable) *VoltTableRow {
	var vtr = new(VoltTableRow)
	vtr.vt = table
	return vtr
}

func (vtr *VoltTableRow) AdvanceRow() bool {
	return vtr.vt.AdvanceRow()
}

func (vtr *VoltTableRow) AdvanceToRow(rowIndex int32) bool {
	return vtr.vt.AdvanceToRow(rowIndex)
}

func (vtr *VoltTableRow) GetString(colIndex int16) (string, error) {
	return vtr.vt.GetString(colIndex)
}

func (vtr *VoltTableRow) GetVarbinary(colIndex int16) ([]byte, error) {
	return vtr.vt.GetVarbinary(colIndex)
}

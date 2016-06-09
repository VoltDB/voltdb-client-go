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

func (vtr *VoltTableRow) GetBigInt(colIndex int16) (interface{}, error) {
	return vtr.vt.GetBigInt(colIndex)
}

func (vtr *VoltTableRow) GetBigIntByName(cn string) (interface{}, error) {
	return vtr.vt.GetBigIntByName(cn)
}

func (vtr *VoltTableRow) GetDecimal(colIndex int16) (interface{}, error) {
	return vtr.vt.GetDecimal(colIndex)
}

func (vtr *VoltTableRow) GetDecimalByName(cn string) (interface{}, error) {
	return vtr.vt.GetDecimalByName(cn)
}

func (vtr *VoltTableRow) GetFloat(colIndex int16) (interface{}, error) {
	return vtr.vt.GetFloat(colIndex)
}

func (vtr *VoltTableRow) GetFloatByName(cn string) (interface{}, error) {
	return vtr.vt.GetFloatByName(cn)
}

func (vtr *VoltTableRow) GetInteger(colIndex int16) (interface{}, error) {
	return vtr.vt.GetInteger(colIndex)
}

func (vtr *VoltTableRow) GetIntegerByName(cn string) (interface{}, error) {
	return vtr.vt.GetIntegerByName(cn)
}

func (vtr *VoltTableRow) GetSmallInt(colIndex int16) (interface{}, error) {
	return vtr.vt.GetSmallInt(colIndex)
}

func (vtr *VoltTableRow) GetSmallIntByName(cn string) (interface{}, error) {
	return vtr.vt.GetSmallIntByName(cn)
}

func (vtr *VoltTableRow) GetString(colIndex int16) (interface{}, error) {
	return vtr.vt.GetString(colIndex)
}

func (vtr *VoltTableRow) GetStringByName(cn string) (interface{}, error) {
	return vtr.vt.GetStringByName(cn)
}

func (vtr *VoltTableRow) GetTimestamp(colIndex int16) (interface{}, error) {
	return vtr.vt.GetTimestamp(colIndex)
}

func (vtr *VoltTableRow) GetTimestampByName(cn string) (interface{}, error) {
	return vtr.vt.GetTimestampByName(cn)
}

func (vtr *VoltTableRow) GetTinyInt(colIndex int16) (interface{}, error) {
	return vtr.vt.GetTinyInt(colIndex)
}

func (vtr *VoltTableRow) GetTinyIntByName(cn string) (interface{}, error) {
	return vtr.vt.GetTinyIntByName(cn)
}

func (vtr *VoltTableRow) GetVarbinary(colIndex int16) (interface{}, error) {
	return vtr.vt.GetVarbinary(colIndex)
}

func (vtr *VoltTableRow) GetVarbinaryByName(cn string) (interface{}, error) {
	return vtr.vt.GetVarbinaryByName(cn)
}

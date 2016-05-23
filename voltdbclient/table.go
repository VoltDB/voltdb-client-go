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
	"bytes"
	"fmt"
)

const INVALID_ROW_INDEX = -1

// Table represents a single result set for a stored procedure invocation.
type VoltTable struct {
	statusCode    int8
	columnCount   int16
	columnTypes   []int8
	columnNames   []string
	columnOffsets [][]int32
	rowCount      int32
	rows          [][]byte
	rowIndex      int32
	readers       []*bytes.Reader
}

func NewVoltTable(statusCode int8, columnCount int16, columnTypes []int8, columnNames []string, rowCount int32, rows [][]byte) *VoltTable {
	var vt = new(VoltTable)
	vt.statusCode = statusCode
	vt.columnCount = columnCount
	vt.columnTypes = columnTypes
	vt.columnNames = columnNames
	vt.columnOffsets = make([][]int32, rowCount)
	vt.rowCount = rowCount
	vt.rows = rows
	vt.rowIndex = INVALID_ROW_INDEX
	vt.readers = make([]*bytes.Reader, rowCount)
	return vt
}

func (vt *VoltTable) AdvanceRow() bool {
	return vt.AdvanceToRow(vt.rowIndex + 1)
}

func (vt *VoltTable) AdvanceToRow(rowIndex int32) bool {
	vt.rowIndex = rowIndex
	if vt.rowIndex >= vt.rowCount {
		return false
	}
	return true
}

func (vt *VoltTable) ColumnCount() int {
	return int(vt.columnCount)
}

func (vt *VoltTable) ColumnNames() []string {
	rv := make([]string, 0)
	rv = append(rv, vt.columnNames...)
	return rv
}

func (vt *VoltTable) ColumnTypes() []int8 {
	rv := make([]int8, 0)
	rv = append(rv, vt.columnTypes...)
	return rv
}

func (vt *VoltTable) FetchRow(rowIndex int32) (*VoltTableRow, error) {
	if rowIndex >= vt.rowCount {
		return nil, fmt.Errorf("row index %v is out of bounds, there are %v rows", rowIndex, vt.rowCount)
	}
	vt.rowIndex = rowIndex
	tr := NewVoltTableRow(vt)
	return tr, nil
}

func (vt *VoltTable) GetString(colIndex int16) (string, error) {
	if colIndex >= vt.columnCount {
		return "", fmt.Errorf("column index %v is out of bounds, there are %v rows", colIndex, vt.columnCount)
	}
	return vt.getString(vt.rowIndex, colIndex)
}

func (vt *VoltTable) GetVarbinary(colIndex int16) ([]byte, error) {
	if colIndex >= vt.columnCount {
		return nil, fmt.Errorf("column index %v is out of bounds, there are %v rows", colIndex, vt.columnCount)
	}
	return vt.getVarbinary(vt.rowIndex, colIndex)
}

func (vt *VoltTable) GoString() string {
	return fmt.Sprintf("Table: statusCode: %v, columnCount: %v, "+
		"rowCount: %v\n", vt.statusCode, vt.columnCount,
		vt.rowCount)
}

// HasNext returns true of there are additional rows to read.
func (vt *VoltTable) HasNext() bool {
	return vt.rowIndex+1 < vt.rowCount
}

// Rowcount returns the number of rows returned by the server for this table.
func (vt *VoltTable) RowCount() int {
	return int(vt.rowCount)
}

func (vt *VoltTable) StatusCode() int {
	return int(vt.statusCode)
}

// private

func (vt *VoltTable) getReader(rowIndex int32) *bytes.Reader {
	r := vt.readers[rowIndex]
	if r == nil {
		r = bytes.NewReader(vt.rows[rowIndex])
		vt.readers[rowIndex] = r
	}
	return r
}

func (vt *VoltTable) getString(rowIndex int32, columnIndex int16) (string, error) {
	r := vt.getReader(rowIndex)
	if columnIndex == 0 {
		return readStringAt(r, 0)
	}
	offsets := vt.getOffsetsForRow(rowIndex)
	return readStringAt(r, int64(offsets[columnIndex]))
}

func (vt *VoltTable) getVarbinary(rowIndex int32, columnIndex int16) ([]byte, error) {
	r := vt.getReader(rowIndex)
	if columnIndex == 0 {
		return readByteArrayAt(r, 0)
	}
	offsets := vt.getOffsetsForRow(rowIndex)
	return readByteArrayAt(r, int64(offsets[columnIndex]))
}

func (vt *VoltTable) getOffsetsForRow(rowIndex int32) []int32 {
	offsets := vt.columnOffsets[rowIndex]
	if offsets == nil {
		return vt.setOffsetsForRow(rowIndex)
	}
	return offsets
}

func (vt *VoltTable) setOffsetsForRow(rowIndex int32) []int32 {
	offsets := make([]int32, vt.columnCount)
	r := vt.getReader(rowIndex)
	var colIndex int16 = 0
	var offset int32 = 0
	for {
		offsets[colIndex] = offset
		colIndex++
		if colIndex >= vt.columnCount {
			break
		}
		len, _ := readIntAt(r, int64(offset))
		offset += (len + 4)
	}
	return offsets
}

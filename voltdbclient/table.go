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
var NULL_DECIMAL = [...]byte {128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
var NULL_TIMESTAMP = [...]byte {128, 0, 0, 0, 0, 0, 0, 0}

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
	cnToCi        map[string]int16
}

func NewVoltTable(statusCode int8, columnCount int16, columnTypes []int8, columnNames []string, rowCount int32, rows [][]byte) *VoltTable {
	var vt = new(VoltTable)
	vt.statusCode = statusCode
	vt.columnCount = columnCount
	vt.columnTypes = columnTypes
	vt.columnNames = columnNames
	// rowCount +1 because want to represent the end of the data as an offset
	// so we can know the ending index of the last column.
	vt.columnOffsets = make([][]int32, rowCount + 1)
	vt.rowCount = rowCount
	vt.rows = rows
	vt.rowIndex = INVALID_ROW_INDEX
	vt.readers = make([]*bytes.Reader, rowCount)

	// store columnName to columnIndex
	vt.cnToCi = make(map[string]int16)
	for ci, cn := range columnNames {
		vt.cnToCi[cn] = int16(ci)
	}

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

// the common logic for reading a column is here.  Read a column as bytes and
// the represent it as the correct type.
func (vt *VoltTable) getBytes(rowIndex int32, columnIndex int16) ([]byte, error) {
	offsets, err := vt.getOffsetsForRow(rowIndex)
	if err != nil {
		return nil, err
	}
	return vt.rows[rowIndex][offsets[columnIndex]:offsets[columnIndex+1]], nil
}

func (vt *VoltTable) getString(rowIndex int32, columnIndex int16) (string, error) {
	b, err := vt.getBytes(rowIndex, columnIndex)
	if err != nil {
		return "", err
	}
	fmt.Printf("XXX bytes %d\n", len(b))
	r := vt.getReader(rowIndex)
	if columnIndex == 0 {
		return readStringAt(r, 0)
	}
	offsets, err := vt.getOffsetsForRow(rowIndex)
	if err != nil {
		return "", err
	}
	for i, v := range offsets {
		fmt.Printf("XXX offsets %d %v\n", i, v)
	}
	fmt.Printf("XXX offset is %v\n", offsets[columnIndex])
	return readStringAt(r, int64(offsets[columnIndex]))
}

func (vt *VoltTable) getVarbinary(rowIndex int32, columnIndex int16) ([]byte, error) {
	r := vt.getReader(rowIndex)
	if columnIndex == 0 {
		return readByteArrayAt(r, 0)
	}
	offsets, err := vt.getOffsetsForRow(rowIndex)
	if (err != nil) {
		return nil, err
	}
	return readByteArrayAt(r, int64(offsets[columnIndex]))
}

func (vt *VoltTable) getOffsetsForRow(rowIndex int32) ([]int32, error) {
	offsets := vt.columnOffsets[rowIndex]
	if offsets != nil {
		return offsets, nil
	}
	offsets, err := vt.calcOffsetsForRow(rowIndex)
	if (err != nil) {
		return nil, err
	}
	vt.columnOffsets[rowIndex] = offsets
	return offsets, nil
}

func (vt *VoltTable) calcOffsetsForRow(rowIndex int32) ([]int32, error) {
	// column count + 1, want starting and ending index for every column
	offsets := make([]int32, vt.columnCount + 1)
	r := vt.getReader(rowIndex)
	var colIndex int16 = 0
	var offset int32 = 0
	offsets[0] = 0
	for ; colIndex < vt.columnCount; colIndex++ {
		len, err := colLength(r, offset, vt.columnTypes[colIndex])
		if err != nil {
			return nil, err
		}
		offset += len
		offsets[colIndex + 1] = offset

	}
	return offsets, nil
}

func colLength(r *bytes.Reader, offset int32, colType int8) (int32, error) {
	switch colType {
	case -99:  // ARRAY
		return 0, fmt.Errorf("Not supporting ARRAY")
	case 1:  // NULL
		return 0, nil
	case 3:  // TINYINT
		return 1, nil
	case 4:  // SMALLINT
		return 2, nil
	case 5:  // INTEGER
		return 4, nil
	case 6:  // BIGINT
		return 8, nil
	case 8:  // FLOAT
		return 8, nil
	case 9:  // STRING
		len, err := readIntAt(r, int64(offset))
		if err != nil {
			return 0, err
		}
		if len == -1 { // encoding for null string.
			return 4, nil
		}
		return len + 4, nil
	case 11:  // TIMESTAMP
		return 8, nil
	case 22:  // DECIMAL
		return 16, nil
	case 25:  // VARBINARY
		len, err := readIntAt(r, int64(offset))
		if err != nil {
			return 0, err
		}
		if len == -1 { // encoding for null.
			return 4, nil
		}
		return len + 4, nil
	case 26:  // GEOGRAPHY_POINT
		return 0, fmt.Errorf("Not supporting GEOGRAPHY_POINT")
	case 27:  // GEOGRAPHY
		return 0, fmt.Errorf("Not supporting GEOGRAPHY")
	default:
		return 0, fmt.Errorf("Unexpected type %d", colType)
	}
}

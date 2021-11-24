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

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/VoltDB/voltdb-client-go/wire"
)

const invalidRowIndex = -1

// Table represents a single result set for a stored procedure invocation.
type voltTable struct {
	columnCount int16
	columnTypes []int8
	columnNames []string
	numRows     int32
	rows        [][]byte
	rowIndex    int32
	cnToCi      map[string]int16
	// offsets for the current rows.
	columnOffsets []int32
}

func newVoltTable(columnCount int16, columnTypes []int8, columnNames []string, rowCount int32, rows [][]byte) *voltTable {
	var vt = &voltTable{
		columnCount: columnCount,
		columnTypes: columnTypes,
		columnNames: columnNames,
		numRows:     rowCount,
		rows:        rows,
		rowIndex:    invalidRowIndex,
		cnToCi:      make(map[string]int16),
	}

	// store columnName to columnIndex
	for ci, cn := range columnNames {
		vt.cnToCi[strings.ToUpper(cn)] = int16(ci)
	}
	return vt
}

func (vt *voltTable) advanceRow() bool {
	return vt.advanceToRow(vt.rowIndex + 1)
}

func (vt *voltTable) advanceToRow(rowIndex int32) bool {
	if rowIndex >= vt.numRows {
		return false
	}
	// the current column offsets are no longer valid if the row
	// pointer moves.
	vt.columnOffsets = nil
	vt.rowIndex = rowIndex
	return true
}

// the common logic for reading a column is here.  Read a column as bytes and
// the represent it as the correct type.
func (vt *voltTable) calcOffsets() error {
	// column count + 1, want starting and ending index for every column
	offsets := make([]int32, vt.columnCount+1)
	r := bytes.NewReader(vt.rows[vt.rowIndex])
	var colIndex int16
	var offset int32
	offsets[0] = 0
	for ; colIndex < vt.columnCount; colIndex++ {
		len, err := vt.colLength(r, offset, vt.columnTypes[colIndex])
		if err != nil {
			return err
		}
		offset += len
		offsets[colIndex+1] = offset

	}
	vt.columnOffsets = offsets
	return nil
}

func (vt *voltTable) colLength(r *bytes.Reader, offset int32, colType int8) (int32, error) {
	a := wire.NewDecoderAt(r)
	switch colType {
	case -99: // ARRAY
		return 0, fmt.Errorf("Not supporting ARRAY")
	case 1: // NULL
		return 0, nil
	case 3: // TINYINT
		return 1, nil
	case 4: // SMALLINT
		return 2, nil
	case 5: // INTEGER
		return 4, nil
	case 6: // BIGINT
		return 8, nil
	case 8: // FLOAT
		return 8, nil
	case 9: // STRING
		strlen, err := a.Int32At(int64(offset))
		if err != nil {
			return 0, err
		}
		if strlen == -1 { // encoding for null string.
			return 4, nil
		}
		return strlen + 4, nil
	case 11: // TIMESTAMP
		return 8, nil
	case 22: // DECIMAL
		return 16, nil
	case 25: // VARBINARY
		strlen, err := a.Int32At(int64(offset))
		if err != nil {
			return 0, err
		}
		if strlen == -1 { // encoding for null.
			return 4, nil
		}
		return strlen + 4, nil
	case 26: // GEOGRAPHY_POINT
		return 0, fmt.Errorf("Not supporting GEOGRAPHY_POINT")
	case 27: // GEOGRAPHY
		return 0, fmt.Errorf("Not supporting GEOGRAPHY")
	default:
		return 0, fmt.Errorf("Unexpected type %d", colType)
	}
}

func (vt *voltTable) getBytes(rowIndex int32, columnIndex int16) ([]byte, error) {
	if vt.columnOffsets == nil {
		err := vt.calcOffsets()
		if err != nil {
			return nil, err
		}
	}
	return vt.rows[rowIndex][vt.columnOffsets[columnIndex]:vt.columnOffsets[columnIndex+1]], nil
}

func (vt *voltTable) getColumnCount() int {
	return int(vt.columnCount)
}

func (vt *voltTable) getColumnTypes() []int8 {
	return vt.columnTypes
}

func (vt *voltTable) getRowCount() int {
	return int(vt.numRows)
}

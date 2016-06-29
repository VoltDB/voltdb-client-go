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
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strings"
	"time"
)

var null_decimal = [...]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
var null_timestamp = [...]byte{128, 0, 0, 0, 0, 0, 0, 0}

// VoltRows is an implementation of database/sql/driver.Rows.
//
// A response to a query from the VoltDB server might include rows from more
// than one table; VoltRows includes methods used to move between tables.
//
// VoltRows also includes two column accessors for each VoltDB column type.
// The value for a column can be accessed by either column index or by column
// name.  These accessors return interface{} type; the returned interface
// needs to be cast to the correct type.  This is how null database values are
// supported, a null value will be returned as nil.
type VoltRows struct {
	voltResponse
	tables     []*voltTable
	tableIndex int16
}

func newVoltRows(resp voltResponse, tables []*voltTable) *VoltRows {
	var vr = new(VoltRows)
	vr.voltResponse = resp
	vr.tables = tables
	if len(tables) == 0 {
		vr.tableIndex = -1
	} else {
		vr.tableIndex = 0
	}
	return vr
}

// Close is essentially a no op as the VoltDB server doesn't support cursors.
func (vr VoltRows) Close() error {
	return nil
}

// Columns returns the names of the columns.
func (vr VoltRows) Columns() []string {
	rv := make([]string, 0)
	if vr.isValidTable() {
		rv = append(rv, vr.table().columnNames...)
	}
	return rv
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
func (vr VoltRows) Next(dest []driver.Value) (err error) {
	if !vr.isValidTable() {
		return errors.New("No valid table")
	}
	if !vr.table().advanceRow() {
		// the go doc says to set rows closed when 'Next' return false.  we won't do that
		// because there can be more than one table.
		return io.EOF
	}
	if vr.table().getColumnCount() != len(dest) {
		return errors.New(fmt.Sprintf("Wrong number of values to Rows.Next, expected %d but saw %d", vr.table().getColumnCount(), len(dest)))
	}

	cts := vr.table().getColumnTypes()
	for i := 0; i < len(dest); i++ {
		ct := cts[i]
		switch ct {
		case -99: // ARRAY
			return fmt.Errorf("Not supporting ARRAY")
		case 1: // NULL
			dest[i] = nil
		case 3: // TINYINT
			v, err := vr.GetTinyInt(int16(i))
			if err != nil {
				return fmt.Errorf("Failed to get TINYINT at column index %d %s", i, err)
			}
			dest[i] = v
		case 4: // SMALLINT
			v, err := vr.GetSmallInt(int16(i))
			if err != nil {
				return fmt.Errorf("Failed to get SMALLINT at column index %d %s", i, err)
			}
			dest[i] = v
		case 5: // INTEGER
			v, err := vr.GetInteger(int16(i))
			if err != nil {
				return fmt.Errorf("Failed to get INTEGER at column index %d %s", i, err)
			}
			dest[i] = v
		case 6: // BIGINT
			v, err := vr.GetBigInt(int16(i))
			if err != nil {
				return fmt.Errorf("Failed to get BIGINT at column index %d %s", i, err)
			}
			dest[i] = v
		case 8: // FLOAT
			v, err := vr.GetFloat(int16(i))
			if err != nil {
				return fmt.Errorf("Failed to get FLOAT at column index %d %s", i, err)
			}
			dest[i] = v
		case 9: // STRING
			v, err := vr.GetVarbinary(int16(i))
			if err != nil {
				return fmt.Errorf("Failed to get STRING/VARBINARY at column index %d %s", i, err)
			}
			dest[i] = v
		case 11: // TIMESTAMP
			return fmt.Errorf("Not supporting TIMESTAMP")
		case 22: // DECIMAL
			return fmt.Errorf("Not supporting DECIMAL")
		case 25: // VARBINARY
			v, err := vr.GetVarbinary(int16(i))
			if err != nil {
				return fmt.Errorf("Failed to get STRING/VARBINARY at column index %d %s", i, err)
			}
			dest[i] = v
		case 26: // GEOGRAPHY_POINT
			return errors.New("Not supporting GEOGRAPHY_POINT")
		case 27: // GEOGRAPHY
			return errors.New("Not supporting GEOGRAPHY")
		default:
			return errors.New(fmt.Sprintf("Unexpected type %d", ct))
		}
	}
	return nil
}

// Advances to the next row of data, returns false if there isn't a next row.
func (vr VoltRows) AdvanceRow() bool {
	return vr.table().advanceRow()
}

// Advances to the row of data indicated by the index.  Returns false if there
// is no row at the given index.
func (vr VoltRows) AdvanceToRow(rowIndex int32) bool {
	if !vr.isValidTable() {
		return false
	}
	return vr.table().advanceToRow(rowIndex)
}

// Advances to the next table.  Returns false if there isn't a next table.
func (vr VoltRows) AdvanceTable() bool {
	return vr.AdvanceToTable(vr.tableIndex + 1)
}

// Advances to the table indicated by the index.  Returns false if there is
// no table at the given index.
func (vr VoltRows) AdvanceToTable(tableIndex int16) bool {
	if tableIndex >= vr.getNumTables() || tableIndex < 0 {
		return false
	}
	vr.tableIndex = tableIndex
	return true
}

// Returns the number of columns in the current table.
func (vr VoltRows) ColumnCount() int {
	if !vr.isValidTable() {
		return 0
	}
	return int(vr.table().columnCount)
}

// Returns the column types of the columns in the current table.
func (vr VoltRows) ColumnTypes() []int8 {
	rv := make([]int8, 0)
	if vr.isValidTable() {
		rv = append(rv, vr.table().columnTypes...)
	}
	return rv
}

// Returns the value of a BIGINT column at the given index in the current row.
func (vr VoltRows) GetBigInt(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 8 {
		return nil, fmt.Errorf("Did not find at BIGINT column at index %d\n", colIndex)
	}
	i := bytesToBigInt(bs)
	if i == math.MinInt64 {
		return nil, nil
	}
	return i, nil
}

// Returns the value of a BIGINT column with the given name in the current row.
func (vr VoltRows) GetBigIntByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetBigInt(ci)
}

// Returns the value of a DECIMAL column at the given index in the current row.
func (vr VoltRows) GetDecimal(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 16 {
		return nil, fmt.Errorf("Did not find at DECIMAL column at index %d\n", colIndex)
	}
	if bytes.Compare(bs, null_decimal[:]) == 0 {
		return nil, nil
	}
	var leadingZeroCount = 0
	for i, b := range bs {
		if b != 0 {
			leadingZeroCount = i
			break
		}
	}
	bi := new(big.Int)
	bi.SetBytes(bs[leadingZeroCount:])
	fl := new(big.Float)
	fl.SetInt(bi)
	dec := new(big.Float)
	dec = dec.Quo(fl, big.NewFloat(1e12))
	return dec, nil
}

// Returns the value of a DECIMAL column with the given name in the current row.
func (vr VoltRows) GetDecimalByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetDecimal(ci)
}

// Returns the value of a FLOAT column at the given index in the current row.
func (vr VoltRows) GetFloat(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 8 {
		return nil, fmt.Errorf("Did not find at FLOAT column at index %d\n", colIndex)
	}
	f := bytesToFloat(bs)
	if f == -1.7E+308 {
		return nil, nil
	}
	return f, nil
}

// Returns the value of a FLOAT column with the given name in the current row.
func (vr VoltRows) GetFloatByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetFloat(ci)
}

// Returns the value of a INTEGER column at the given index in the current row.
func (vr VoltRows) GetInteger(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 4 {
		return nil, fmt.Errorf("Did not find at INTEGER column at index %d\n", colIndex)
	}
	i := bytesToInt(bs)
	if i == math.MinInt32 {
		return nil, nil
	}
	return i, nil
}

// Returns the value of a INTEGER column with the given name in the current row.
func (vr VoltRows) GetIntegerByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetInteger(ci)
}

// Returns the value of a SMALLINT column at the given index in the current row.
func (vr VoltRows) GetSmallInt(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 2 {
		return nil, fmt.Errorf("Did not find at SMALLINT column at index %d\n", colIndex)
	}
	i := bytesToSmallInt(bs)
	if i == math.MinInt16 {
		return nil, nil
	}
	return i, nil
}

// Returns the value of a SMALLINT column with the given name in the current row.
func (vr VoltRows) GetSmallIntByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetSmallInt(ci)
}

// Returns the value of a STRING column at the given index in the current row.
func (vr VoltRows) GetString(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	// if there are only four bytes then there is just the
	// length, which must be -1, the null encoding.
	if len(bs) == 4 {
		return nil, nil
	}
	// exclude the length from the string itself.
	return string(bs[4:]), nil
}

// Returns the value of a STRING column with the given name in the current row.
func (vr VoltRows) GetStringByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetString(ci)
}

// Returns the value of a TIMESTAMP column at the given index in the current row.
func (vr VoltRows) GetTimestamp(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 8 {
		return nil, fmt.Errorf("Did not find at TIMESTAMP column at index %d\n", colIndex)
	}
	if bytes.Compare(bs, null_timestamp[:]) == 0 {
		return nil, nil
	}
	t := bytesToTime(bs)
	return t, nil
}

// Returns the value of a TIMESTAMP column with the given name in the current row.
func (vr VoltRows) GetTimestampByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetTimestamp(ci)
}

// Returns the value of a TINYINT column at the given index in the current row.
func (vr VoltRows) GetTinyInt(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) > 1 {
		return nil, fmt.Errorf("Did not find at TINYINT column at index %d\n", colIndex)
	}
	i := int8(bs[0])
	if i == math.MinInt8 {
		return nil, nil
	}
	return i, nil

}

// Returns the value of a TINYINT column with the given name in the current row.
func (vr VoltRows) GetTinyIntByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetTinyInt(ci)
}

// Returns the value of a VARBINARY column at the given index in the current row.
func (vr VoltRows) GetVarbinary(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) == 4 {
		return nil, nil
	}
	return bs[4:], nil
}

// Returns the value of a VARBINARY column with the given name in the current row.
func (vr VoltRows) GetVarbinaryByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetVarbinary(ci)
}

func (vr VoltRows) isValidTable() bool {
	return vr.tableIndex != -1
}

func (vr VoltRows) table() *voltTable {
	return vr.tables[vr.tableIndex]
}

// funcs that cast:
func bytesToBigInt(bs []byte) int64 {
	return int64(order.Uint64(bs))
}

func bytesToInt(bs []byte) int32 {
	return int32(order.Uint32(bs))
}

func bytesToFloat(bs []byte) float64 {
	return math.Float64frombits(order.Uint64(bs))
}

func bytesToSmallInt(bs []byte) int16 {
	return int16(order.Uint16(bs))
}

func bytesToTime(bs []byte) time.Time {
	// the time is essentially a long as milliseconds
	millis := int64(order.Uint64(bs))
	// time.Unix will take either seconds or nanos.  Multiply by 1000 and use nanos.
	return time.Unix(0, millis*1000)
}

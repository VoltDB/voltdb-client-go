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

var NULL_DECIMAL = [...]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
var NULL_TIMESTAMP = [...]byte{128, 0, 0, 0, 0, 0, 0, 0}

type VoltRows struct {
	VoltResult
	numTables  int16
	tables     []*VoltTable
	tableIndex int16
}

func newVoltRows(result VoltResult, numTables int16, tables []*VoltTable) *VoltRows {
	var vr = new(VoltRows)
	vr.VoltResult = result
	vr.numTables = numTables
	vr.tables = tables
	vr.tableIndex = 0
	return vr
}

// interface for database/sql/driver.Rows
func (vr VoltRows) Close() error {
	return nil
}

func (vr VoltRows) Columns() []string {
	rv := make([]string, 0)
	rv = append(rv, vr.table().columnNames...)
	return rv
}

func (vr VoltRows) Next(dest []driver.Value) (err error) {
	if vr.err != nil {
		return err
	}
	if !vr.table().AdvanceRow() {
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

// volt api

func (vr VoltRows) AdvanceRow() bool {
	if vr.err != nil {
		panic("Check error with Error() before calling AdvanceRow()")
	}
	return vr.table().AdvanceRow()
}

func (vr VoltRows) AdvanceToRow(rowIndex int32) bool {
	if vr.err != nil {
		panic("Check error with Error() before calling AdvanceToRow()")
	}
	return vr.table().AdvanceToRow(rowIndex)
}

func (vr VoltRows) AdvanceTable() bool {
	if vr.tableIndex+1 >= vr.numTables {
		return false
	}
	vr.tableIndex++
	return true
}

func (vr VoltRows) ColumnCount() int {
	return int(vr.table().columnCount)
}

func (vr VoltRows) ColumnTypes() []int8 {
	rv := make([]int8, 0)
	rv = append(rv, vr.table().columnTypes...)
	return rv
}

// accessors by type
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

func (vr VoltRows) GetBigIntByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetBigInt(ci)
}

func (vr VoltRows) GetDecimal(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 16 {
		return nil, fmt.Errorf("Did not find at DECIMAL column at index %d\n", colIndex)
	}
	if bytes.Compare(bs, NULL_DECIMAL[:]) == 0 {
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

func (vr VoltRows) GetDecimalByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetDecimal(ci)
}

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

func (vr VoltRows) GetFloatByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetFloat(ci)
}

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

func (vr VoltRows) GetIntegerByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetInteger(ci)
}

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

func (vr VoltRows) GetSmallIntByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetSmallInt(ci)
}

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

func (vr VoltRows) GetStringByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetString(ci)
}

func (vr VoltRows) GetTimestamp(colIndex int16) (interface{}, error) {
	bs, err := vr.table().getBytes(vr.table().rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 8 {
		return nil, fmt.Errorf("Did not find at TIMESTAMP column at index %d\n", colIndex)
	}
	if bytes.Compare(bs, NULL_TIMESTAMP[:]) == 0 {
		return nil, nil
	}
	t := bytesToTime(bs)
	return t, nil
}

func (vr VoltRows) GetTimestampByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetTimestamp(ci)
}

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

func (vr VoltRows) GetTinyIntByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetTinyInt(ci)
}

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

func (vr VoltRows) GetVarbinaryByName(cn string) (interface{}, error) {
	ci, ok := vr.table().cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetVarbinary(ci)
}

func (vr VoltRows) table() *VoltTable {
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

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
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
)

const INVALID_ROW_INDEX = -1

var NULL_DECIMAL = [...]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
var NULL_TIMESTAMP = [...]byte{128, 0, 0, 0, 0, 0, 0, 0}

type VoltRows struct {
	clientHandle         int64
	appStatus            int8
	appStatusString      string
	clusterRoundTripTime int32
	columnCount          int16
	columnTypes          []int8
	columnNames          []string
	columnOffsets        [][]int32
	rowCount             int32
	rows                 [][]byte
	rowIndex             int32
	readers              []*bytes.Reader
	cnToCi               map[string]int16
}

func NewVoltTableRow(clientHandle int64, appStatus int8, appStatusString string, clusterRoundTripTime int32, columnCount int16,
	columnTypes []int8, columnNames []string, rowCount int32, rows [][]byte) *VoltRows {
	var vr = new(VoltRows)
	vr.clientHandle = clientHandle
	vr.appStatus = appStatus
	vr.appStatusString = appStatusString
	vr.clusterRoundTripTime = clusterRoundTripTime
	vr.columnCount = columnCount
	vr.columnTypes = columnTypes
	vr.columnNames = columnNames
	// rowCount +1 because want to represent the end of the data as an offset
	// so we can know the ending index of the last column.
	vr.columnOffsets = make([][]int32, rowCount+1)
	vr.rowCount = rowCount
	vr.rows = rows
	vr.rowIndex = INVALID_ROW_INDEX
	vr.readers = make([]*bytes.Reader, rowCount)

	// store columnName to columnIndex
	vr.cnToCi = make(map[string]int16)
	for ci, cn := range columnNames {
		vr.cnToCi[cn] = int16(ci)
	}

	return vr
}

// interface for database/sql/driver.Rows
func (vr VoltRows) Close() error {
	return nil
}

func (vr VoltRows) Columns() []string {
	rv := make([]string, 0)
	rv = append(rv, vr.columnNames...)
	return rv
}

func (vr VoltRows) Next(dest []driver.Value) error {
	return nil
}

// volt api

func (vr *VoltRows) AdvanceRow() bool {
	return vr.AdvanceToRow(vr.rowIndex + 1)
}

func (vr *VoltRows) AdvanceToRow(rowIndex int32) bool {
	vr.rowIndex = rowIndex
	if vr.rowIndex >= vr.rowCount {
		return false
	}
	return true
}

func (vr *VoltRows) AppStatus() int8 {
	return vr.appStatus
}

func (vr *VoltRows) AppStatusString() string {
	return vr.appStatusString
}

func (vr *VoltRows) ClusterRoundTripTime() int32 {
	return vr.clusterRoundTripTime
}

func (vr *VoltRows) ColumnCount() int {
	return int(vr.columnCount)
}

func (vr *VoltRows) ColumnTypes() []int8 {
	rv := make([]int8, 0)
	rv = append(rv, vr.columnTypes...)
	return rv
}

func (vr *VoltRows) GetBigInt(colIndex int16) (interface{}, error) {
	bs, err := vr.getBytes(vr.rowIndex, colIndex)
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

func (vr *VoltRows) GetBigIntByName(cn string) (interface{}, error) {
	ci, ok := vr.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetBigInt(ci)
}

func (vr *VoltRows) GetDecimal(colIndex int16) (interface{}, error) {
	bs, err := vr.getBytes(vr.rowIndex, colIndex)
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

func (vr *VoltRows) GetDecimalByName(cn string) (interface{}, error) {
	ci, ok := vr.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetDecimal(ci)
}

func (vr *VoltRows) GetFloat(colIndex int16) (interface{}, error) {
	bs, err := vr.getBytes(vr.rowIndex, colIndex)
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

func (vr *VoltRows) GetFloatByName(cn string) (interface{}, error) {
	ci, ok := vr.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetFloat(ci)
}

func (vr *VoltRows) GetInteger(colIndex int16) (interface{}, error) {
	bs, err := vr.getBytes(vr.rowIndex, colIndex)
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

func (vr *VoltRows) GetIntegerByName(cn string) (interface{}, error) {
	ci, ok := vr.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetInteger(ci)
}

func (vr *VoltRows) GetSmallInt(colIndex int16) (interface{}, error) {
	bs, err := vr.getBytes(vr.rowIndex, colIndex)
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

func (vr *VoltRows) GetSmallIntByName(cn string) (interface{}, error) {
	ci, ok := vr.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetSmallInt(ci)
}

func (vr *VoltRows) GetString(colIndex int16) (interface{}, error) {
	bs, err := vr.getBytes(vr.rowIndex, colIndex)
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

func (vr *VoltRows) GetStringByName(cn string) (interface{}, error) {
	ci, ok := vr.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetString(ci)
}

func (vr *VoltRows) GetTimestamp(colIndex int16) (interface{}, error) {
	bs, err := vr.getBytes(vr.rowIndex, colIndex)
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

func (vr *VoltRows) GetTimestampByName(cn string) (interface{}, error) {
	ci, ok := vr.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetTimestamp(ci)
}

func (vr *VoltRows) GetTinyInt(colIndex int16) (interface{}, error) {
	bs, err := vr.getBytes(vr.rowIndex, colIndex)
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

func (vr *VoltRows) GetTinyIntByName(cn string) (interface{}, error) {
	ci, ok := vr.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetTinyInt(ci)
}

func (vr *VoltRows) GetVarbinary(colIndex int16) (interface{}, error) {
	bs, err := vr.getBytes(vr.rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) == 4 {
		return nil, nil
	}
	return bs[4:], nil
}

func (vr *VoltRows) GetVarbinaryByName(cn string) (interface{}, error) {
	ci, ok := vr.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vr.GetVarbinary(ci)
}

// the common logic for reading a column is here.  Read a column as bytes and
// the represent it as the correct type.
func (vr *VoltRows) calcOffsetsForRow(rowIndex int32) ([]int32, error) {
	// column count + 1, want starting and ending index for every column
	offsets := make([]int32, vr.columnCount+1)
	r := vr.getReader(rowIndex)
	var colIndex int16 = 0
	var offset int32 = 0
	offsets[0] = 0
	for ; colIndex < vr.columnCount; colIndex++ {
		len, err := vr.colLength(r, offset, vr.columnTypes[colIndex])
		if err != nil {
			return nil, err
		}
		offset += len
		offsets[colIndex+1] = offset

	}
	return offsets, nil
}

func (vr *VoltRows) colLength(r *bytes.Reader, offset int32, colType int8) (int32, error) {
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
		strlen, err := readInt32At(r, int64(offset))
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
		strlen, err := readInt32At(r, int64(offset))
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

func (vr *VoltRows) getBytes(rowIndex int32, columnIndex int16) ([]byte, error) {
	offsets, err := vr.getOffsetsForRow(rowIndex)
	if err != nil {
		return nil, err
	}
	return vr.rows[rowIndex][offsets[columnIndex]:offsets[columnIndex+1]], nil
}

func (vr *VoltRows) getOffsetsForRow(rowIndex int32) ([]int32, error) {
	offsets := vr.columnOffsets[rowIndex]
	if offsets != nil {
		return offsets, nil
	}
	offsets, err := vr.calcOffsetsForRow(rowIndex)
	if err != nil {
		return nil, err
	}
	vr.columnOffsets[rowIndex] = offsets
	return offsets, nil
}

func (vr *VoltRows) getReader(rowIndex int32) *bytes.Reader {
	r := vr.readers[rowIndex]
	if r == nil {
		r = bytes.NewReader(vr.rows[rowIndex])
		vr.readers[rowIndex] = r
	}
	return r
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

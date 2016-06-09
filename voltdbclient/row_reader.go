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
	"math"
	"math/big"
	"strings"
	"time"
)

func (vt *VoltTable) GetBigInt(colIndex int16) (interface{}, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 8 {
		return nil, fmt.Errorf("Did not find at BIGINT column at index %d\n", colIndex)
	}
	i := vt.bytesToBigInt(bs)
	if i == math.MinInt64 {
		return nil, nil
	}
	return i, nil
}

func (vt *VoltTable) GetBigIntByName(cn string) (interface{}, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetBigInt(ci)
}

func (vt *VoltTable) GetDecimal(colIndex int16) (interface{}, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
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

func (vt *VoltTable) GetDecimalByName(cn string) (interface{}, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetDecimal(ci)
}

func (vt *VoltTable) GetFloat(colIndex int16) (interface{}, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 8 {
		return nil, fmt.Errorf("Did not find at FLOAT column at index %d\n", colIndex)
	}
	f := vt.bytesToFloat(bs)
	if f == -1.7E+308 {
		return nil, nil
	}
	return f, nil
}

func (vt *VoltTable) GetFloatByName(cn string) (interface{}, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetFloat(ci)
}

func (vt *VoltTable) GetInteger(colIndex int16) (interface{}, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 4 {
		return nil, fmt.Errorf("Did not find at INTEGER column at index %d\n", colIndex)
	}
	i := vt.bytesToInt(bs)
	if i == math.MinInt32 {
		return nil, nil
	}
	return i, nil
}

func (vt *VoltTable) GetIntegerByName(cn string) (interface{}, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetInteger(ci)
}

func (vt *VoltTable) GetSmallInt(colIndex int16) (interface{}, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 2 {
		return nil, fmt.Errorf("Did not find at SMALLINT column at index %d\n", colIndex)
	}
	i := vt.bytesToSmallInt(bs)
	if i == math.MinInt16 {
		return nil, nil
	}
	return i, nil
}

func (vt *VoltTable) GetSmallIntByName(cn string) (interface{}, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetSmallInt(ci)
}

func (vt *VoltTable) GetString(colIndex int16) (interface{}, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
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

func (vt *VoltTable) GetStringByName(cn string) (interface{}, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetString(ci)
}

func (vt *VoltTable) GetTimestamp(colIndex int16) (interface{}, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) != 8 {
		return nil, fmt.Errorf("Did not find at TIMESTAMP column at index %d\n", colIndex)
	}
	if bytes.Compare(bs, NULL_TIMESTAMP[:]) == 0 {
		return nil, nil
	}
	t := vt.bytesToTime(bs)
	return t, nil
}

func (vt *VoltTable) GetTimestampByName(cn string) (interface{}, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetTimestamp(ci)
}

func (vt *VoltTable) GetTinyInt(colIndex int16) (interface{}, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
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

func (vt *VoltTable) GetTinyIntByName(cn string) (interface{}, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetTinyInt(ci)
}

func (vt *VoltTable) GetVarbinary(colIndex int16) (interface{}, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return nil, err
	}
	if len(bs) == 4 {
		return nil, nil
	}
	return bs[4:], nil
}

func (vt *VoltTable) GetVarbinaryByName(cn string) (interface{}, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetVarbinary(ci)
}

func (vt *VoltTable) bytesToBigInt(bs []byte) int64 {
	return int64(order.Uint64(bs))
}

func (vt *VoltTable) bytesToInt(bs []byte) int32 {
	return int32(order.Uint32(bs))
}

func (vt *VoltTable) bytesToFloat(bs []byte) float64 {
	return math.Float64frombits(order.Uint64(bs))
}

func (vt *VoltTable) bytesToSmallInt(bs []byte) int16 {
	return int16(order.Uint16(bs))
}

func (vt *VoltTable) bytesToTime(bs []byte) time.Time {
	// the time is essentially a long as milliseconds
	millis := int64(order.Uint64(bs))
	// time.Unix will take either seconds or nanos.  Multiply by 1000 and use nanos.
	return time.Unix(0, millis*1000)
}

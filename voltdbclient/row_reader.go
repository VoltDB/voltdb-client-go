package voltdbclient

import (
	"fmt"
	"math"
	"strings"
	"time"
	"bytes"
	"math/big"
)

func (vt *VoltTable) GetBigInt(colIndex int16) (int64, bool, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return 0, false, err
	}
	if len(bs) != 8 {
		return 0, false, fmt.Errorf("Did not find at BIGINT column at index %d\n", colIndex)
	}
	i := vt.bytesToBigInt(bs)
	if i == math.MinInt64 {
		return 0, true, nil
	}
	return i, false, nil
}

func (vt *VoltTable) GetBigIntByName(cn string) (int64, bool, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return 0, false, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetBigInt(ci)
}

func (vt *VoltTable) GetDecimal(colIndex int16) (*big.Float, bool, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return new(big.Float), false, err
	}
	if len(bs) != 16 {
		return new(big.Float), false, fmt.Errorf("Did not find at DECIMAL column at index %d\n", colIndex)
	}
	if bytes.Compare(bs, NULL_DECIMAL[:]) == 0 {
		return nil, true, nil
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
	return dec, false, nil
}

func (vt *VoltTable) GetDecimalByName(cn string) (*big.Float, bool, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return new(big.Float), false, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetDecimal(ci)
}

func (vt *VoltTable) GetFloat(colIndex int16) (float64, bool, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return 0, false, err
	}
	if len(bs) == 0 {
		return 0, true, nil
	}
	if len(bs) != 8 {
		return 0, false, fmt.Errorf("Did not find at FLOAT column at index %d\n", colIndex)
	}
	f := vt.bytesToFloat(bs)
	if f == -1.7E+308 {
		return 0, true, nil
	}
	return f, false, nil
}

func (vt *VoltTable) GetFloatByName(cn string) (float64, bool, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return 0, false, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetFloat(ci)
}


func (vt *VoltTable) GetInteger(colIndex int16) (int32, bool, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return 0, false, err
	}
	i := vt.bytesToInt(bs)
	if i == math.MinInt32 {
		return 0, true, nil
	}
	return i, false, nil
}

func (vt *VoltTable) GetIntegerByName(cn string) (int32, bool, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return 0, false, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetInteger(ci)
}

func (vt *VoltTable) GetSmallInt(colIndex int16) (int16, bool, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return 0, false, err
	}
	if len(bs) != 2 {
		return 0, false, fmt.Errorf("Did not find at SMALLINT column at index %d\n", colIndex)
	}
	i := vt.bytesToSmallInt(bs)
	if i == math.MinInt16 {
		return 0, true, nil
	}
	return i, false, nil
}


func (vt *VoltTable) GetSmallIntByName(cn string) (int16, bool, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return 0, false, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetSmallInt(ci)
}

func (vt *VoltTable) GetString(colIndex int16) (string, bool, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return "", false, err
	}
	// if there are only four bytes then there is just the
	// length, which must be -1, the null encoding.
	if len(bs) == 4 {
		return "", true, nil
	}
	// exclude the length from the string itself.
	return string(bs[4:]), false, nil
}

func (vt *VoltTable) GetStringByName(cn string) (string, bool, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return "", false, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetString(ci)
}

func (vt *VoltTable) GetTimestamp(colIndex int16) (time.Time, bool, error) {
	var zeroTime time.Time
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return zeroTime, false, err
	}
	if len(bs) != 8 {
		return zeroTime, false, fmt.Errorf("Did not find at TIMESTAMP column at index %d\n", colIndex)
	}
	if bytes.Compare(bs, NULL_TIMESTAMP[:]) == 0 {
		return zeroTime, true, nil
	}
	t := vt.bytesToTime(bs)
	return t, false, nil
}

func (vt *VoltTable) GetTimestampByName(cn string) (time.Time, bool, error) {
	var zeroTime time.Time
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return zeroTime, false, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetTimestamp(ci)
}


func (vt *VoltTable) GetTinyInt(colIndex int16) (int8, bool, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return 0, false, err
	}
	if len(bs) > 1 {
		return 0, false, fmt.Errorf("Did not find at TINYINT column at index %d\n", colIndex)
	}
	i := int8(bs[0])
	if i == math.MinInt8 {
		return 0, true, nil
	}
	return i, false, nil

}

func (vt *VoltTable) GetTinyIntByName(cn string) (int8, bool, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return 0, false, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetTinyInt(ci)
}

func (vt *VoltTable) GetVarbinary(colIndex int16) ([]byte, bool, error) {
	bs, err := vt.getBytes(vt.rowIndex, colIndex)
	if err != nil {
		return nil, false, err
	}
	if len(bs) == 4 {
		return nil, true, nil
	}
	return bs[4:], false, nil
}

func (vt *VoltTable) GetVarbinaryByName(cn string) ([]byte, bool, error) {
	ci, ok := vt.cnToCi[strings.ToUpper(cn)]
	if !ok {
		return nil, false, fmt.Errorf("column name %v was not found", cn)
	}
	return vt.GetVarbinary(ci)
}

func (vt *VoltTable) bytesToBigInt(bs []byte) (int64) {
	return int64(order.Uint64(bs))
}

func (vt *VoltTable) bytesToInt(bs []byte) (int32) {
	return int32(order.Uint32(bs))
}

func (vt *VoltTable) bytesToFloat(bs []byte) (float64) {
	return math.Float64frombits(order.Uint64(bs))
}

func (vt *VoltTable) bytesToSmallInt(bs []byte) (int16) {
	return int16(order.Uint16(bs))
}

func (vt *VoltTable) bytesToTime(bs []byte) time.Time {
	// the time is essentially a long as milliseconds
	millis := int64(order.Uint64(bs))
	// time.Unix will take either seconds or nanos.  Multiply by 1000 and use nanos.
	return time.Unix(0, millis * 1000)
}

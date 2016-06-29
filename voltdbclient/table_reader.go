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
	"math"
)

func readByteAt(r *bytes.Reader, off int64) (byte, error) {
	var b [1]byte
	bs := b[:1]
	_, err := r.ReadAt(bs, off)
	if err != nil {
		return 0, err
	}
	return bs[0], nil
}

func readByteArrayAt(r *bytes.Reader, off int64) ([]byte, error) {
	len, err := readInt32At(r, off)
	if err != nil {
		return nil, err
	}
	if len == -1 {
		return nil, nil
	}
	bs := make([]byte, len)
	_, err = r.ReadAt(bs, off+4)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func readInt16At(r *bytes.Reader, off int64) (int16, error) {
	var b [2]byte
	bs := b[:2]
	_, err := r.ReadAt(bs, off)
	if err != nil {
		return 0, err
	}
	return int16(order.Uint16(bs)), nil
}

func readInt8At(r *bytes.Reader, off int64) (int8, error) {
	var b [1]byte
	bs := b[:1]
	_, err := r.ReadAt(bs, off)
	if err != nil {
		return 0, err
	}
	return int8(bs[0]), nil
}

func readInt32At(r *bytes.Reader, off int64) (int32, error) {
	var b [4]byte
	bs := b[:4]
	_, err := r.ReadAt(bs, off)
	if err != nil {
		return 0, err
	}
	return int32(order.Uint32(bs)), nil
}

func readInt64At(r *bytes.Reader, off int64) (int64, error) {
	var b [8]byte
	bs := b[:8]
	_, err := r.ReadAt(bs, off)
	if err != nil {
		return 0, err
	}
	return int64(order.Uint64(bs)), nil
}

func readFloatAt(r *bytes.Reader, off int64) (float64, error) {
	var b [8]byte
	bs := b[:8]
	_, err := r.ReadAt(bs, off)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(order.Uint64(bs)), nil
}

func readStringAt(r *bytes.Reader, off int64) (string, error) {
	strlen, err := readInt32At(r, off)
	if err != nil {
		return "", err
	}
	if strlen == -1 {
		// NULL string not supported, return zero value
		return "", nil
	}
	bs := make([]byte, strlen)
	_, err = r.ReadAt(bs, off+4)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

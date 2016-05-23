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

import "bytes"

func readIntAt(r *bytes.Reader, off int64) (int32, error) {
	var b [4]byte
	bs := b[:4]
	_, err := r.ReadAt(bs, off)
	if err != nil {
		return 0, err
	}
	result := order.Uint32(bs)
	return int32(result), nil
}

func readByteArrayAt(r *bytes.Reader, off int64) ([]byte, error) {
	len, err := readIntAt(r, off)
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

func readStringAt(r *bytes.Reader, off int64) (string, error) {
	len, err := readIntAt(r, off)
	if err != nil {
		return "", err
	}
	if len == -1 {
		// NULL string not supported, return zero value
		return "", nil
	}
	bs := make([]byte, len)
	_, err = r.ReadAt(bs, off+4)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

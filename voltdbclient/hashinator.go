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
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"strconv"

	"errors"

	"github.com/spaolacci/murmur3"
)

type hashinator interface {
	getConfigurationType() string

	/**
	 * Given the type of the targeting partition parameter and an object,
	 * coerce the object to the correct type and hash it.
	 */
	getHashedPartitionForParameter(partitionParameterType int, partitionValue driver.Value) (hashedPartition int, err error)
}

type hashinatorElastic struct {
	// sorted array of token2partition pair
	tp Token2PartitionSlice
}

func newHashinatorElastic(hashConfigFormat int, cooked bool, hashConfig []byte) (h *hashinatorElastic, err error) {
	if hashConfigFormat != JSON_FORMAT {
		return nil, errors.New("Only support JSON format hashconfig.")
	}
	h = new(hashinatorElastic)
	if cooked {
		hashConfig, err = fromGzip(hashConfig)
		if err != nil {
			return nil, err
		}
	}

	// unmarshall json
	if err = h.unmarshalJSONConfig(hashConfig); err != nil {
		return nil, err
	}

	return h, nil
}

func (h *hashinatorElastic) getConfigurationType() string {
	return ELASTIC
}

func (h *hashinatorElastic) getHashedPartitionForParameter(partitionParameterType int, partitionValue driver.Value) (hashedPartition int, err error) {
	return h.hashinateBytes(valueToBytes(partitionValue))
}

/**
 * Given []bytes, pick a partition to store the data.
 */
func (h hashinatorElastic) hashinateBytes(b []byte) (partition int, err error) {
	if b == nil {
		return 0, nil
	}

	v1, _ := murmur3.Sum128(b)
	hash := int(v1 >> 32)
	partition = SearchToken2Partitions(h.tp, hash)
	return partition, nil
}

/**
 * Converts the object into bytes for hashing.
 * return a byte array representation of obj
 * OR nil if the obj is nullValue or any other Volt representation
 * of a null value.
 */
func valueToBytes(v driver.Value) []byte {
	if v == nil {
		return nil
	}
	var value uint64
	switch v.(type) {
	case nullValue:
		return nil
	case []byte:
		return v.([]byte)
	case string:
		return []byte(v.(string))
	case byte:
		value = uint64(v.(byte))
	case int8:
		value = uint64(v.(int8))
	case int16:
		value = uint64(v.(int16))
	case int32:
		value = uint64(v.(int32))
	case int64:
		value = uint64(v.(int64))
	}

	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, value)
	return bs
}

// until go 1.7, go lang won't support non-string type keys for (un-)marshal
// https://github.com/golang/go/commit/ffbd31e9f79ad8b6aaeceac1397678e237581064
// need to one more loop for the conversation
func (h *hashinatorElastic) unmarshalJSONConfig(bytes []byte) (err error) {

	// Unmarshal the string-keyed map
	sk := make(map[string]int)
	err = json.Unmarshal(bytes, &sk)
	if err != nil {
		return
	}

	h.tp = make(Token2PartitionSlice, 128)
	// Copy the values

	var ki int
	for k, v := range sk {
		ki, err = strconv.Atoi(k)
		if err != nil {
			return
		}
		h.tp = append(h.tp, token2Partition{ki, v})
	}
	h.tp.Sort()

	return
}

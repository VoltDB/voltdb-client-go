/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
	"errors"
	"strconv"

	"sync"

	"github.com/spaolacci/murmur3"
)

var spool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 8)
	},
}

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
	if hashConfigFormat != JSONFormat {
		return nil, errors.New("Only support JSON format hashconfig.")
	}

	if cooked {
		hashConfig, err = fromGzip(hashConfig)
		if err != nil {
			return nil, err
		}
	}

	h = &hashinatorElastic{}

	// unmarshall json
	if err = h.unmarshalJSONConfig(hashConfig); err != nil {
		return nil, err
	}

	return h, nil
}

func (h *hashinatorElastic) getConfigurationType() string {
	return Elastic
}

func (h *hashinatorElastic) getHashedPartitionForParameter(partitionParameterType int, v driver.Value) (hashedPartition int, err error) {
	if v == nil {
		return 0, nil
	}
	var value uint64
	switch v.(type) {
	case nullValue:
		return 0, nil
	case []byte:
		v1, _ := murmur3.Sum128(v.([]byte))
		hash := int(v1 >> 32)
		return SearchToken2Partitions(h.tp, hash), nil
	case string:
		v1, _ := murmur3.Sum128([]byte(v.(string)))
		hash := int(v1 >> 32)
		return SearchToken2Partitions(h.tp, hash), nil
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
	buf := spool.Get().([]byte)
	defer spool.Put(buf)
	binary.LittleEndian.PutUint64(buf, value)
	return h.checkPertition(&buf)
}

func (h *hashinatorElastic) checkPertition(v *[]byte) (int, error) {
	v1, _ := murmur3.Sum128(*v)
	hash := int(v1 >> 32)
	return SearchToken2Partitions(h.tp, hash), nil
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

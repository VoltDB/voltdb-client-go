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
	"compress/gzip"
	"io/ioutil"
	"reflect"
	"sort"
)

// partitonSlice attaches the methods of sort.Interface to []int64, sorting in increasing order.
type token2Partition struct {
	token     int
	partition int
}

type Token2PartitionSlice []token2Partition

func (s Token2PartitionSlice) Len() int           { return len(s) }
func (s Token2PartitionSlice) Less(i, j int) bool { return s[i].token < s[j].token }
func (s Token2PartitionSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Sort is a convenience method.
func (s Token2PartitionSlice) Sort() {
	sort.Sort(s)
}

func SearchToken2Partitions(a []token2Partition, token int) (partition int) {
	t := sort.Search(len(a), func(i int) bool { return a[i].token >= token })
	partition = a[t-1].partition
	return
}

// helper function for clearing content of any type
func clear(v interface{}) {
	p := reflect.ValueOf(v).Elem()
	p.Set(reflect.Zero(p.Type()))
}

func fromGzip(compressed []byte) ([]byte, error) {
	var b = bytes.NewReader(compressed)
	r, err := gzip.NewReader(b)
	defer r.Close()
	if err != nil {
		return nil, err
	}
	decompressed, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return decompressed, nil
}

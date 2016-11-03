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

package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"time"
)

type payLoadProcessor struct {
	keySize        int
	minValueSize   int
	maxValueSize   int
	entropy        int
	poolSize       int
	useCompression bool
	keyFormat      string
}

// var entropyBytesGlobal bytes.Buffer = new(bytes.Buffer)
const entropyByteSize int = 1024 * 1024 * 4

var entropyBytes *bytes.Reader

func newPayloadProcessor(keySize, minValueSize, maxValueSize, entropy, poolSize int, useCompression bool) *payLoadProcessor {
	rand.Seed(time.Now().UTC().UnixNano())
	proc := new(payLoadProcessor)
	proc.keySize = keySize
	proc.minValueSize = minValueSize
	proc.maxValueSize = maxValueSize
	proc.entropy = entropy
	proc.poolSize = poolSize
	proc.useCompression = useCompression

	proc.keyFormat = "K%" + strconv.Itoa(proc.keySize) + "v"
	// rand.Seed()
	b := make([]byte, entropyByteSize)
	for i := range b {
		b[i] = byte(rand.Intn(127) % entropy)
	}
	entropyBytes = bytes.NewReader(b)
	return proc

}

func (proc *payLoadProcessor) generateForStore() (key string, rawValue, storeValue []byte) {
	key = fmt.Sprintf(proc.keyFormat, rand.Intn(proc.poolSize))
	rawValue = make([]byte, proc.minValueSize+rand.Intn(proc.maxValueSize-proc.minValueSize+1))
	if entropyBytes.Len() > len(rawValue) {
		entropyBytes.Read(rawValue)
	} else {
		entropyBytes.Seek(0, 0)
		entropyBytes.Read(rawValue)
	}
	if proc.useCompression {
		return key, rawValue, toGzip(rawValue)
	}
	return key, rawValue, rawValue
}

func (proc *payLoadProcessor) generateRandomKeyForRetrieval() string {
	return fmt.Sprintf(proc.keyFormat, rand.Intn(proc.poolSize))
}

func (proc *payLoadProcessor) retrieveFromStore(storeValue []byte) ([]byte, []byte) {
	if proc.useCompression {
		return fromGzip(storeValue), storeValue
	}
	return storeValue, storeValue
}

func fromGzip(compressed []byte) []byte {
	var b = bytes.NewReader(compressed)
	r, _ := gzip.NewReader(b)
	decompressed, _ := ioutil.ReadAll(r)
	r.Close()
	return decompressed
}

func toGzip(raw []byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(raw)
	w.Close()
	return b.Bytes()
}

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

)
import (
	"fmt"
	"math/rand"
	"compress/gzip"
	"bytes"
)

type payLoadProcessor struct {
	keySize int
	minValueSize int
	maxValueSize int
	entropy int
	poolSize int
	useCompression bool
	keyFormat string
	r rand.Rand
}

func NewPayloadProcessor(keySize, minValueSize, maxValueSize, entropy, poolSize int, useCompression bool) *payLoadProcessor {
	proc := new(payLoadProcessor)
	proc.keySize = keySize
	proc.minValueSize = minValueSize
	proc.maxValueSize = maxValueSize
	proc.entropy = entropy
	proc.poolSize = poolSize
	proc.useCompression = useCompression

	proc.keyFormat = "K%v"
	// rand.Seed()
	return proc
}

var entropyBytesGlobal bytes.Buffer

func (proc *payLoadProcessor) generateForStore() (key string, rawValue, StoreValue []byte) {
	key = fmt.Sprintf(proc.keyFormat, rand.Intn(proc.poolSize))
	rawValue = make([]byte,proc.minValueSize+rand.Intn(proc.maxValueSize-proc.minValueSize + 1))
	// fill rawValue

	if (proc.useCompression) {
		return key, rawValue, togzip(rawValue)
	} else {
		return key, rawValue, nil
	}
}

func togzip (raw []byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(raw)
	w.Close()
	return b.Bytes()
}
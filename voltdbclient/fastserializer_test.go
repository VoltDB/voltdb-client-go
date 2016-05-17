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
	"testing"
	"time"
)

func TestWriteByte(t *testing.T) {
	var b bytes.Buffer
	writeByte(&b, 0x10)
	r, err := b.ReadByte()
	if err != nil {
		t.Errorf("writeByte produced error: %v", err)
	}
	if r != 0x10 {
		t.Errorf("writeByte has %v wants %v", r, 0x10)
	}
}

func TestRoundTripByte(t *testing.T) {
	testVals := [...]int8{-128, -10, 0, 127}
	for _, val := range testVals {
		var b bytes.Buffer
		writeByte(&b, val)
		r, _ := readByte(&b)
		if val != r {
			t.Errorf("Expected %v have %v", val, r)
		}
	}
}

func TestWriteShort(t *testing.T) {
	var b bytes.Buffer
	writeShort(&b, 0x4BCD)
	b1, err := b.ReadByte()
	if err != nil {
		t.Errorf("writeShort produced error on first byte: %v", err)
	}
	b2, err := b.ReadByte()
	if err != nil {
		t.Errorf("writeShort produced error on second byte: %v", err)
	}
	if b1 != 0x4B && b2 != 0xCD {
		t.Errorf("writeShort has %v,%v wants %v,%v", b1, b2, 0xCD, 0x4B)
	}
}

// only verifies non-error, correct number of bytes written
func TestWriteInt(t *testing.T) {
	var b bytes.Buffer
	testVals := [...]int32{-100, -1, 0, 1, 100}
	for _, v := range testVals {
		b.Reset()
		err := writeInt(&b, v)
		if err != nil {
			t.Errorf("writeInt produced error %v for %v", err, v)
		}
		length := b.Len()
		if length != 4 {
			t.Errorf("writeInt wrote %v bytes expected 4 bytes", length)
		}
	}
}

func TestRoundTripInt(t *testing.T) {
	testVals := [...]int32{0xF00000, -10, 0, 0xEFFFFF}
	for _, val := range testVals {
		var b bytes.Buffer
		writeInt(&b, val)
		r, _ := readInt(&b)
		if val != r {
			t.Errorf("Expected %v have %v", val, r)
		}
	}
}

// only verifies non-error, correct number of bytes written
func TestWriteFloat(t *testing.T) {
	var b bytes.Buffer
	testVals := [...]float64{-100.1, -1.01, 0.0, 1.01, 100.1}
	for _, v := range testVals {
		b.Reset()
		err := writeFloat(&b, v)
		if err != nil {
			t.Errorf("writeFloat produced error %v for %v", err, v)
		}
		length := b.Len()
		if length != 8 {
			t.Errorf("writeFloat wrote %v bytes expected 8 bytes", length)
		}
	}
}

func TestRoundTripFloat(t *testing.T) {
	testVals := [...]float64{-100, -1, 0, 1, 100, 60.1798012160534}
	for _, val := range testVals {
		var b bytes.Buffer
		writeFloat(&b, val)
		r, _ := readFloat(&b)
		if val != r {
			t.Errorf("Expected %v have %v", val, r)
		}
	}
}

// only tests a single pure-ascii string
func TestWriteString(t *testing.T) {
	var b bytes.Buffer
	expected := [...]byte{0x00, 0x00, 0x00, 0x06, 'a', 'b', 'c', 'd', 'e', 'f'}
	err := writeString(&b, "abcdef")
	if err != nil {
		t.Errorf("writeString produced error %v", err)
	}
	if b.Len() != 10 {
		t.Errorf("writeString wrote %v but expected to write 10", b.Len())
	}
	for idx, val := range expected {
		actual, _ := b.ReadByte()
		if val != actual {
			t.Errorf("writeString at index %v has %v wants %v", idx, actual, val)
		}
	}
}

func TestRoundTripString(t *testing.T) {
	val := "⋒♈ℱ8 ♈ᗴᔕ♈ ᔕ♈ᖇᓰﬡᘐ"
	var b bytes.Buffer
	writeString(&b, val)
	result, _ := readString(&b)
	if val != result {
		t.Errorf("expected %v received %v", val, result)
	}
}

func TestRoundTripNullTimestamp(t *testing.T) {
	var b bytes.Buffer
	writeTimestamp(&b, time.Time{})
	result, _ := readTimestamp(&b)
	if !result.IsZero() {
		t.Error("timestamp round trip failed. Want zero-value have non-zero")
	}
}

func TestRoundTripNegativeTimestamp(t *testing.T) {
	ts := time.Unix(-10000, 0)

	var b bytes.Buffer
	writeTimestamp(&b, ts)
	result, _ := readTimestamp(&b)
	if result != ts {
		t.Errorf("timestamp round trip failed, expected %s got %s", ts.String(), result.String())
	}
}

func TestReflection(t *testing.T) {
	var b bytes.Buffer
	var expInt8 int8 = 5
	marshalParam(&b, expInt8)
	rVtByte, _ := readByte(&b) // volttype
	if rVtByte != vt_BOOL {
		t.Errorf("reflect failed to write volttype byte")
	}
	result, _ := readByte(&b)
	if result != expInt8 {
		t.Errorf("int8 reflection failed. Want %d have %d", expInt8, result)
	}

	b.Reset()
	var expString string = "abcde"
	marshalParam(&b, expString)
	rVtString, _ := readByte(&b) // volttype
	if rVtString != vt_STRING {
		t.Errorf("reflect failed to write volttype string")
	}
	rString, _ := readString(&b)
	if rString != expString {
		t.Errorf("string reflection failed. Want %s have %s", expString, rString)
	}

	b.Reset()
	var expTimestamp time.Time = time.Now().Round(time.Microsecond)
	marshalParam(&b, expTimestamp)
	rVtTimestamp, _ := readByte(&b) // volttype
	if rVtTimestamp != vt_TIMESTAMP {
		t.Errorf("reflect failed to write volttype timestamp")
	}
	rTimestamp, _ := readTimestamp(&b)
	if rTimestamp != expTimestamp {
		t.Errorf("timestamp reflection failed. Want %v have %v", expTimestamp, rTimestamp)
	}
}

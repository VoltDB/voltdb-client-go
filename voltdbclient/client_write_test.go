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
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"testing"
	"time"
)

func SimpleProcedureCall(t *testing.T) {

	// this is the writer, write the serialized procedure to this buffer.
	var bs []byte
	buf := bytes.NewBuffer(bs)
	nw := newNetworkWriter()
	var handle int64 = 51515
	pi := newProcedureInvocation(handle, true, "HELLOWORLD.insert", []driver.Value{}, time.Minute*2)
	nw.serializePI(buf, pi)
	r := bytes.NewReader(buf.Bytes())
	checkSimpleBuffer(t, r, 0, "HELLOWORLD.insert", 51515, 3, "Bonjour", "Monde", "French")
}

func InsertDifferentTypes(t *testing.T) {
	var bs []byte
	buf := bytes.NewBuffer(bs)

	var id int32 = 100
	var nid int32 = 100
	var name string = "Poe"
	var data []byte = []byte("Once upon a midnight dreary, while I pondered, weak and weary, \n" +
		"Over many a quaint and curious volume of forgotten lore— \n" +
		"While I nodded, nearly napping, suddenly there came a tapping, \n" +
		"As of some one gently rapping, rapping at my chamber door.")

	var status int8 = 36
	var typ int16 = -1000
	var pan int64 = 1465474603108
	var bo float64 = -1234.5678
	//bal := big.NewFloat(float64(12345.6789))
	now := time.Date(2016, time.June, 10, 23, 0, 0, 0, time.UTC)

	var handle int64 = 61616
	nw := newNetworkWriter()
	pi := newProcedureInvocation(handle, true, "EXAMPLE_OF_TYPES.insert", []driver.Value{id, nid, name, data, status, typ, pan, bo, now}, time.Minute*2)
	nw.serializePI(buf, pi)

	// read the verification file into a buffer and compare the two buffers
	bs, err := ioutil.ReadFile("./test_resources/verify_insert_types.msg")
	if err != nil {
		t.Errorf(fmt.Sprintf("Read of verification file failed %s", err.Error()))
	}

	bb := buf.Bytes()
	if bytes.Compare(bs, bb) != 0 {
		t.Errorf("Unexpected result, byte buffers don't match")
	}
}

func TestPtrParam(t *testing.T) {
	var f float64 = 451.0
	var bs []byte
	buf := bytes.NewBuffer(bs)
	marshallParam(buf, &f)

	expLen := 9
	if expLen != buf.Len() {
		t.Logf("Unexpected buffer length, expected: %d, actual: %d", expLen, buf.Len())
		t.FailNow()
	}

	r := bytes.NewReader(buf.Bytes())
	verifyInt8At(t, r, 0, VT_FLOAT)
	verifyFloatAt(t, r, 1, f)
}

func TestIntArrayParam(t *testing.T) {
	array := []int32{11, 12, 13}
	var bs []byte
	buf := bytes.NewBuffer(bs)
	marshallParam(buf, array)

	expLen := 18
	if expLen != buf.Len() {
		t.Logf("Unexpected buffer length, expected: %d, actual: %d", expLen, buf.Len())
		t.FailNow()
	}

	var offset int64 = 0
	r := bytes.NewReader(buf.Bytes())
	verifyInt8At(t, r, offset, VT_ARRAY) // verify array type
	offset++

	verifyInt16At(t, r, offset, int16(3)) // verify number of params
	offset += 2

	// verify each int
	for _, exp := range array {
		verifyInt8At(t, r, offset, VT_INT)
		offset++
		verifyInt32At(t, r, offset, exp)
		offset += 4
	}
}

func TestStringArrayParam(t *testing.T) {
	array := []string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven",
		"twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"}
	var bs []byte
	buf := bytes.NewBuffer(bs)
	marshallParam(buf, array)

	expLen := 213
	if expLen != buf.Len() {
		t.Logf("Unexpected buffer length, expected: %d, actual: %d", expLen, buf.Len())
		t.FailNow()
	}

	var offset int64 = 0
	r := bytes.NewReader(buf.Bytes())
	verifyInt8At(t, r, offset, VT_ARRAY) // verify array type
	offset++

	verifyInt16At(t, r, offset, int16(20)) // verify number of params
	offset += 2

	// verify each string
	for _, exp := range array {
		verifyInt8At(t, r, offset, VT_STRING)
		offset++
		verifyStringAt(t, r, offset, exp)
		offset += int64((4 + len(exp)))
	}
}

func TestFloatArrayParam(t *testing.T) {
	array := []float64{-459.67, 32.0, 212.0}
	var bs []byte
	buf := bytes.NewBuffer(bs)
	marshallParam(buf, array)

	expLen := 30
	if expLen != buf.Len() {
		t.Logf("Unexpected buffer length, expected: %d, actual: %d", expLen, buf.Len())
		t.FailNow()
	}
	var offset int64 = 0
	r := bytes.NewReader(buf.Bytes())
	verifyInt8At(t, r, offset, VT_ARRAY) // verify array type
	offset++

	verifyInt16At(t, r, offset, int16(3)) // verify number of params
	offset += 2

	// verify each float
	for _, exp := range array {
		verifyInt8At(t, r, offset, VT_FLOAT)
		offset++
		verifyFloatAt(t, r, offset, exp)
		offset += 8
	}
}

func checkSimpleBuffer(t *testing.T, r *bytes.Reader, expectedBtt byte, expectedPName string, expectedHandle int64,
	expectedNumParams int16, expectedStringParamOne string, expectedStringParamTwo string, expectedStringParamThree string) {
	var offset int64 = 0
	bufLen, err := readInt32At(r, offset)
	if err != nil {
		t.Error("Failed reading length")
		return
	}
	offset += 4

	// batch timeout type
	btt, err := readByteAt(r, offset)
	if err != nil {
		t.Error(fmt.Printf("Failed reading batch timeout type %s", err))
		return
	}
	if btt != expectedBtt {
		t.Error(fmt.Printf("For batch timeout type, expected %b but saw %b\n", expectedBtt, btt))
		return
	}
	offset++

	// procedure name
	pname, err := readStringAt(r, offset)
	if err != nil {
		t.Error("Failed reading procedure name")
		return
	}
	if pname != expectedPName {
		t.Error(fmt.Printf("For procedure name, expected %s but saw %s\n", expectedPName, pname))
		return
	}
	offset = offset + 4 + int64(len(pname))

	// client handle
	handle, err := readInt64At(r, offset)
	if err != nil {
		t.Error(fmt.Printf("Failed reading handle %s\n", err))
		return
	}
	if handle != expectedHandle {
		t.Error(fmt.Printf("For handle, expected %d but saw %d\n", expectedHandle, handle))
		return
	}
	offset += 8

	numParams, err := readInt16At(r, offset)
	if err != nil {
		t.Error(fmt.Printf("Failed reading numParams %s\n", err))
		return
	}
	if numParams != expectedNumParams {
		t.Error(fmt.Printf("For num params, expected %d but saw %d\n", expectedNumParams, numParams))
		return
	}
	offset += 2

	colType, err := readInt8At(r, offset)
	if err != nil {
		t.Error(fmt.Printf("Failed reading colType %s\n", err))
		return
	}
	if colType != VT_STRING {
		t.Error(fmt.Printf("For stringParamOne, expected colType %d but saw %d\n", VT_STRING, colType))
		return
	}
	offset++

	stringParamOne, err := readStringAt(r, offset)
	if err != nil {
		t.Error(fmt.Printf("Failed reading stringParamOne %s\n", err))
		return
	}
	if stringParamOne != expectedStringParamOne {
		t.Error(fmt.Printf("For handle, expected %s but saw %s\n", expectedStringParamOne, stringParamOne))
		return
	}
	offset = offset + 4 + int64(len(stringParamOne))

	colType, err = readInt8At(r, offset)
	if err != nil {
		t.Error(fmt.Printf("Failed reading colType %s\n", err))
		return
	}
	if colType != VT_STRING {
		t.Error(fmt.Printf("For stringParamOne, expected colType %d but saw %d\n", VT_STRING, colType))
		return
	}
	offset++

	stringParamTwo, err := readStringAt(r, offset)
	if err != nil {
		t.Error(fmt.Printf("Failed reading stringParamTwo %s\n", err))
		return
	}
	if stringParamTwo != expectedStringParamTwo {
		t.Error(fmt.Printf("For handle, expected %s but saw %s\n", expectedStringParamTwo, stringParamTwo))
		return
	}
	offset = offset + 4 + int64(len(stringParamTwo))

	colType, err = readInt8At(r, offset)
	if err != nil {
		t.Error(fmt.Printf("Failed reading colType %s\n", err))
		return
	}
	if colType != VT_STRING {
		t.Error(fmt.Printf("For stringParamOne, expected colType %d but saw %d\n", VT_STRING, colType))
		return
	}
	offset++

	stringParamThree, err := readStringAt(r, offset)
	if err != nil {
		t.Error(fmt.Printf("Failed reading stringParamThree %s\n", err))
		return
	}
	if stringParamThree != expectedStringParamThree {
		t.Error(fmt.Printf("For handle, expected %s but saw %s\n", expectedStringParamThree, stringParamThree))
		return
	}
	offset = offset + 4 + int64(len(stringParamThree))

	if int64((bufLen + 4)) != offset {
		t.Error("Failed to read all of buffer")
	}
}

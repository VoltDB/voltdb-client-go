package voltdbclient

import (
	"testing"
	"bytes"
	"fmt"
)

func TestWriteProcedureCall(t *testing.T) {

	config := ClientConfig{"", ""}

	// this is the writer, write the serialized procedure to this buffer.
	var bs []byte
	buf := bytes.NewBuffer(bs)
	client := Client{&config, nil, buf, nil, nil, 0}
	var handle int64 = 51515
	client.writeProcedureCall(client.writer, "HELLOWORLD.insert", handle, []interface{}{"Bonjour", "Monde", "French"})
	r := bytes.NewReader(buf.Bytes())
	readTheBuffer(t, r, 0, "HELLOWORLD.insert", 51515, 3, "Bonjour", "Monde", "French")
}

func readTheBuffer(t *testing.T, r *bytes.Reader, expectedBtt byte, expectedPName string, expectedHandle int64,
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
		t.Error("Failed reading batch timeout type %s", err)
		return
	}
	if btt != expectedBtt {
		t.Error(fmt.Printf("For batch timeout type, expected %s but saw %s\n", expectedBtt, btt))
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
		t.Error("Failed reading handle %s", err)
		return
	}
	if handle != expectedHandle {
		t.Error(fmt.Printf("For handle, expected %s but saw %s\n", expectedHandle, handle))
		return
	}
	offset += 8

	numParams, err := readInt16At(r, offset)
	if err != nil {
		t.Error("Failed reading numParams %s", err)
		return
	}
	if numParams != expectedNumParams {
		t.Error(fmt.Printf("For handle, expected %s but saw %s\n", expectedNumParams, numParams))
		return
	}
	offset += 2

	colType, err := readInt8At(r, offset)
	if err != nil {
		t.Error("Failed reading colType %s", err)
		return
	}
	if colType != VT_STRING {
		t.Error(fmt.Printf("For stringParamOne, expected colType %d but saw %d\n", VT_STRING, colType))
		return
	}
	offset++

	stringParamOne, err := readStringAt(r, offset)
	if err != nil {
		t.Error("Failed reading stringParamOne %s", err)
		return
	}
	if stringParamOne != expectedStringParamOne {
		t.Error(fmt.Printf("For handle, expected %s but saw %s\n", expectedStringParamOne, stringParamOne))
		return
	}
	offset = offset + 4 + int64(len(stringParamOne))

	colType, err = readInt8At(r, offset)
	if err != nil {
		t.Error("Failed reading colType %s", err)
		return
	}
	if colType != VT_STRING {
		t.Error(fmt.Printf("For stringParamOne, expected colType %d but saw %d\n", VT_STRING, colType))
		return
	}
	offset++

	stringParamTwo, err := readStringAt(r, offset)
	if err != nil {
		t.Error("Failed reading stringParamTwo %s", err)
		return
	}
	if stringParamTwo != expectedStringParamTwo {
		t.Error(fmt.Printf("For handle, expected %s but saw %s\n", expectedStringParamTwo, stringParamTwo))
		return
	}
	offset = offset + 4 + int64(len(stringParamTwo))

	colType, err = readInt8At(r, offset)
	if err != nil {
		t.Error("Failed reading colType %s", err)
		return
	}
	if colType != VT_STRING {
		t.Error(fmt.Printf("For stringParamOne, expected colType %d but saw %d\n", VT_STRING, colType))
		return
	}
	offset++

	stringParamThree, err := readStringAt(r, offset)
	if err != nil {
		t.Error("Failed reading stringParamThree %s", err)
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

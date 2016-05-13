package voltdbclient

import (
	"bytes"
	"testing"
)

func TestCallOnClosedConn(t *testing.T) {
	conn := Conn{nil, nil}
	_, err := conn.Call("bad", 1, 2)
	if err == nil {
		t.Errorf("Expected error calling procedure on closed Conn")
	}
}

func TestTableAccessors(t *testing.T) {
	statusCode := 1
	columnCount := 10
	columnTypes := []int8{1, 2, 3}
	columnNames := []string{"abc", "def", "ghi"}
	rowCount := 5
	rows := bytes.NewBufferString("rowbuf")
	table := Table{
		int8(statusCode),
		int16(columnCount),
		columnTypes,
		columnNames,
		int32(rowCount),
		*rows}

	if table.StatusCode() != statusCode {
		t.Errorf("Bad StatusCode()")
	}
	if table.ColumnCount() != columnCount {
		t.Errorf("Bad ColumnCount()")
	}
	if table.ColumnTypes()[0] != columnTypes[0] {
		t.Errorf("Bad ColumnTypes(). Have %v. Expected %v",
			table.ColumnTypes()[0], columnTypes[0])
	}
	if table.ColumnNames()[1] != columnNames[1] {
		t.Errorf("Bad ColumnNames(). Have %v. Expected %v",
			table.ColumnNames()[1], columnNames[1])
	}
	if table.RowCount() != rowCount {
		t.Errorf("Bad RowCount()")
	}
}

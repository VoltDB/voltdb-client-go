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
	"testing"
)

func TestTableAccessors(t *testing.T) {
	statusCode := 1
	columnCount := 10
	columnTypes := []int8{1, 2, 3}
	columnNames := []string{"abc", "def", "ghi"}
	rowCount := 5
	rows := make([][]byte, 1)
	rows[0] = []byte("rowbuf")

	table := NewVoltTable(
		int8(statusCode),
		int16(columnCount),
		columnTypes,
		columnNames,
		int32(rowCount),
		rows)

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

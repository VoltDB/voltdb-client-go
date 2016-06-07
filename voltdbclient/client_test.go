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
	"io/ioutil"
	"bytes"
	"fmt"
	"strings"
)

func TestCallOnClosedConn(t *testing.T) {
	client := Client{nil, nil, nil, nil, nil, 0}
	_, err := client.Call("bad", 1, 2)
	if err == nil {
		t.Errorf("Expected error calling procedure on closed Conn")
	}
}

func TestReadDataTypes(t *testing.T) {
	b, err := ioutil.ReadFile("./test_resources/examples_of_types.msg")
	check(t, err)
	r := bytes.NewReader(b)
	nl := NewListener(r)
	resp, err := nl.readOneMsg(r)
	check(t, err)

	if resp.tableCount != 1 {
		t.Fatal("Unexpected table count");
	}
	vt := resp.tables[0]

	// expected columns
	expCols := []string{"ID", "NULLABLE_ID", "NAME", "DATA", "STATUS", "TYPE", "PAN", "BALANCE_OPEN", "BALANCE", "LAST_UPDATED"}
	if len(expCols) != vt.ColumnCount() {
		t.Fatal("Unexpected column count")
	}

	if vt.rowCount != 3 {
		t.Fatal("Unexpected row count")
	}


	for i := 0; i < 3; i++ {
		vtr, err := vt.FetchRow(int32(i))
		check(t, err)
		id, null, err := vtr.GetInteger(0)
		check(t, err)
		if null {
			t.Fatal("See null id")
		}
		if id == 25 {
			checkRowData(t, vtr, int32(25), true, 0, true, "", true, "", 0, true, 0, true, 0, true, 0, true, 0,
			true, 0, true, "")
		} else if id == 50 {
			checkRowData(t, vtr, int32(50), false, 50, false, "Beckett", false, "Every word is like", 67,
				false, int8(36), false, int16(-500), false, int64(1465242120398), false, float64(-2469.1356),
			false, float64(12345.678900000000), false, "2016-06-06 15:42:00.398 -0400 EDT")
		} else {
			checkRowData(t, vtr, int32(100), false, 100, false, "Poe", false, "Once upon a midnight dreary", 246,
				false, int8(36), false, int16(-1000), false, int64(1465242120384), false, float64(-1234.5678),
			false, float64(12345.678900000000), false, "2016-06-06 15:42:00.385 -0400 EDT")
		}
	}
}

func checkRowData(t *testing.T, row *VoltTableRow, expectedId int32, nIdIsNull bool, expectedNId int32,
nameIsNull bool, expectedName string, dataIsNull bool, expectedPrefix string, expectedDataLen int,
statusIsNull bool, expectedStatus int8, typeIsNull bool, expectedType int16, panIsNull bool, expectedPan int64,
boIsNull bool, expectedBo float64, balanceIsNull bool, expectedBalance float64,
lastUpdatedIsNull bool, expectedLastUpdated string) {
	// ID
	id, null, err := row.GetIntegerByName("ID")
	check(t, err)
	if null {
		t.Error("Unexpected null value for ID\n")
	}
	if expectedId != id {
		t.Error(fmt.Printf("For ID, expected %d but saw %d\n", expectedId, id))
	}

	// NULLABLE_ID
	n_id, null, err := row.GetIntegerByName("NULLABLE_ID")
	check(t, err)
	if null {
		if !nIdIsNull {
			t.Error(fmt.Printf("For NULLABLE_ID, expected %d but saw null\n", expectedId))
		}
	} else if expectedNId != n_id {
		t.Error(fmt.Printf("For NULLABLE_ID, expected %d but saw %d\n", expectedId, id))
	}

	// NAME
	name, null, err := row.GetStringByName("NAME")
	check(t, err)
	if null {
		if !nameIsNull {
			t.Error("Unexpected null value for NAME\n")
		}
	} else if expectedName != name {
		t.Error(fmt.Printf("For NAME, expected %s but saw %s\n", expectedName, name))
	}

	// DATA
	data, null, err := row.GetVarbinaryByName("DATA")
	check(t, err)
	if null {
		if !dataIsNull {
			t.Error("Unexpected null value for DATA\n")
		}
	} else {
		if !strings.HasPrefix(string(data), expectedPrefix) {
			t.Error(fmt.Printf("For DATA, expected data to start with %s\n", expectedPrefix))
		} else if expectedDataLen != len(data) {
			t.Error(fmt.Printf("For DATA, expected length %d but saw length %d\n", expectedDataLen, len(data)))
		}
	}

	// STATUS
	status, null, err := row.GetTinyIntByName("STATUS")
	check(t, err)
	if null {
		if !statusIsNull {
			t.Error("Unexpected null value for STATUS\n")
		}
	} else if expectedStatus != status {
		t.Error(fmt.Printf("For STATUS, expected %d but saw %d\n", expectedStatus, status))
	}

	// TYPE
	typ, null, err := row.GetSmallIntByName("TYPE")
	check(t, err)
	if null {
		if !typeIsNull {
			t.Error("Unexpected null value for TYPE\n")
		}
	} else if expectedType != typ {
		t.Error(fmt.Printf("For TYPE, expected %d but saw %d\n", expectedType, typ))
	}

	// PAN
	pan, null, err := row.GetBigIntByName("PAN")
	check(t, err)
	if null {
		if !panIsNull {
			t.Error("Unexpected null value for PAN\n")
		}
	} else if expectedPan != pan {
		t.Error(fmt.Printf("For PAN, expected %d but saw %d\n", expectedPan, pan))
	}

	// BALANCE_OPEN
	bo, null, err := row.GetFloatByName("BALANCE_OPEN")
	check(t, err)
	if null {
		if !boIsNull {
			t.Error("Unexpected null value for BALANCE_OPEN\n")
		}
	} else if expectedBo != bo {
		t.Error(fmt.Printf("For BALANCE_OPEN, expected %d but saw %d\n", expectedBo, bo))
	}

	// BALANCE
	balance, null, err := row.GetDecimalByName("BALANCE")
	check(t, err)
	if null {
		if !balanceIsNull {
			t.Error("Unexpected null value for BALANCE\n")
		}
	} else {
		fl, _ := balance.Float64()
		if expectedBalance != fl {
			t.Error(fmt.Printf("For BALANCE, expected %d but saw %d\n", expectedBalance, fl))
		}
	}

	lu, null, err := row.GetTimestampByName("LAST_UPDATED")
	check(t, err)
	if null {
		if ! lastUpdatedIsNull {
			t.Error("Unexpected null value for LAST_UPDATED\n")
		}
	} else {
		s := lu.String()
		if expectedLastUpdated != s {
			t.Error(fmt.Printf("For LAST_UPDATED, expected %s but saw %s\n", expectedLastUpdated, s))
		}
	}
}

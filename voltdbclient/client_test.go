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
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"
)

/* TODO: fix CallOnClosedConn
func CallOnClosedConn(t *testing.T) {
	conn := newNodeConn("", nil)
	pi := newSyncProcedureInvocation(0, true, "HELLOWORLD.select", []driver.Value{}, time.Minute*2)
	_, err := conn.query(pi, func(int32) {})
	if err == nil {
		t.Errorf("Expected error calling procedure on closed Conn")
	}
}
*/

/* TODO: fix ReadDataTypes
func ReadDataTypes(t *testing.T) {
	b, err := ioutil.ReadFile("./test_resources/examples_of_types.msg")
	check(t, err)
	r := bytes.NewReader(b)

	nl := newNetworkListener(nil, "")
	var handle int64 = 1
	pi := newSyncProcedureInvocation(handle, true, "HELLOWORLD.select", []driver.Value{}, time.Minute*2)
	ch := nl.registerSyncRequest(nil, pi)

	nl.readResponse(r, handle)
	vr := <-ch
	vrows := vr.(VoltRows)

	expCols := []string{"ID", "NULLABLE_ID", "NAME", "DATA", "STATUS", "TYPE", "PAN", "BALANCE_OPEN", "BALANCE", "LAST_UPDATED"}
	actCols := vrows.Columns()
	if len(expCols) != len(actCols) {
		t.Logf("Unexpected buffer length, expected: %d, actual: %d", len(expCols), len(actCols))
		t.FailNow()
	}

	for i, actCol := range actCols {
		if actCol != expCols[i] {
			t.Logf("Unexpected column name, expected: %s, actual: %s", expCols[i], actCol)
			t.FailNow()
		}
	}

	if !vrows.AdvanceRow() {
		t.Logf("Didn't see expected row")
		t.FailNow()
	}
	iID, err := vrows.GetInteger(0)
	id := iID.(int32)
	check(t, err)
	checkRows(t, vrows, id)

	if !vrows.AdvanceRow() {
		t.Logf("Didn't see expected row")
		t.FailNow()
	}
	iID, err = vrows.GetInteger(0)
	id = iID.(int32)
	check(t, err)
	checkRows(t, vrows, id)

	if !vrows.AdvanceRow() {
		t.Logf("Didn't see expected row")
		t.FailNow()
	}
	iID, err = vrows.GetInteger(0)
	id = iID.(int32)
	check(t, err)
	checkRows(t, vrows, id)

	if vrows.AdvanceRow() {
		t.Logf("Saw unexpected row")
		t.FailNow()
	}
}
*/

func checkRows(t *testing.T, rows VoltRows, id int32) {
	if id == 25 {
		checkRowData(t, rows, int32(25), true, 0, true, "", true, "", 0, true, 0, true, 0, true, 0, true, 0,
			true, 0, true, "")
	} else if id == 50 {
		checkRowData(t, rows, int32(50), false, 50, false, "Beckett", false, "Every word is like", 67,
			false, int8(36), false, int16(-500), false, int64(1465242120398), false, float64(-2469.1356),
			false, float64(12345.678900000000), false, "2016-06-06 15:42:00.398 -0400 EDT")
	} else {
		checkRowData(t, rows, int32(100), false, 100, false, "Poe", false, "Once upon a midnight dreary", 246,
			false, int8(36), false, int16(-1000), false, int64(1465242120384), false, float64(-1234.5678),
			false, float64(12345.678900000000), false, "2016-06-06 15:42:00.385 -0400 EDT")
	}
}

func checkRowData(t *testing.T, rows VoltRows, expectedID int32, nIDIsNull bool, expectedNID int32,
	nameIsNull bool, expectedName string, dataIsNull bool, expectedPrefix string, expectedDataLen int,
	statusIsNull bool, expectedStatus int8, typeIsNull bool, expectedType int16, panIsNull bool, expectedPan int64,
	boIsNull bool, expectedBo float64, balanceIsNull bool, expectedBalance float64,
	lastUpdatedIsNull bool, expectedLastUpdated string) {
	// ID
	iID, err := rows.GetIntegerByName("ID")
	if err != nil {
		t.Fatal(err)
	}
	id := iID.(int32)
	if expectedID != id {
		t.Error(fmt.Printf("For ID, expected %d but saw %d\n", expectedID, id))
	}

	// NULLABLE_ID
	iNID, err := rows.GetIntegerByName("NULLABLE_ID")
	if err != nil {
		t.Fatal(err)
	}
	if iNID != nil {
		nID := iNID.(int32)
		if nIDIsNull || expectedNID != nID {
			t.Error(fmt.Printf("For NULLABLE_ID, expected value %d", expectedNID))
		}
	} else {
		if !nIDIsNull {
			t.Error("Unexpected null value for NULLABLE_ID\n")
		}
	}

	// NAME
	iName, err := rows.GetStringByName("NAME")
	if err != nil {
		t.Fatal(err)
	}
	if iName != nil {
		name := iName.(string)
		if nameIsNull || expectedName != name {
			t.Logf(fmt.Sprintf("For NAME, expected value %v", expectedName))
			t.FailNow()
		}
	} else {
		if !nameIsNull {
			t.Error("Unexpected null value for NAME\n")
		}
	}

	// DATA
	iData, err := rows.GetVarbinaryByName("DATA")
	if err != nil {
		t.Fatal(err)
	}
	if iData != nil {
		data := iData.([]byte)
		if dataIsNull || !strings.HasPrefix(string(data), expectedPrefix) {
			t.Error(fmt.Printf("For DATA, expected value to start with %s", expectedPrefix))
		}
	} else {
		if !dataIsNull {
			t.Error("Unexpected null value for DATA\n")
		}
	}

	// STATUS
	iStatus, err := rows.GetTinyIntByName("STATUS")
	if err != nil {
		t.Fatal(err)
	}
	if iStatus != nil {
		status := iStatus.(int8)
		if statusIsNull || expectedStatus != status {
			t.Error(fmt.Printf("For STATUS, expected value %d", expectedStatus))
		}
	} else {
		if !statusIsNull {
			t.Error("Unexpected null value for STATUS\n")
		}
	}

	// TYPE
	iType, err := rows.GetSmallIntByName("TYPE")
	if err != nil {
		t.Fatal(err)
	}
	if iType != nil {
		typ := iType.(int16)
		if typeIsNull || expectedType != typ {
			t.Error(fmt.Printf("For TYPE, expected value %d", expectedType))
		}
	} else {
		if !typeIsNull {
			t.Error("Unexpected null value for TYPE\n")
		}
	}

	// PAN
	iPan, err := rows.GetBigIntByName("PAN")
	if err != nil {
		t.Fatal(err)
	}
	if iPan != nil {
		pan := iPan.(int64)
		if panIsNull || expectedPan != pan {
			t.Error(fmt.Printf("For PAN, expected value %d", expectedPan))
		}
	} else {
		if !panIsNull {
			t.Error("Unexpected null value for PAN\n")
		}
	}

	// BALANCE_OPEN
	iBo, err := rows.GetFloatByName("BALANCE_OPEN")
	if err != nil {
		t.Fatal(err)
	}
	if iBo != nil {
		bo := iBo.(float64)
		if boIsNull || expectedBo != bo {
			t.Error(fmt.Printf("For BALANCE_OPEN, expected value %f", expectedBo))
		}
	} else {
		if !boIsNull {
			t.Error("Unexpected null value for BALANCE_OPEN\n")
		}
	}

	// BALANCE
	iBalance, err := rows.GetDecimalByName("BALANCE")
	if err != nil {
		t.Fatal(err)
	}
	if iBalance != nil {
		balance := iBalance.(*big.Float)
		fl, _ := balance.Float64()
		if balanceIsNull || expectedBalance != fl {
			t.Error(fmt.Printf("For BALANCE, expected value %f", expectedBalance))
		}
	} else {
		if !balanceIsNull {
			t.Error("Unexpected null value for BALANCE\n")
		}
	}

	// LAST_UPDATED
	iLu, err := rows.GetTimestampByName("LAST_UPDATED")
	if err != nil {
		t.Fatal(err)
	}
	if iLu != nil {
		lu := iLu.(time.Time)
		if lastUpdatedIsNull || expectedLastUpdated != lu.String() {
			t.Error(fmt.Printf("For LAST_UPDATED, expected value %s", expectedLastUpdated))
		}
	} else {
		if !lastUpdatedIsNull {
			t.Error("Unexpected null value for LAST_UPDATED\n")
		}
	}
}

func TestDDLResult(t *testing.T) {

	// Please comment the following line  to run this test
	//
	// This is intentionally skipped as it requires a live voltdb connection to
	// run. If there is a running voltdb instance you can comment t.Skip() and
	// adjust the connection string to point to your database instance.
	t.Skip()
	db, err := sql.Open("voltdb", "localhost:21212")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := `
	CREATE TABLE foo(
		n INTEGER,
	);
	`
	r, err := db.Exec("@AdHoc", s)
	if err != nil {
		t.Fatal(err)
	}
	last, err := r.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if last != 0 {
		t.Errorf("expected 0 got %d", last)
	}
	rows, err := r.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Errorf("expected 0 got %d", rows)
	}
}

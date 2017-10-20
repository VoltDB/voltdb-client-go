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

// VoltResult is an implementation of database/sql/driver.Result
type VoltResult struct {
	voltResponse
	rowsAff []int64
	ti      int
}

func newVoltResult(resp voltResponse, rowsAff []int64) *VoltResult {
	return &VoltResult{
		voltResponse: resp,
		rowsAff:      rowsAff,
	}
}

// AdvanceTable advances to the next table. Returns false if there isn't a next
// table.
func (vr *VoltResult) AdvanceTable() bool {
	return vr.AdvanceToTable(vr.ti + 1)
}

// AdvanceToTable advances to the table indicated by the index. Returns false if
// there is no table at the given index.
func (vr *VoltResult) AdvanceToTable(ti int) bool {
	if ti >= len(vr.rowsAff) || ti < 0 {
		return false
	}
	vr.ti = ti
	return true
}

// LastInsertId is not populated by VoltDB, calls to LastInsertId return 0.
func (vr VoltResult) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected returns the number of rows affected by the query.
func (vr VoltResult) RowsAffected() (int64, error) {
	return vr.rowsAff[vr.ti], nil
}

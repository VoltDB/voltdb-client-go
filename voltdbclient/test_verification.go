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
)

func verifyInt8At(t *testing.T, r *bytes.Reader, off int64, exp int8) {
	act, err := readInt8At(r, off)
	if err != nil {
		t.Logf(err.Error())
		t.FailNow()
	}
	if exp != act {
		t.Logf("Expected: %d, actual: %d", exp, act)
		t.FailNow()
	}
}

func verifyInt16At(t *testing.T, r *bytes.Reader, off int64, exp int16) {
	act, err := readInt16At(r, off)
	if err != nil {
		t.Logf(err.Error())
		t.FailNow()
	}
	if exp != act {
		t.Logf("Expected: %d, actual: %d", exp, act)
		t.FailNow()
	}
}

func verifyInt32At(t *testing.T, r *bytes.Reader, off int64, exp int32) {
	act, err := readInt32At(r, off)
	if err != nil {
		t.Logf(err.Error())
		t.FailNow()
	}
	if exp != act {
		t.Logf("Expected: %d, actual: %d", exp, act)
		t.FailNow()
	}
}

func verifyFloatAt(t *testing.T, r *bytes.Reader, off int64, exp float64) {
	act, err := readFloatAt(r, off)
	if err != nil {
		t.Logf(err.Error())
		t.FailNow()
	}
	if exp != act {
		t.Logf("Expected: %f, actual: %f", exp, act)
		t.FailNow()
	}
}

func verifyStringAt(t *testing.T, r *bytes.Reader, off int64, exp string) {
	act, err := readStringAt(r, off)
	if err != nil {
		t.Logf(err.Error())
		t.FailNow()
	}
	if exp != act {
		t.Logf("Expected: %s, actual: %s", exp, act)
		t.FailNow()
	}
}

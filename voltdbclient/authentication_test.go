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
	"bytes"
	"io/ioutil"
	"testing"
)

func TestLoginResponse(t *testing.T) {
	b, err := ioutil.ReadFile("./test_resources/authentication_response.msg")
	check(t, err)
	reader := bytes.NewReader(b)
	connData, err := readLoginResponse(reader)
	check(t, err)
	// consider as passed if returns non nil connection data
	if connData == nil {
		t.Fatal("login response didn't return connection data")
	}
}

func check(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err.Error())
	}
}

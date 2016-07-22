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
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
)

type procedureInvocation struct {
	handle  int64
	isQuery bool // as opposed to an exec.
	query   string
	params  []driver.Value
	timeout time.Duration
	slen    int // length of pi once serialized
}

func newProcedureInvocation(handle int64, isQuery bool, query string, params []driver.Value, timeout time.Duration) *procedureInvocation {
	var pi = new(procedureInvocation)
	pi.handle = handle
	pi.isQuery = isQuery
	pi.query = query
	pi.params = params
	pi.timeout = timeout
	pi.slen = -1
	return pi
}

func (pi *procedureInvocation) getLen() int {
	if pi.slen == -1 {
		pi.slen = pi.calcLen()
	}
	return pi.slen
}

func (pi *procedureInvocation) calcLen() int {
	// fixed - 1 for batch timeout type, 4 for str length (proc name), 8 for handle, 2 for paramCount
	var slen int = 15
	slen += len(pi.query)
	for _, param := range pi.params {
		slen += pi.calcParamLen(param)
	}
	return slen
}

func (pi *procedureInvocation) calcParamLen(param interface{}) int {
	// add one to each because the type itself takes one byte
	// nil is 1
	if param == nil {
		return 1
	}
	v := reflect.ValueOf(param)
	switch v.Kind() {
	case reflect.Bool:
		return 2
	case reflect.Int8:
		return 2
	case reflect.Int16:
		return 3
	case reflect.Int32:
		return 5
	case reflect.Int64:
		return 9
	case reflect.Float64:
		return 9
	case reflect.String:
		return 5 + v.Len()
	case reflect.Slice:
		return 5 + v.Len()
	case reflect.Struct:
		panic("Can't marshal a struct")
	case reflect.Ptr:
		panic("Can't marshal a pointer")
	default:
		panic(fmt.Sprintf("Can't marshal %v-type parameters", v.Kind()))
	}
}

func (pi procedureInvocation) getPassedParamCount() int {
	return len(pi.params)
}

func (pi procedureInvocation) getPartitionParamValue(index int) driver.Value {
	return pi.params[index]
}

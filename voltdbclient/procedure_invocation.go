/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
)

type procedureInvocation struct {

	// This is a unique integer that is used to identify the procedure invocation.
	handle  int64
	isQuery bool // as opposed to an exec.

	// The SQL query to be sent to voltdb
	query  string
	params []driver.Value

	// The channel on which response from voltdb will be sent. The node connection
	// thwt will write on this channel is stored in the conn field.
	responseCh chan voltResponse
	timeout    time.Duration
	arc        AsyncResponseConsumer
	async      bool

	// This is the connection that received the invocation request. It is through
	// this connection that the response to the procedure invocation will be sent.
	conn   *nodeConn
	cancel func()
	stop   func()
}

func newSyncProcedureInvocation(handle int64, isQuery bool, query string, params []driver.Value, responseCh chan voltResponse, timeout time.Duration) *procedureInvocation {
	return &procedureInvocation{
		handle:     handle,
		isQuery:    isQuery,
		query:      query,
		params:     params,
		responseCh: responseCh,
		timeout:    timeout,
		async:      false,
	}
}

func newAsyncProcedureInvocation(handle int64, isQuery bool, query string, params []driver.Value, timeout time.Duration, arc AsyncResponseConsumer) *procedureInvocation {
	return &procedureInvocation{
		handle:  handle,
		isQuery: isQuery,
		query:   query,
		params:  params,
		timeout: timeout,
		arc:     arc,
		async:   true,
	}
}

// a procedure invocation that will be processed based on its handle.
func newProcedureInvocationByHandle(handle int64, isQuery bool, query string, params []driver.Value) *procedureInvocation {
	return &procedureInvocation{
		handle:  handle,
		isQuery: isQuery,
		query:   query,
		params:  params,
		async:   true,
	}
}

func (pi *procedureInvocation) getLen() int {
	return pi.calcLen()
}

func (pi *procedureInvocation) calcLen() int {
	// fixed - 1 for batch timeout type, 4 for str length (proc name),
	// 8 for handle, 2 for paramCount
	var slen = 15
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
		if _, ok := v.Interface().(time.Time); ok {
			return 9
		}
		panic("Can't determine length of struct")

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

func (pi procedureInvocation) isAsync() bool {
	return pi.async
}

// This will wait until the context ctx is cancelled/timeout. If it timeout then
// we handle this procedure invocation as timedout.
//
// This is blocking, so call it in a separate goroutine.
func (pi *procedureInvocation) handleTimeoutsAndCancel(ctx context.Context) {
	if pi.cancel != nil {
		<-ctx.Done()

		// If the parent context was cancelled nothing will happen. We just make sure
		// that the timeout is handled.
		//
		// We call cancel to clear any resources associated with this procedure
		// invocation.
		if ctx.Err() == context.DeadlineExceeded {
			pi.conn.handleTimeout(pi)
		}
		pi.cancel()
	}
}

func (pi *procedureInvocation) Close() {
	if pi.stop != nil {
		pi.stop()
	}
}

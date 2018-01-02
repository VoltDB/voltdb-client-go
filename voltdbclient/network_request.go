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
	"time"
)

type networkRequest struct {
	handle int64
	query  bool
	ch     chan voltResponse
	sync   bool
	arc    AsyncResponseConsumer
	// the size of the serialized request written to the server.
	numBytes  int
	timeout   time.Duration
	submitted time.Time
}

func newSyncRequest(handle int64, ch chan voltResponse, isQuery bool, numBytes int, timeout time.Duration, submitted time.Time) *networkRequest {
	return &networkRequest{
		handle:    handle,
		query:     isQuery,
		ch:        ch,
		sync:      true,
		arc:       nil,
		numBytes:  numBytes,
		submitted: submitted,
		timeout:   timeout,
	}
}

func newAsyncRequest(handle int64, ch chan voltResponse, isQuery bool, arc AsyncResponseConsumer, numBytes int, timeout time.Duration, submitted time.Time) *networkRequest {
	return &networkRequest{
		handle:    handle,
		query:     isQuery,
		ch:        ch,
		sync:      false,
		arc:       arc,
		numBytes:  numBytes,
		submitted: submitted,
		timeout:   timeout,
	}
}

func (nr *networkRequest) getArc() AsyncResponseConsumer {
	return nr.arc
}

func (nr *networkRequest) isSync() bool {
	return nr.sync
}

func (nr *networkRequest) isQuery() bool {
	return nr.query
}

func (nr *networkRequest) getChan() chan voltResponse {
	return nr.ch
}

func (nr *networkRequest) getNumBytes() int {
	return nr.numBytes
}

// wrap these two things together so that they can go on the asyncs channel.
type asyncVoltResponse struct {
	vr  voltResponse
	arc AsyncResponseConsumer
}

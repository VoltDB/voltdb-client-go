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

import "fmt"

// Response is a stored procedure result.
type Response struct {
	clientHandle         int64
	fieldsPresent        uint8
	status               int8
	statusString         string
	appStatus            int8
	appStatusString      string
	clusterRoundTripTime int32
	exceptionLength      int32
	exceptionBytes       []byte
	tableCount           int16
	tables               []*VoltTable
}

// Response status codes
type Status int

const (
	SUCCESS            Status = 1
	USER_ABORT         Status = -1
	GRACEFUL_FAILURE   Status = -2
	UNEXPECTED_FAILURE Status = -3
	CONNECTION_LOST    Status = -4
)

func (s Status) String() string {
	if s == SUCCESS {
		return "SUCCESS"
	} else if s == USER_ABORT {
		return "USER ABORT"
	} else if s == GRACEFUL_FAILURE {
		return "GRACEFUL FAILURE"
	} else if s == UNEXPECTED_FAILURE {
		return "UNEXPECTED FAILURE"
	} else if s == CONNECTION_LOST {
		return "CONNECTION LOST"
	} else {
		panic(fmt.Sprintf("Invalid status code: %d", int(s)))
	}
	return "unreachable"
}

func (rsp *Response) Status() Status {
	return Status(rsp.status)
}

func (rsp *Response) StatusString() string {
	return rsp.statusString
}

func (rsp *Response) AppStatus() int {
	return int(rsp.appStatus)
}

func (rsp *Response) AppStatusString() string {
	return rsp.appStatusString
}

func (rsp *Response) ClusterRoundTripTime() int {
	return int(rsp.clusterRoundTripTime)
}

func (rsp *Response) ResultSets() []*VoltTable {
	return rsp.tables
}

func (rsp *Response) Table(offset int) *VoltTable {
	return rsp.tables[offset]
}

func (rsp *Response) TableCount() int16 {
	return rsp.tableCount
}

func (rsp *Response) GoString() string {
	return fmt.Sprintf("Response: clientHandle:%v, status:%v, statusString:%v, "+
		"clusterRoundTripTime: %v, appStatus: %v, appStatusString: %v\n",
		rsp.clientHandle, rsp.status, rsp.statusString,
		rsp.clusterRoundTripTime, rsp.appStatus, rsp.appStatusString)
}

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
	"errors"
	"fmt"
	"io"
	"math"
)

type voltResponse interface {
	getAppStatus() int8
	getAppStatusString() string
	getClusterRoundTripTime() int32
	getHandle() int64
	getStatus() int8
	getStatusString() string

	getError() error
	setError(err error)
}

// helds a processed response, either a VoltResult or a VoltRows
type voltResponseInfo struct {
	handle               int64
	status               int8
	statusString         string
	appStatus            int8
	appStatusString      string
	clusterRoundTripTime int32
	err                  error
}

func newVoltResponseInfo(handle int64, status int8, statusString string, appStatus int8, appStatusString string, clusterRoundTripTime int32, err error) *voltResponseInfo {
	var vrsp = new(voltResponseInfo)
	vrsp.handle = handle
	vrsp.status = status
	vrsp.statusString = statusString
	vrsp.appStatus = appStatus
	vrsp.appStatusString = appStatusString
	vrsp.clusterRoundTripTime = clusterRoundTripTime
	vrsp.err = err
	return vrsp
}

func (vrsp voltResponseInfo) getAppStatus() int8 {
	return vrsp.appStatus
}

func (vrsp voltResponseInfo) getAppStatusString() string {
	return vrsp.appStatusString
}

func (vrsp voltResponseInfo) getClusterRoundTripTime() int32 {
	return vrsp.clusterRoundTripTime
}

func (vrsp voltResponseInfo) getError() error {
	return vrsp.err
}

func (vrsp voltResponseInfo) setError(err error) {
	vrsp.err = err
}

func (vrsp voltResponseInfo) getHandle() int64 {
	return vrsp.handle
}

func (vrsp voltResponseInfo) getStatus() int8 {
	return vrsp.status
}

func (vrsp voltResponseInfo) getStatusString() string {
	return vrsp.statusString
}

// Status codes returned by the VoltDB server.
// Each response to a client Query or Exec has an associated status code.
type ResponseStatus int8

const (
	SUCCESS ResponseStatus = 1
	USER_ABORT ResponseStatus = -1
	GRACEFUL_FAILURE ResponseStatus = -2
	UNEXPECTED_FAILURE ResponseStatus = -3
	CONNECTION_LOST ResponseStatus = -4
)

// Represent a ResponseStatus as a string.
func (rs ResponseStatus) String() string {
	if rs == SUCCESS {
		return "SUCCESS"
	} else if rs == USER_ABORT {
		return "USER ABORT"
	} else if rs == GRACEFUL_FAILURE {
		return "GRACEFUL FAILURE"
	} else if rs == UNEXPECTED_FAILURE {
		return "UNEXPECTED FAILURE"
	} else if rs == CONNECTION_LOST {
		return "CONNECTION LOST"
	} else {
		panic(fmt.Sprintf("Invalid status code: %d", int(rs)))
	}
	return "unreachable"
}

func deserializeResponse(r io.Reader, handle int64) (rsp voltResponse) {
	// Some fields are optionally included in the response.  Which of these optional
	// fields are included is indicated by this byte, 'fieldsPresent'.  The set
	// of optional fields includes 'statusString', 'appStatusString', and 'exceptionLength'.
	fieldsPresent, err := readUint8(r)
	if err != nil {
		return *(newVoltResponseInfo(handle, 0, "", 0, "", 0, err))
	}

	status, err := readByte(r)
	if err != nil {
		return *(newVoltResponseInfo(handle, 0, "", 0, "", 0, err))
	}
	var statusString string
	if ResponseStatus(status) != SUCCESS {
		if fieldsPresent&(1<<5) != 0 {
			statusString, err = readString(r)
			if err != nil {
				return *(newVoltResponseInfo(handle, status, "", 0, "", 0, err))
			}
		}
		errString := fmt.Sprintf("Bad status %s %s\n", ResponseStatus(status).String(), statusString)
		return *(newVoltResponseInfo(handle, status, statusString, 0, "", 0, errors.New(errString)))
	}

	appStatus, err := readByte(r)
	if err != nil {
		return *(newVoltResponseInfo(handle, status, statusString, 0, "", 0, err))
	}
	var appStatusString string
	if appStatus != 0 && appStatus != math.MinInt8 {
		if fieldsPresent&(1<<7) != 0 {
			appStatusString, err = readString(r)
			if err != nil {
				return *(newVoltResponseInfo(handle, status, statusString, appStatus, "", 0, err))
			}
		}
		errString := fmt.Sprintf("Bad app status %d %s\n", appStatus, appStatusString)
		return *(newVoltResponseInfo(handle, status, statusString, 0, "", 0, errors.New(errString)))
	}

	clusterRoundTripTime, err := readInt(r)
	if err != nil {
		return *(newVoltResponseInfo(handle, status, statusString, appStatus, appStatusString, 0, err))
	}

	return *(newVoltResponseInfo(handle, status, statusString, appStatus, appStatusString, clusterRoundTripTime, nil))
}

func deserializeRows(r io.Reader, rsp voltResponse) (rows VoltRows) {
	if rsp.getError() != nil {
		return *(newVoltRows(rsp, 0, nil))
	}
	numTables, err := readShort(r)
	if err != nil {
		rsp.setError(err)
		return *(newVoltRows(rsp, 0, nil))
	}
	if numTables < 0 {
		err := errors.New("Negative value for numTables")
		rsp.setError(err)
		return *(newVoltRows(rsp, 0, nil))
	}
	tables := make([]*voltTable, numTables)
	for idx, _ := range tables {
		if tables[idx], err = deserializeTable(r); err != nil {
			rsp.setError(err)
			return *(newVoltRows(rsp, numTables, nil))
		}
	}
	vr := newVoltRows(rsp, numTables, tables)
	return *vr
}

func deserializeTable(r io.Reader) (*voltTable, error) {
	var err error
	_, err = readInt(r) // ttlLength
	if err != nil {
		return nil, err
	}
	_, err = readInt(r) // metaLength
	if err != nil {
		return nil, err
	}

	statusCode, err := readByte(r)
	if err != nil {
		return nil, err
	}
	// Todo: not sure about this, sometimes see 0 sometimes -128
	if statusCode != 0 && statusCode != math.MinInt8 {
		return nil, errors.New(fmt.Sprintf("Bad return status on table %d", statusCode))
	}

	columnCount, err := readShort(r)
	if err != nil {
		return nil, err
	}

	// column type "array" and column name "array" are not
	// length prefixed arrays. they are really just columnCount
	// len sequences of bytes (types) and strings (names).
	var i int16
	var columnTypes []int8
	for i = 0; i < columnCount; i++ {
		ct, err := readByte(r)
		if err != nil {
			return nil, err
		}
		columnTypes = append(columnTypes, ct)
	}

	var columnNames []string
	for i = 0; i < columnCount; i++ {
		cn, err := readString(r)
		if err != nil {
			return nil, err
		}
		columnNames = append(columnNames, cn)
	}

	rowCount, err := readInt(r)
	if err != nil {
		return nil, err
	}

	rows := make([][]byte, rowCount)
	var offset int64 = 0
	var rowI int32
	for rowI = 0; rowI < rowCount; rowI++ {
		rowLen, _ := readInt(r)
		rows[rowI] = make([]byte, rowLen)
		_, err = r.Read(rows[rowI])
		offset += int64(rowLen + 4)
	}

	return newVoltTable(columnCount, columnTypes, columnNames, rowCount, rows), nil
}

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
	"errors"
	"fmt"
	"io"
	"math"
)

// Response status codes
type Status int8

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

func deserializeResult(r io.Reader, handle int64) (res driver.Result) {
	// Some fields are optionally included in the response.  Which of these optional
	// fields are included is indicated by this byte, 'fieldsPresent'.  The set
	// of optional fields includes 'statusString', 'appStatusString', and 'exceptionLength'.
	fieldsPresent, err := readUint8(r)
	if err != nil {
		return *(newVoltResult(handle, 0, "", 0, "", 0, err))
	}

	status, err := readByte(r)
	if err != nil {
		return *(newVoltResult(handle, 0, "", 0, "", 0, err))
	}
	var statusString string
	if Status(status) != SUCCESS {
		if fieldsPresent&(1<<5) != 0 {
			statusString, err = readString(r)
			if err != nil {
				return *(newVoltResult(handle, status, "", 0, "", 0, err))
			}
		}
		errString := fmt.Sprintf("Bad status %s %s\n", Status(status).String(), statusString)
		return *(newVoltResult(handle, status, statusString, 0, "", 0, errors.New(errString)))
	}

	appStatus, err := readByte(r)
	if err != nil {
		return *(newVoltResult(handle, status, statusString, 0, "", 0, err))
	}
	var appStatusString string
	if appStatus != 0 && appStatus != math.MinInt8 {
		if fieldsPresent&(1<<7) != 0 {
			appStatusString, err = readString(r)
			if err != nil {
				return *(newVoltResult(handle, status, statusString, appStatus, "", 0, err))
			}
		}
		errString := fmt.Sprintf("Bad app status %d %s\n", appStatus, appStatusString)
		return *(newVoltResult(handle, status, statusString, 0, "", 0, errors.New(errString)))
	}

	clusterRoundTripTime, err := readInt(r)
	if err != nil {
		return *(newVoltResult(handle, status, statusString, appStatus, appStatusString, 0, err))
	}

	return *(newVoltResult(handle, status, statusString, appStatus, appStatusString, clusterRoundTripTime, nil))
}

// readCallResponse reads a stored procedure invocation response.
func deserializeRows(r io.Reader, handle int64) (rows driver.Rows) {
	res := deserializeResult(r, handle)
	vres := res.(VoltResult)
	if vres.error() != nil {
		return newVoltRows(vres, 0, nil)
	}
	numTables, err := readShort(r)
	if err != nil {
		vres.setError(err)
		return *(newVoltRows(vres, 0, nil))
	}
	if numTables < 0 {
		err := errors.New("Negative value for numTables")
		vres.setError(err)
		return *(newVoltRows(vres, 0, nil))
	}
	tables := make([]*VoltTable, numTables)
	for idx, _ := range tables {
		if tables[idx], err = deserializeTable(r); err != nil {
			vres.setError(err)
			return *(newVoltRows(vres, numTables, nil))
		}
	}

	vr := newVoltRows(vres, numTables, tables)
	return *vr
}

func deserializeTable(r io.Reader) (*VoltTable, error) {
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

	return NewVoltTable(columnCount, columnTypes, columnNames, rowCount, rows), nil
}

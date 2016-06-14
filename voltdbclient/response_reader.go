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

// reads and deserializes a procedure call response from the server.
func readResponse(r io.Reader) (*VoltRows, error) {
	buf, err := readMessage(r)
	if err != nil {
		return nil, err
	}
	return deserializeCallResponse(buf)
}

// readCallResponse reads a stored procedure invocation response.
func deserializeCallResponse(r io.Reader) (vr *VoltRows, err error) {
	clientHandle, err := readLong(r)
	if err != nil {
		return nil, err
	}

	// Some fields are optionally included in the response.  Which of these optional
	// fields are included is indicated by this byte, 'fieldsPresent'.  The set
	// of optional fields includes 'statusString', 'appStatusString', and 'exceptionLength'.
	fieldsPresent, err := readUint8(r)
	if err != nil {
		return nil, err
	}

	statusB, err := readByte(r)
	if err != nil {
		return nil, err
	}
	status := Status(statusB)
	if status != SUCCESS {
		if fieldsPresent&(1<<5) != 0 {
			statusString, err := readString(r)
			if err != nil {
				return nil, err
			}
			if statusString != "" {
				return nil, errors.New(fmt.Sprintf("Unexpected status in response %d, %s\n", status.String(), statusString))
			} else {
				return nil, errors.New(fmt.Sprintf("Unexpected status in response %d\n", status.String()))

			}
		}
	}

	appStatus, err := readByte(r)
	if err != nil {
		return nil, err
	}
	var appStatusString string
	if fieldsPresent&(1<<7) != 0 {
		appStatusString, err = readString(r)
		if err != nil {
			return nil, err
		}
	}

	clusterRoundTripTime, err := readInt(r)
	if err != nil {
		return nil, err
	}
	tableCount, err := readShort(r)
	if err != nil {
		return nil, err
	}
	if tableCount < 0 {
		return nil, fmt.Errorf("Bad table count in procudure response %v", tableCount)
	}
	tables := make([]*VoltTable, tableCount)
	for idx, _ := range tables {
		if tables[idx], err = deserializeTable(r, clientHandle, appStatus, appStatusString, clusterRoundTripTime); err != nil {
			return nil, err
		}
	}
	// TODO: if more than one table.
	rows := tables[0].Rows()
	return &rows, nil
}

func deserializeTable(r io.Reader, clientHandle int64, appStatus int8, appStatusString string, clusterRoundTripTime int32) (*VoltTable, error) {
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

	return NewVoltTable(clientHandle, appStatus, appStatusString, clusterRoundTripTime, columnCount,
		columnTypes, columnNames, rowCount, rows), nil
}

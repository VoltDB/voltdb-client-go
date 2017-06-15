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
	"errors"
	"fmt"
	"io"
	"math"
)

type voltResponse interface {
	getAppStatus() ResponseStatus
	getAppStatusString() string
	getClusterRoundTripTime() int32
	getHandle() int64
	getNumTables() int16
	getStatus() ResponseStatus
	getStatusString() string
}

// VoltError type
type VoltError struct {
	voltResponse
	error
}

// helds a processed response, either a VoltResult or a VoltRows
type voltResponseInfo struct {
	handle               int64
	status               ResponseStatus
	statusString         string
	appStatus            ResponseStatus
	appStatusString      string
	clusterRoundTripTime int32
	numTables            int16
}

func newVoltResponseInfo(handle int64, status ResponseStatus, statusString string, appStatus ResponseStatus, appStatusString string, clusterRoundTripTime int32, numTables int16) *voltResponseInfo {
	var vrsp = new(voltResponseInfo)
	vrsp.handle = handle
	vrsp.status = status
	vrsp.statusString = statusString
	vrsp.appStatus = appStatus
	vrsp.appStatusString = appStatusString
	vrsp.clusterRoundTripTime = clusterRoundTripTime
	vrsp.numTables = numTables
	return vrsp
}

func emptyVoltResponseInfo() voltResponseInfo {
	var vrsp = new(voltResponseInfo)
	vrsp.handle = 0
	vrsp.status = Success
	vrsp.statusString = ""
	vrsp.appStatus = UninitializedAppStatusCode
	vrsp.appStatusString = ""
	vrsp.clusterRoundTripTime = -1
	vrsp.numTables = 0
	return *vrsp
}

func emptyVoltResponseInfoWithLatency(rtt int32) voltResponseInfo {
	var vrsp = new(voltResponseInfo)
	vrsp.handle = 0
	vrsp.status = Success
	vrsp.statusString = ""
	vrsp.appStatus = UninitializedAppStatusCode
	vrsp.appStatusString = ""
	vrsp.clusterRoundTripTime = rtt
	vrsp.numTables = 0
	return *vrsp
}

func (vrsp voltResponseInfo) getAppStatus() ResponseStatus {
	return vrsp.appStatus
}

func (vrsp voltResponseInfo) getAppStatusString() string {
	return vrsp.appStatusString
}

func (vrsp voltResponseInfo) getClusterRoundTripTime() int32 {
	return vrsp.clusterRoundTripTime
}

func (vrsp voltResponseInfo) getHandle() int64 {
	return vrsp.handle
}

func (vrsp voltResponseInfo) getNumTables() int16 {
	return vrsp.numTables
}

func (vrsp voltResponseInfo) getStatus() ResponseStatus {
	return vrsp.status
}

func (vrsp voltResponseInfo) getStatusString() string {
	return vrsp.statusString
}

// ResponseStatus handles the Status codes returned by the VoltDB server.
// Each response to a client Query or Exec has an associated status code.
type ResponseStatus int8

// The available ResponseStatus codes
const (
	Success                    ResponseStatus = 1
	UserAbort                  ResponseStatus = -1
	GracefulFailure            ResponseStatus = -2
	UnexpectedFailure          ResponseStatus = -3
	ConnectionLost             ResponseStatus = -4
	ServerUnavailable          ResponseStatus = -5
	ConnectionTimeout          ResponseStatus = -6
	ResponseUnknown            ResponseStatus = -7
	TXNRestart                 ResponseStatus = -8
	OperationalFailure         ResponseStatus = -9
	UninitializedAppStatusCode ResponseStatus = -128
)

// Represent a ResponseStatus as a string.
func (rs ResponseStatus) String() string {
	if rs == Success {
		return "SUCCESS"
	} else if rs == UserAbort {
		return "USER ABORT"
	} else if rs == GracefulFailure {
		return "GRACEFUL FAILURE"
	} else if rs == UnexpectedFailure {
		return "UNEXPECTED FAILURE"
	} else if rs == ConnectionLost {
		return "CONNECTION LOST"
	} else if rs == ServerUnavailable {
		return "SERVER UNAVAILABLE"
	} else if rs == ConnectionTimeout {
		return "CONNECTION TIMEOUT"
	} else if rs == ResponseUnknown {
		return "RESPONSE UNKNOWN"
	} else if rs == TXNRestart {
		return "TXN RESTART"
	} else if rs == OperationalFailure {
		return "OPERATIONAL FAILURE"
	} else if rs == UninitializedAppStatusCode {
		return "UNINITIALIZED APP STATUS CODE"
	}
	panic(fmt.Sprintf("Invalid status code: %d", int(rs)))
}

func deserializeResponse(r io.Reader, handle int64) (rsp voltResponse, volterr error) {
	// Some fields are optionally included in the response.  Which of these optional
	// fields are included is indicated by this byte, 'fieldsPresent'.  The set
	// of optional fields includes 'statusString', 'appStatusString', and 'exceptionLength'.
	fieldsPresent, err := readUint8(r)
	if err != nil {
		return nil, VoltError{voltResponse: emptyVoltResponseInfo(), error: err}
	}

	b, err := readByte(r)
	status := ResponseStatus(b)
	if err != nil {
		return nil, VoltError{voltResponse: emptyVoltResponseInfo(), error: err}
	}
	var statusString string
	if status != Success {
		if fieldsPresent&(1<<5) != 0 {
			statusString, err = readString(r)
			if err != nil {
				return nil, VoltError{voltResponse: emptyVoltResponseInfo(), error: err}
			}
		}
		errString := fmt.Sprintf("Bad status %s %s\n", ResponseStatus(status).String(), statusString)
		return nil, VoltError{voltResponse: emptyVoltResponseInfo(), error: errors.New(errString)}
	}

	b, err = readByte(r)
	appStatus := ResponseStatus(b)
	if err != nil {
		return nil, VoltError{voltResponse: emptyVoltResponseInfo(), error: err}
	}
	var appStatusString string
	if appStatus != 0 && appStatus != math.MinInt8 {
		if fieldsPresent&(1<<7) != 0 {
			appStatusString, err = readString(r)
			if err != nil {
				return nil, VoltError{voltResponse: emptyVoltResponseInfo(), error: err}
			}
		}
		errString := fmt.Sprintf("Bad app status %d %s\n", appStatus, appStatusString)
		return nil, VoltError{voltResponse: emptyVoltResponseInfo(), error: errors.New(errString)}
	}

	clusterRoundTripTime, err := readInt(r)
	if err != nil {
		return nil, VoltError{voltResponse: emptyVoltResponseInfo(), error: err}
	}

	numTables, err := readShort(r)
	if err != nil {
		return *(newVoltRows(rsp, nil)), VoltError{voltResponse: emptyVoltResponseInfoWithLatency(clusterRoundTripTime), error: err}
	}

	if numTables < 0 {
		err := errors.New("Negative value for numTables")
		return *(newVoltRows(rsp, nil)), VoltError{voltResponse: emptyVoltResponseInfoWithLatency(clusterRoundTripTime), error: err}
	}

	return *(newVoltResponseInfo(handle, status, statusString, appStatus, appStatusString, clusterRoundTripTime, numTables)), nil
}

func deserializeResult(r io.Reader, rsp voltResponse) (VoltResult, error) {
	numTables := rsp.getNumTables()
	ras := make([]int64, numTables)
	var i int16
	for ; i < numTables; i++ {
		ra, err := deserializeTableForResult(r)
		if err != nil {
			return *(newVoltResult(rsp, []int64{0})), VoltError{voltResponse: rsp, error: err}
		}
		ras[i] = ra
	}
	res := newVoltResult(rsp, ras)
	return *res, nil
}

func deserializeRows(r io.Reader, rsp voltResponse) (VoltRows, error) {
	var err error
	numTables := rsp.getNumTables()
	tables := make([]*voltTable, numTables)
	for idx := range tables {
		if tables[idx], err = deserializeTableForRows(r); err != nil {
			return *(newVoltRows(rsp, nil)), VoltError{voltResponse: rsp, error: err}
		}
	}
	vr := newVoltRows(rsp, tables)
	return *vr, nil
}

func deserializeTableCommon(r io.Reader) (colCount int16, err error) {
	_, err = readInt(r) // ttlLength
	if err != nil {
		return 0, err
	}
	_, err = readInt(r) // metaLength
	if err != nil {
		return 0, err
	}

	statusCode, err := readByte(r)
	if err != nil {
		return 0, err
	}

	// Todo: not sure about this, sometimes see 0 sometimes -128
	if statusCode != 0 && statusCode != math.MinInt8 {
		return 0, fmt.Errorf("Bad return status on table %d", statusCode)
	}

	colCount, err = readShort(r)
	if err != nil {
		return 0, err
	}
	return colCount, nil
}

// for a result, care only about the number of rows.
func deserializeTableForResult(r io.Reader) (rowsAff int64, err error) {

	var colCount int16
	colCount, err = deserializeTableCommon(r)
	if err != nil {
		return 0, err
	}
	if colCount != 1 {
		return 0, errors.New("Unexpected number of columns for result")
	}

	colType, err := readByte(r)
	if err != nil {
		return 0, err
	}
	if colType != 6 {
		return 0, errors.New("Unexpected columntype for result")
	}

	cname, err := readString(r)
	if err != nil {
		return 0, err
	}

	if cname != "modified_tuples" && cname != "STATUS" {
		return 0, errors.New("Expected 'modified_tubles'  or STATUS  for column name for result")
	}

	rowCount, err := readInt(r)
	if err != nil {
		return 0, err
	}
	if rowCount != 1 {
		return 0, errors.New("Expected one row for result")
	}

	rowLen, err := readInt(r)
	if err != nil {
		return 0, err
	}
	if rowLen != 8 {
		return 0, errors.New("Expected a long value result")
	}
	return readLong(r)
}

func deserializeTableForRows(r io.Reader) (*voltTable, error) {

	var colCount int16
	colCount, err := deserializeTableCommon(r)
	if err != nil {
		return nil, err
	}

	// column type "array" and column name "array" are not
	// length prefixed arrays. they are really just columnCount
	// len sequences of bytes (types) and strings (names).
	var i int16
	var columnTypes []int8
	for i = 0; i < colCount; i++ {
		ct, err := readByte(r)
		if err != nil {
			return nil, err
		}
		columnTypes = append(columnTypes, ct)
	}

	var columnNames []string
	for i = 0; i < colCount; i++ {
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
	//var offset int64 = 0
	var rowI int32
	for rowI = 0; rowI < rowCount; rowI++ {
		rowLen, _ := readInt(r)
		rows[rowI] = make([]byte, rowLen)
		_, _ = r.Read(rows[rowI])
		//offset += int64(rowLen + 4)
	}

	return newVoltTable(colCount, columnTypes, columnNames, rowCount, rows), nil
}

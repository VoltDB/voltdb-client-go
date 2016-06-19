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

type VoltResult struct {
	clientHandle         int64
	appStatus            int8
	appStatusString      string
	clusterRoundTripTime int32
}

func NewVoltResult(clientHandle int64, appStatus int8, appStatusString string, clusterRoundTripTime int32) *VoltResult {
	var vres = new(VoltResult)
	vres.clientHandle = clientHandle
	vres.appStatus = appStatus
	vres.appStatusString = appStatusString
	vres.clusterRoundTripTime = clusterRoundTripTime
	return vres
}

func (vres VoltResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (vres VoltResult) RowsAffected() (int64, error) {
	return 0, nil
}

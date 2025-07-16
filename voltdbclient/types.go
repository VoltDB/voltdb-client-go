/* This file is part of VoltDB.
 * Copyright (C) 2025 VoltDB Inc.
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

type VoltType int8

const (
	ARRAY           VoltType = -99
	NULL            VoltType = 1
	TINYINT         VoltType = 3
	SMALLINT        VoltType = 4
	INTEGER         VoltType = 5
	BIGINT          VoltType = 6
	FLOAT           VoltType = 8
	STRING          VoltType = 9
	TIMESTAMP       VoltType = 11
	DATE            VoltType = 12
	DECIMAL         VoltType = 22
	VARBINARY       VoltType = 25
	GEOGRAPHY_POINT VoltType = 26
	GEOGRAPHY       VoltType = 27
)

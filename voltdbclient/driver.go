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
	"database/sql"
	"database/sql/driver"
	"time"
)

// VoltDriver implements A database/sql/driver for VoltDB.  This driver is
// registered as 'voltdb'
type VoltDriver struct{}

// NewVoltDriver returns a new instance of a VoltDB driver.
func NewVoltDriver() *VoltDriver {
	return &VoltDriver{}
}

// Open a connection to the VoltDB server.
func (vd *VoltDriver) Open(hostAndPort string) (driver.Conn, error) {
	return vd.OpenWithConnectTimeout(hostAndPort, DefaultConnectionTimeout)
}

// Open a connection to the VoltDB server.
func (vd *VoltDriver) OpenWithConnectTimeout(hostAndPort string, duration time.Duration) (driver.Conn, error) {
	return OpenConnWithTimeout(hostAndPort, duration)
}

func init() {
	sql.Register("voltdb", &VoltDriver{})
}

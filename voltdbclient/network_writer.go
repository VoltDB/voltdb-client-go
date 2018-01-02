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
	"github.com/VoltDB/voltdb-client-go/wire"
)

func EncodePI(e *wire.Encoder, pi *procedureInvocation) error {
	_, err := e.Int32(int32(pi.getLen()))
	if err != nil {
		return err
	}

	// batch timeout type
	_, err = e.Byte(0)
	if err != nil {
		return err
	}

	_, err = e.String(pi.query)
	if err != nil {
		return err
	}
	_, err = e.Int64(pi.handle)
	if err != nil {
		return err
	}

	_, err = e.Int16(int16(len(pi.params)))
	if err != nil {
		return err
	}
	for i := 0; i < len(pi.params); i++ {
		_, err = e.Marshal(pi.params[i])
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	return nil
}

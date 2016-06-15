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
package main

import (
	"database/sql/driver"
	"fmt"
	"github.com/VoltDB/voltdb-client-go/voltdbclient"
	"log"
	"os"
)

func main() {
	voltDriver := voltdbclient.NewVoltDriver()
	conn, err := voltDriver.Open("localhost:21212")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("HELLOWORLD.select")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}

	rows, err := stmt.Query([]driver.Value{"French"})
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}

	voltRows := rows.(voltdbclient.VoltRows)
	if voltRows.AdvanceRow() {
		hello, err := voltRows.GetStringByName("HELLO")
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}
		world, err := voltRows.GetStringByName("WORLD")
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}
		fmt.Printf("%v, %v!\n", hello, world)
	}
}

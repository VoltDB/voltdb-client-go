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

// A simple example that demonstrates the use of synchronous Query and Exec calls.
package main

import (
	"database/sql/driver"
	"fmt"
	"log"

	"github.com/VoltDB/voltdb-client-go/voltdbclient"
)

func main() {
	conn, err := voltdbclient.OpenConn("localhost:21212")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	result, err := conn.Exec("@AdHoc", []driver.Value{"DELETE FROM HELLOWORLD;"})
	if err != nil {
		log.Fatal(err)
	}
	ra, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d row(s) deleted\n", ra)

	conn.Exec("HELLOWORLD.insert", []driver.Value{"Bonjour", "Monde", "French"})
	conn.Exec("HELLOWORLD.insert", []driver.Value{"Hello", "World", "English"})
	conn.Exec("HELLOWORLD.insert", []driver.Value{"Hola", "Mundo", "Spanish"})
	conn.Exec("HELLOWORLD.insert", []driver.Value{"Hej", "Verden", "Danish"})
	conn.Exec("HELLOWORLD.insert", []driver.Value{"Ciao", "Mondo", "Italian"})

	rows, err := conn.Query("@AdHoc", []driver.Value{"select * from HELLOWORLD where DIALECT = ?", "French"})
	if err != nil {
		fmt.Println(err)
	} else {
		printRow(rows)
	}

	// with prepared statement
	stmt, err := conn.Prepare("select * from HELLOWORLD")
	if err != nil {
		log.Fatal(err)
	}

	rows, err = stmt.Query([]driver.Value{})
	if err != nil {
		log.Fatal(err)
	}
	printRow(rows)
}

func printRow(rows driver.Rows) {
	voltRows := rows.(voltdbclient.VoltRows)
	for voltRows.AdvanceRow() {
		hello, err := voltRows.GetStringByName("HELLO")
		if err != nil {
			fmt.Println(err)
		}
		world, err := voltRows.GetStringByName("WORLD")
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("%v, %v!\n", hello, world)
	}
}

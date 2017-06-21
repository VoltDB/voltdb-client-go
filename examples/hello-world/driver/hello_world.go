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

// A simple example that demonstrates the use of the VoltDB database/sql/driver.
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/VoltDB/voltdb-client-go/voltdbclient"
)

func main() {
	// If using a version of VoltDB server prior to 5.2, then
	// set the version of the wire protocol to 0.  The default
	// value 1, indicates a server version of 5.2 or later.
	// voltdbclient.ProtocolVersion = 0

	db, err := sql.Open("voltdb", "localhost:21212")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	rows, err := db.Query("HELLOWORLD.select", "French")
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// with prepared statement
	stmt, err := db.Prepare("select * from HELLOWORLD where dialect = ?")
	if err != nil {
		log.Fatal(err)
	}

	rows, err = stmt.Query("French")
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

}

func printRows(rows *sql.Rows) {
	for rows.Next() {
		var hello string
		var world string
		var dialect string
		err := rows.Scan(&hello, &world, &dialect)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Printf("SUCCESS: %s %s %s\n", hello, world, dialect)
	}
}

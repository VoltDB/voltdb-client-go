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

// A simple example that demonstrates the use of the VoltDB database/sql/driver.
package main

import "database/sql"
import (
	"fmt"
	_ "github.com/VoltDB/voltdb-client-go/voltdbclient"
	"log"
	"os"
)

func main() {

	db, err := sql.Open("voltdb", "localhost:21212/voltdb")
	if err != nil {
		fmt.Println("open")
		log.Fatal(err)
		os.Exit(-1)
	}
	err = db.Ping()
	if err != nil {
		fmt.Println("open")
		log.Fatal(err)
		os.Exit(-1)
	}

	stmt, err := db.Prepare("{call Vote(?, ?, ?)}")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}

	rows, err := stmt.Query( 1,2,3)
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	printRows(rows)

	/*
	rows, err := db.Query("HELLOWORLD.select", "French")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	printRows(rows)

	// with prepared statement
	stmt, err := db.Prepare("select * from HELLOWORLD where dialect = ?")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}

	rows, err = stmt.Query("c")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	printRows(rows)
	*/

}

func printRows(rows *sql.Rows) {
	for rows.Next() {
		var hello  int64
		err := rows.Scan(&hello)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Printf("SUCCESS: %s %s %s\n", hello)
	}
}

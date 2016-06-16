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
	"math/rand"
	"os"
)

func main() {
	conn1, err := voltdbclient.OpenConn("localhost:21212")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	defer conn1.Close()

	conn2, err := voltdbclient.OpenConn("localhost:21212")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	defer conn2.Close()

	stmt1, err := conn1.Prepare("HELLOWORLD.select")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	vs1 := stmt1.(voltdbclient.VoltStatement)

	stmt2, err := conn2.Prepare("HELLOWORLD.select")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	vs2 := stmt2.(voltdbclient.VoltStatement)

	keys := []string{"English", "French", "Spanish", "Danish", "Italian"}
	for i := 0; i < 10000; i++ {
		key := keys[rand.Intn(5)]
		err := vs1.QueryAsync([]driver.Value{key})
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}

		err = vs2.QueryAsync([]driver.Value{key})
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}
	}

	for {
		if !conn1.HasExecutingStatements() && !conn2.HasExecutingStatements() {
			break;
		}

		if conn1.HasExecutingStatements() {
			rows1, err := conn1.StatementResult()
			if err != nil {
				log.Fatal(err)
				os.Exit(-1)
			}
			handleRows(rows1)
		}

		if conn2.HasExecutingStatements() {
			rows2, err := conn2.StatementResult()
			if err != nil {
				log.Fatal(err)
				os.Exit(-1)
			}
			handleRows(rows2)
		}
	}
}

func handleRows(rows driver.Rows) {
	vrows := rows.(voltdbclient.VoltRows)
	vrows.AdvanceRow()
	iHello, err := vrows.GetStringByName("HELLO")
	hello := iHello.(string)
	if err != nil {
		log.Fatal(err)
	}
	iWorld, err := vrows.GetStringByName("WORLD")
	world := iWorld.(string)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v, %v!\n", hello, world)
}

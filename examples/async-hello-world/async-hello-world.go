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

	// create one connection, save the async results and wait on them explicitly.
	conn1, err := voltdbclient.OpenConn("localhost:21212")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	defer conn1.Close()

	stmt1, err := conn1.Prepare("HELLOWORLD.select")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	vs1 := stmt1.(voltdbclient.VoltStatement)

	keys := []string{"English", "French", "Spanish", "Danish", "Italian"}

	cbs := make([]*voltdbclient.VoltQueryResult, 100)
	for i := 0; i < 100; i++ {
		key := keys[rand.Intn(5)]
		cb, err := vs1.QueryAsync([]driver.Value{key})
		cbs[i] = cb
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}
	}

	// process the callbacks
	for _, cb := range cbs {
		rows := cb.Rows()
		handleRows(rows)
	}

	// create two connections and have them query continuously, then drain the results.
	conn2, err := voltdbclient.OpenConn("localhost:21212")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	defer conn2.Close()

	conn3, err := voltdbclient.OpenConn("localhost:21212")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	defer conn3.Close()

	stmt2, err := conn2.Prepare("HELLOWORLD.select")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	vs2 := stmt2.(voltdbclient.VoltStatement)

	stmt3, err := conn3.Prepare("HELLOWORLD.select")
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	vs3 := stmt3.(voltdbclient.VoltStatement)

	for i := 0; i < 2000; i++ {
		key := keys[rand.Intn(5)]
		_, err := vs2.QueryAsync([]driver.Value{key})
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}

		_, err = vs3.QueryAsync([]driver.Value{key})
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}
	}

	for {
		if !conn2.HasExecutingStatements() && !conn3.HasExecutingStatements() {
			break;
		}

		if conn2.HasExecutingStatements() {
			rows2, err := conn2.StatementResult()
			if err != nil {
				log.Fatal(err)
				os.Exit(-1)
			}
			handleRows(rows2)
		}

		if conn3.HasExecutingStatements() {
			rows3, err := conn3.StatementResult()
			if err != nil {
				log.Fatal(err)
				os.Exit(-1)
			}
			handleRows(rows3)
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

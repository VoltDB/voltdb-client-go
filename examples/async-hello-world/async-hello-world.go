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
	conn, err := voltdbclient.OpenConn("localhost:21212")
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
	vs := stmt.(voltdbclient.VoltStatement)

	keys := []string{"English", "French", "Spanish", "Danish", "Italian"}
	// The numbers returned from rand are deterministic based on the seed.
	// This code will always produce the same result.
	rand.Seed(11)
	for _, key := range keys {
		err := vs.QueryAsync([]driver.Value{key})
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}
	}
	for conn.HasExecutingStatements() {
		rows, err := conn.StatementResult()
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}
		handleRows(rows)
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

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

// A simple example that demonstrates the use of asynchronous Query and Exec calls.
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

	conn1.Exec("@AdHoc", []driver.Value{"DELETE FROM HELLOWORLD;"})

	resCons := ResponseConsumer{}

	conn1.ExecAsync(resCons, "HELLOWORLD.insert", []driver.Value{"Bonjour", "Monde", "French"})
	conn1.ExecAsync(resCons, "HELLOWORLD.insert", []driver.Value{"Hello", "World", "English"})
	conn1.ExecAsync(resCons, "HELLOWORLD.insert", []driver.Value{"Hola", "Mundo", "Spanish"})
	conn1.ExecAsync(resCons, "HELLOWORLD.insert", []driver.Value{"Hej", "Verden", "Danish"})
	conn1.ExecAsync(resCons, "HELLOWORLD.insert", []driver.Value{"Ciao", "Mondo", "Italian"})
	conn1.Drain()

	keys := []string{"English", "French", "Spanish", "Danish", "Italian"}

	for i := 0; i < 100; i++ {
		key := keys[rand.Intn(5)]
		err := conn1.QueryAsync(resCons, "HELLOWORLD.select", []driver.Value{key})
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}
	}
	conn1.Drain()

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

	for i := 0; i < 2000; i++ {
		key := keys[rand.Intn(5)]
		err := conn2.QueryAsync(resCons, "HELLOWORLD.select", []driver.Value{key})
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}

		err = conn3.QueryAsync(resCons, "HELLOWORLD.select", []driver.Value{key})
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}
	}

	conn2.Drain()
	conn3.Drain()
}

type ResponseConsumer struct{}

func (rc ResponseConsumer) ConsumeError(err error) {
	fmt.Println(err)
}

func (rc ResponseConsumer) ConsumeResult(res driver.Result) {
	ra, _ := res.RowsAffected()
	lid, _ := res.LastInsertId()
	fmt.Printf("%d, %d\n", ra, lid)
}

func (rc ResponseConsumer) ConsumeRows(rows driver.Rows) {
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

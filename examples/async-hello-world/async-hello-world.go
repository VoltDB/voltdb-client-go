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
	"fmt"
	"github.com/VoltDB/voltdb-client-go/voltdbclient"
	"log"
	"math/rand"
)

func main() {
	client := voltdbclient.NewClient("", "")
	if err := client.CreateConnection("localhost:21212"); err != nil {
		log.Fatal("failed to connect to server")
	}
	defer func() {
		client.Close()
	}()

	// rows to insert
	insertRows := make([][]string, 5)
	insertRows[0] = []string{"Hello", "World", "English"}
	insertRows[1] = []string{"Bonjour", "Monde", "French"}
	insertRows[2] = []string{"Hola", "Mundo", "Spanish"}
	insertRows[3] = []string{"Hej", "Verden", "Danish"}
	insertRows[4] = []string{"Ciao", "Mondo", "Italian"}
	for _, row := range insertRows {
		insertDataRows(client, row[0], row[1], row[2])
	}

	// The numbers returned from rand are deterministic based on the seed.
	// This code will always produce the same result.
	rand.Seed(11)
	callbacks := make([]*voltdbclient.Callback, 5)
	for i := 0; i < 5; i++ {
		callback, err := client.CallAsync("HELLOWORLD.select", insertRows[rand.Intn(len(insertRows))][2])
		if err != nil {
			log.Fatal(err)
		}
		callbacks[i] = callback
	}
	ch := client.MultiplexCallbacks(callbacks)
	for i := 0; i < 5; i++ {
		rows := <-ch
		handleRows(rows)
	}
}

func insertDataRows(client *voltdbclient.Client, hello, world, dialect string) {
	_, err := client.Call("HELLOWORLD.insert", hello, world, dialect)
	if err != nil {
		log.Fatal(err)
	}
}

func handleRows(rows *voltdbclient.VoltRows) {
	rows.AdvanceRow()
	iHello, err := rows.GetStringByName("HELLO")
	hello := iHello.(string)
	if err != nil {
		log.Fatal(err)
	}
	iWorld, err := rows.GetStringByName("WORLD")
	world := iWorld.(string)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v, %v!\n", hello, world)
}

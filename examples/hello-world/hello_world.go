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
)

func main() {
	client := voltdbclient.NewClient("", "")
	if err := client.CreateConnection("localhost:21212"); err != nil {
		log.Fatal("failed to connect to server")
	}
	defer func() {
		if client != nil {
			client.Close()
		}
	}()

	// rows to insert
	insertRows := make([][]string, 5)
	insertRows[0] = []string{"Hello", "World", "English"}
	insertRows[1] = []string{"Bonjour", "Monde", "French"}
	insertRows[2] = []string{"Hola", "Mundo", "Spanish"}
	insertRows[3] = []string{"Hej", "Verden", "Danish"}
	insertRows[4] = []string{"Ciao", "Mondo", "Italian"}
	for _, insertRow := range insertRows {
		insertDataRow(client, insertRow[0], insertRow[1], insertRow[2])
	}
	rows, err := client.Call("HELLOWORLD.select", "French")
	if err != nil {
		log.Fatal(err)
	}
	if rows.AdvanceRow() {
		hello, err := rows.GetStringByName("HELLO")
		if err != nil {
			log.Fatal(err)
		}
		world, err := rows.GetStringByName("WORLD")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%v, %v!\n", hello, world)
	}
}

func insertDataRow(client *voltdbclient.Client, hello, world, dialect string) {
	_, err := client.Call("HELLOWORLD.insert", hello, world, dialect)
	if err != nil {
		log.Fatal(err)
	}
}

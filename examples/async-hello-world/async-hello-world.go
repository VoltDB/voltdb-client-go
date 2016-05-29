package main

import (
	"fmt"
	"math/rand"
	"github.com/VoltDB/voltdb-client-go/voltdbclient"
	"log"
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
	rows := make([][]string, 5)
	rows[0] = []string{"Hello", "World", "English"}
	rows[1] = []string{"Bonjour", "Monde", "French"}
	rows[2] = []string{"Hola", "Mundo", "Spanish"}
	rows[3] = []string{"Hej", "Verden", "Danish"}
	rows[4] = []string{"Ciao", "Mondo", "Italian"}
	for _, row := range rows {
		insertData(client, row[0], row[1], row[2])
	}

	// The numbers returned from rand are deterministic based on the seed.
	// This code will always produce the same result.
	rand.Seed(11)
	callbacks := make([]*voltdbclient.Callback, 5)
	for i := 0; i < 5; i++ {
		callback, err := client.CallAsync("HELLOWORLD.select", rows[rand.Intn(len(rows))][2])
		if err != nil {
			log.Fatal(err)
		}
		callbacks[i] = callback;
	}
	ch := client.MultiplexCallbacks(callbacks)
	for i := 0; i < 5; i++ {
		resp := <- ch
		handleResponse(resp)
	}
}

func insertData(client *voltdbclient.Client, hello, world, dialect string) {
	response, err := client.Call("HELLOWORLD.insert", hello, world, dialect)
	if err != nil {
		log.Fatal(err)
	}
	if (response.Status() != voltdbclient.SUCCESS) {
		log.Fatal("Insert failed with " + response.StatusString())
	}
}

func handleResponse(resp *voltdbclient.Response) {
	if resp.TableCount() > 0 {
		table := resp.Table(0)
		row, err := table.FetchRow(0)
		if err != nil {
			log.Fatal(err)
		}
		hello, err := row.GetStringByName("HELLO")
		if err != nil {
			log.Fatal(err)
		}
		world, err := row.GetStringByName("WORLD")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%v, %v!\n", hello, world)
	} else {
		log.Fatal("Select statement didn't return any data")
	}
}

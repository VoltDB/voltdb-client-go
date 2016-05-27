package main

import (
	"github.com/VoltDB/voltdb-client-go/voltdbclient"
	"fmt"
	"os"
)

func main() {
	client := voltdbclient.NewClient("", "")
	if err := client.CreateConnection("localhost:21212"); err != nil {
		fmt.Println("failed to connect to server")
		os.Exit(-1);
	}
	defer func() {
		if client != nil {
			client.Close()
		}
	}()

	// rows to insert
	rows := make([][]string, 5)
	rows[0] = []string{"Hello", "World", "English"}
	rows[1] = []string{"Bonjour", "Monde", "French"}
	rows[2] = []string{"Hola", "Mundo", "Spanish"}
	rows[3] = []string{"Hej", "Verden", "Danish"}
	rows[4] = []string{"Ciao", "Mondo", "Italian"}
	for _, row := range rows {
		err := insertData(client, row[0], row[1], row[2])
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(-1)
		}
	}
	response, err := client.Call("HELLOWORLD.select", "French")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	if response.TableCount() > 0 {
		table := response.Table(0)
		row, err := table.FetchRow(0)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(-1)
		}
		hello, err := row.GetStringByName("HELLO")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(-1)
		}
		world, err := row.GetStringByName("WORLD")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(-1)
		}
		fmt.Printf("%v, %v!\n", hello, world)
	} else {
		fmt.Println("Select statement didn't return any data")
	}
}

func insertData(client *voltdbclient.Client, hello, world, dialect string) error {
	response, err := client.Call("HELLOWORLD.insert", hello, world, dialect)
	if err != nil {
		return err
	}
	if (response.Status() != voltdbclient.SUCCESS) {
		return fmt.Errorf("Insert failed with " + response.StatusString())
	}
	return nil;
}

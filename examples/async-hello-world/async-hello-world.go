package main

import (
	"fmt"
	"math/rand"
	"github.com/VoltDB/voltdb-client-go/voltdbclient"
	"os"
)

func main() {
	client := voltdbclient.NewClient("", "")
	if err := client.CreateConnection("localhost:21212"); err != nil {
		fmt.Println("failed to connect to server")
		os.Exit(-1);
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
		err := insertData(client, row[0], row[1], row[2])
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(-1)
		}
	}

	// The numbers returned from rand are deterministic based on the seed.
	// This code will always produce the same result.
	rand.Seed(11)
	for i := 0; i < 5; i++ {
		data, err := selectData(client, rows[rand.Intn(len(rows))][2])
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(data)
		}
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

func selectData(client *voltdbclient.Client, dialect string) (string, error) {
	cb, err := client.CallAsync("HELLOWORLD.select", dialect)
	if err != nil {
		return "", err
	}
	resp := <- cb.Channel
	if resp.TableCount() > 0 {
		table := resp.Table(0)
		row, err := table.FetchRow(0)
		if err != nil {
			return "", err
		}
		hello, err := row.GetStringByName("HELLO")
		if err != nil {
			return "", err
		}
		world, err := row.GetStringByName("WORLD")
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v, %v!", hello, world), nil
	}
	return "", fmt.Errorf("Select statement didn't return any data")
}

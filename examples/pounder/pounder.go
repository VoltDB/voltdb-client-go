package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	_ "github.com/VoltDB/voltdb-client-go/voltdbclient"
)

// for use sql/driver
const VOLTDB_DRIVER = "voltdb"

var (
	servers    string
	goroutines int
	duration   time.Duration
	rows       uint64
	totalRows  uint64
)

func main() {
	flag.StringVar(&(servers), "servers", "localhost:21212", "Comma separated list of the form server[:port] to connect to.")
	flag.IntVar(&(goroutines), "goroutines", 40, "Number of concurrent goroutines synchronously calling procedures.")
	flag.DurationVar(&(duration), "duration", 120*time.Second, "Benchmark duration, in seconds. ")
	flag.Uint64Var(&(rows), "rows", 10000, "Number of rows to insert.")
	flag.Parse()
	volt := openAndPingDB(servers)
	defer volt.Close()

	putKV(goroutines, duration, putKVSql)
}

func openAndPingDB(servers string) *sql.DB {
	db, err := sql.Open(VOLTDB_DRIVER, servers+"/"+VOLTDB_DRIVER)
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
	return db
}

func putKV(gorotines int, duration time.Duration,
	fn func(join chan int, duration time.Duration)) {
	var joiners = make([]chan int, 0)
	for i := 0; i < gorotines; i++ {
		var joinchan = make(chan int)
		joiners = append(joiners, joinchan)
		go fn(joinchan, duration)
	}

	var totalCount = 0
	for _, join := range joiners {
		ops := <-join
		totalCount += ops
	}
	fmt.Printf("%d rows insert.\n", totalCount)
	return
}

func putKVSql(join chan int, duration time.Duration) {
	volt := openAndPingDB(servers)
	defer volt.Close()

	timeout := time.After(duration)

	ops := 0
	for {
		select {
		case <-timeout:
			join <- ops
			return
		default:
			id := atomic.AddUint64(&totalRows, 1)
			if id <= rows {
				if _, err := volt.Exec("inalerts", id, fmt.Sprintf("message_%d", id)); err != nil {
					log.Panic(err)
				} else {
					ops++
				}
			} else {
				join <- ops
				return
			}
		}

	}
}

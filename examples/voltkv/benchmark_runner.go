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
	"log"
	"math/rand"
	"os"
	"time"

	"sync/atomic"

	"github.com/VoltDB/voltdb-client-go/voltdbclient"
)

// handy, rather than typing this out several times
const HORIZONTAL_RULE = "----------" + "----------" + "----------" + "----------" +
	"----------" + "----------" + "----------" + "----------" + "\n"

// benchmark state
var successfulGets, missedGets, failedGets, rawGetData, networkGetData, successfulPuts, failedPuts, rawPutData, networkPutData uint64


type benchmark struct {
	config kvConfig
	conn   *voltdbclient.VoltConn
	// time time.Timer
	// benchmarkStartTS
	processor *payLoadProcessor

	// Statistics manager objects from the client
	// periodicStatsContext
	// fullStatsContext
}

func NewBenchmark(kv kvConfig) (*benchmark, error) {
	bm := new(benchmark)
	bm.config = kv
	bm.processor = NewPayloadProcessor(kv.keysize, kv.minvaluesize, kv.maxvaluesize, kv.entropy,
		kv.poolsize, kv.usecompression)

	return bm, nil
}

func (bm *benchmark) runBenchmark(){
	fmt.Print(HORIZONTAL_RULE)
	fmt.Println(" Setup & Initialization")
	fmt.Println(HORIZONTAL_RULE)
	bm.conn = connect(bm.config.servers)

	// preload keys if requested
	fmt.Println()
	if bm.config.preload {
		fmt.Println("Preloading data store...")
		for i := 0; i < bm.config.poolsize; i++ {
			_, _, storeValue := bm.processor.generateForStore()
			bm.conn.ExecAsync("STORE.upsert", []driver.Value{fmt.Sprintf(bm.processor.keyFormat, i), storeValue})
		}
		bm.conn.DrainAll()
		fmt.Print("Preloading complete.\n\n")
	}

	fmt.Print(HORIZONTAL_RULE)
	fmt.Println(" Starting Benchmark")
	fmt.Println(HORIZONTAL_RULE)

	// Run the benchmark loop for the requested warmup time
	// The throughput may be throttled depending on client configuration
	fmt.Println("Warming up...")
	switch bm.config.runtype {
	case ASYNC:
		runGetPutAsync(bm.conn, bm.processor, bm.config.getputratio, bm.config.warmup)
	case SYNC:
		runGetPutSync(bm.config.goroutines, bm.processor, bm.config.getputratio, bm.config.warmup)
	}

	//reset the stats after warmup

	// print periodic statistics to the console

	// Run the benchmark loop for the requested duration
	// The throughput may be throttled depending on client configuration
	fmt.Print("\nRunning benchmark...\n")
	switch bm.config.runtype {
	case ASYNC:
		runGetPutAsync(bm.conn, bm.processor, bm.config.getputratio, bm.config.duration)
	case SYNC:
		runGetPutSync(bm.config.goroutines, bm.processor, bm.config.getputratio, bm.config.duration)
	}

	// results := bm.conn.DrainAll()

	// cancel periodic stats printing
	// timer.cancel();

	// block until all outstanding txns return
	// client.drain();

	// print the summary results

	// close down the client connections
	// client.close();
}

func runGetPutAsync(conn *voltdbclient.VoltConn, proc *payLoadProcessor, getputratio float64, duration time.Duration) {
	timeout := time.After(duration)
	for {
		select {
		case <-timeout:
			fmt.Println("Done")
			return
		default:
			if rand.Float64() < getputratio {
				conn.QueryAsync("STORE.select", []driver.Value{proc.generateRandomKeyForRetrieval()})
			} else {
				key, _, storeValue := proc.generateForStore()
				conn.ExecAsync("STORE.upsert", []driver.Value{key, storeValue})
			}

		}

	}
}

func runGetPutSync(gorotines int, proc *payLoadProcessor, getputratio float64, duration time.Duration) {
	var joiners = make([]chan int, 0)
	for i := 0; i < gorotines; i++ {
		var joinchan = make(chan int)
		joiners = append(joiners, joinchan)
		go runGetPut(joinchan, proc, getputratio, duration)
	}

	var totalOps = 0
	for v, join := range joiners {
		ops := <-join
		totalOps += ops
		fmt.Printf("kver %v finished and acted %v ops.\n", v, ops)
	}
	fmt.Printf("Acted %v ops in %v seconds (%0.0f ops/second)\n",
		totalOps, duration.Seconds(),
		float64(totalOps)/duration.Seconds())
	fmt.Println(successfulGets,failedGets, successfulPuts, failedPuts)
	return
}

func runGetPut(join chan int, proc *payLoadProcessor, getputratio float64, duration time.Duration) {
	volt, err := voltdbclient.OpenConn("localhost:21212")
	defer volt.Close()
	if err != nil {
		log.Fatalf("Connection error db. %v\n", err)
	}

	ops := 0
	timeout := time.After(duration)
	for {
		select {
		case <-timeout:
			join <- ops
			return
		default:
			if rand.Float64() < getputratio {
				if rows, err := volt.Query("STORE.select", []driver.Value{proc.generateRandomKeyForRetrieval()}); err == nil {
					if voltRows := rows.(voltdbclient.VoltRows); voltRows.AdvanceRow() {
						atomic.AddUint64(&successfulGets, 1)
						if val, err := voltRows.GetVarbinary(1); err == nil {
							rawValue, storeValue := proc.retrieveFromStore(val.([]byte))
							atomic.AddUint64(&networkGetData, uint64(len(storeValue)))
							atomic.AddUint64(&rawGetData, uint64(len(rawValue)))
						} else {
							log.Panic(err)
						}
					} else {
						atomic.AddUint64(&missedGets, 1)
					}
					ops++
				} else {
					log.Panic(err)
					atomic.AddUint64(&failedGets, 1)
				}
			} else {
				key, rawValue, storeValue := proc.generateForStore()
				if _, err := volt.Exec("STORE.upsert", []driver.Value{key, storeValue}); err == nil {
					atomic.AddUint64(&successfulPuts, 1)
					ops++
				} else {
					log.Panic(err)
					atomic.AddUint64(&failedPuts, 1)
				}
				atomic.AddUint64(&networkPutData, uint64(len(storeValue)))
				atomic.AddUint64(&rawPutData, uint64(len(rawValue)))
			}
		}

	}

}

func connect(servers string) *voltdbclient.VoltConn {
	conn, err := voltdbclient.OpenConn(servers)
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	return conn
}

func main() {
	kv, err := NewKVConfig()
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	bm, _ := NewBenchmark(*kv)
	bm.runBenchmark()
}

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
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/VoltDB/voltdb-client-go/voltdbclient"
	"database/sql"
)

// handy, rather than typing this out several times
const HORIZONTAL_RULE = "----------" + "----------" + "----------" + "----------" +
	"----------" + "----------" + "----------" + "----------" + "\n"

// for use sql/driver
const VOLTDB_DRIVER="voltdb"

// benchmark stats
type benchmarkStats struct {
	totalOps, successfulGets, missedGets, failedGets, rawGetData, networkGetData, successfulPuts, failedPuts, rawPutData, networkPutData uint64
}

func clear(v interface{}) {
	p := reflect.ValueOf(v).Elem()
	p.Set(reflect.Zero(p.Type()))
}

var periodicStats, fullStats *benchmarkStats

var cpuprofile = ""
var config *kvConfig
var ticker *time.Ticker
var timeStart, timeSent, timeDrained, timeProcessed time.Time

type benchmark struct {
	conn      *voltdbclient.VoltConn
	processor *payLoadProcessor
}

func NewBenchmark() (*benchmark, error) {
	bm := new(benchmark)
	bm.processor = NewPayloadProcessor(config.keysize, config.minvaluesize, config.maxvaluesize, config.entropy,
		config.poolsize, config.usecompression)
	periodicStats = new(benchmarkStats)
	fullStats = new(benchmarkStats)
	return bm, nil
}

func (bm *benchmark) runBenchmark() {
	fmt.Print(HORIZONTAL_RULE)
	fmt.Println(" Setup & Initialization")
	fmt.Println(HORIZONTAL_RULE)
	bm.conn = connect(config.servers)
	defer bm.conn.Close()

	// preload keys if requested
	fmt.Println()
	if config.preload {
		fmt.Println("Preloading data store...")
		for i := 0; i < config.poolsize; i++ {
			_, _, storeValue := bm.processor.generateForStore()
			bm.conn.ExecAsync("STORE.insert", []driver.Value{fmt.Sprintf(bm.processor.keyFormat, i), storeValue})
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
	switch config.runtype {
	case ASYNC:
		runGetPutAsync(bm.conn, bm.processor, config.getputratio, config.warmup)
		bm.conn.DrainAll()
	case SYNC:
		runGetPutSync(config.goroutines, bm.processor, config.getputratio, config.warmup, runGetPutConn)
	case STATEMENT:
		runGetPutSync(config.goroutines, bm.processor, config.getputratio, config.warmup, runGetPutStatement)
	}

	//reset the stats after warmup
	clear(fullStats)
	clear(periodicStats)

	// print periodic statistics to the console
	ticker = time.NewTicker(config.displayinterval)
	defer ticker.Stop()
	go printStatistics()
	timeStart = time.Now()

	// Run the benchmark loop for the requested duration
	// The throughput may be throttled depending on client configuration
	fmt.Print("\nRunning benchmark...\n")
	switch config.runtype {
	case ASYNC:
		runGetPutAsync(bm.conn, bm.processor, config.getputratio, config.duration)
		// block until all outstanding txns return
		timeSent = time.Now()
		responses := bm.conn.DrainAll()
		timeDrained = time.Now()
		handleResponses(bm.processor, responses)
		timeProcessed = time.Now()
		fmt.Printf("Time spent on sent:%v drain:%v postprocess: %v\n", timeSent.Sub(timeStart), timeDrained.Sub(timeSent), timeProcessed.Sub(timeDrained))
	case SYNC:
		runGetPutSync(config.goroutines, bm.processor, config.getputratio, config.duration, runGetPutConn)
	case STATEMENT:
		runGetPutSync(config.goroutines, bm.processor, config.getputratio, config.duration, runGetPutStatement)
	}

	timeElapsed := time.Now().Sub(timeStart)
	// print the summary results
	printResults(timeElapsed)
}

func openAndPingDB(servers string)  *sql.DB {
	db, err := sql.Open(VOLTDB_DRIVER, servers + "/" + VOLTDB_DRIVER)
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

func runGetPutStatement(join chan int, proc *payLoadProcessor, getputratio float64, duration time.Duration) {
	volt := openAndPingDB(config.servers)
	defer volt.Close()

	timeout := time.After(duration)
	// with prepared statement
	getStmt, getErr := volt.Prepare("select value from store where key = ?;")
	if getErr != nil {
		log.Fatal(getErr)
		os.Exit(-1)
	}
	defer  getStmt.Close()

	putStmt, putErr := volt.Prepare("upsert into store (key, value) values (?,?) ")
	if putErr != nil {
		log.Fatal(putErr)
		os.Exit(-1)
	}
	defer putStmt.Close()

	ops := 0
	for {
		select {
		case <-timeout:
			join <- ops
			return
		default:
			if rand.Float64() < getputratio {
				rows, err := getStmt.Query(proc.generateRandomKeyForRetrieval())
				ops += handleSqlRows(proc, *rows, err)
			} else {
				key, rawValue, storeValue := proc.generateForStore()
				atomic.AddUint64(&(fullStats.networkPutData), uint64(len(storeValue)))
				atomic.AddUint64(&(fullStats.rawPutData), uint64(len(rawValue)))
				res, err := putStmt.Exec(key, storeValue)
				ops += handleResults(proc, res, err)
			}
		}

	}
}

func runGetPutConn(join chan int, proc *payLoadProcessor, getputratio float64, duration time.Duration) {
	volt := connect(config.servers)
	defer volt.Close()

	timeout := time.After(duration)

	ops := 0
	for {
		select {
		case <-timeout:
			join <- ops
			return
		default:
			if rand.Float64() < getputratio {
				rows, err := volt.Query("STORE.select", []driver.Value{proc.generateRandomKeyForRetrieval()})
				ops += handleDriverRows(proc, rows, err)
			} else {
				key, rawValue, storeValue := proc.generateForStore()
				atomic.AddUint64(&(fullStats.networkPutData), uint64(len(storeValue)))
				atomic.AddUint64(&(fullStats.rawPutData), uint64(len(rawValue)))
				res, err := volt.Exec("STORE.upsert", []driver.Value{key, storeValue})
				ops += handleResults(proc, res, err)
			}
		}

	}

}

func runGetPutSync(gorotines int, proc *payLoadProcessor, getputratio float64, duration time.Duration,
			fn func(join chan int, proc *payLoadProcessor, getputratio float64, duration time.Duration)) {
	var joiners = make([]chan int, 0)
	for i := 0; i < gorotines; i++ {
		var joinchan = make(chan int)
		joiners = append(joiners, joinchan)
		go fn(joinchan, proc, getputratio, duration)
	}

	var totalCount = 0
	for _, join := range joiners {
		ops := <-join
		totalCount += ops
		//fmt.Printf("kver %v finished and acted %v ops.\n", v, ops)
	}
	// fmt.Println(totalCount, fullStats.totalOps)
	return
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

func handleResponses(proc *payLoadProcessor, responses []*voltdbclient.VoltAsyncResponse) int {
	ops := 0
	for _, response := range responses {
		if response.IsQuery() {
			rows, err := response.Rows()
			ops += handleDriverRows(proc, rows, err)
		} else {
			res, err := response.Result()
			ops += handleResults(proc, res, err)
		}
	}
	return ops
}

func handleSqlRows(proc *payLoadProcessor, rows sql.Rows, err error) (success int) {
	defer  rows.Close()
	if err == nil {
		if rows.Next() {
			atomic.AddUint64(&(fullStats.successfulGets), 1)
			var val string
			if err := rows.Scan(&val); err == nil {
				rawValue, storeValue := proc.retrieveFromStore([]byte(val))
				atomic.AddUint64(&(fullStats.networkGetData), uint64(len(storeValue)))
				atomic.AddUint64(&(fullStats.rawGetData), uint64(len(rawValue)))
			} else {
				log.Panic(err)
				// shouldn't be here
			}
		} else {
			atomic.AddUint64(&(fullStats.missedGets), 1)
		}
		atomic.AddUint64(&(fullStats.totalOps), 1)
		atomic.AddUint64(&(periodicStats.totalOps), 1)
		success = 1
	} else {
		log.Panic(err)
		atomic.AddUint64(&(fullStats.failedGets), 1)
		success = 0
	}
	return
}

func handleDriverRows(proc *payLoadProcessor, rows driver.Rows, err error) (success int) {
	defer rows.Close()
	if err == nil {
		if voltRows := rows.(voltdbclient.VoltRows); voltRows.AdvanceRow() {
			atomic.AddUint64(&(fullStats.successfulGets), 1)
			if val, err := voltRows.GetVarbinary(1); err == nil {
				rawValue, storeValue := proc.retrieveFromStore(val.([]byte))
				atomic.AddUint64(&(fullStats.networkGetData), uint64(len(storeValue)))
				atomic.AddUint64(&(fullStats.rawGetData), uint64(len(rawValue)))
			} else {
				log.Panic(err)
				// shouldn't be here
			}
		} else {
			atomic.AddUint64(&(fullStats.missedGets), 1)
		}
		atomic.AddUint64(&(fullStats.totalOps), 1)
		atomic.AddUint64(&(periodicStats.totalOps), 1)
		success = 1
	} else {
		log.Panic(err)
		atomic.AddUint64(&(fullStats.failedGets), 1)
		success = 0
	}
	return
}

func handleResults(proc *payLoadProcessor, res driver.Result, err error) (success int) {
	if err == nil {
		atomic.AddUint64(&(fullStats.successfulPuts), 1)
		atomic.AddUint64(&(fullStats.totalOps), 1)
		atomic.AddUint64(&(periodicStats.totalOps), 1)
		success = 1
	} else {
		log.Panic(err)
		atomic.AddUint64(&(fullStats.failedPuts), 1)
		success = 0
	}
	return
}

func connect(servers string) *voltdbclient.VoltConn {
	conn, err := voltdbclient.OpenConn(servers)
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	return conn
}

func setupProfiler() {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}
}

func teardownProfiler() {
	if cpuprofile != "" {
		pprof.StopCPUProfile()
	}
}

func printStatistics() {
	s := time.Now()
	for t := range ticker.C {
		fmt.Print(t.Sub(s))
		fmt.Printf(" Throughput %v/s\n", float64(periodicStats.totalOps)/config.displayinterval.Seconds())
		clear(periodicStats)
	}
}

func printResults(timeElapsed time.Duration) {
	// 1. Get/Put performance results
	display := "\n" + HORIZONTAL_RULE +
		" KV Store Results\n" +
		HORIZONTAL_RULE +
		"\nA total of %v operations were posted...\n" +
		" - GETs: %v Operations (%v Misses and %v Failures)\n" +
		"         %v MB in compressed store data\n" +
		"         %v MB in uncompressed application data\n" +
		"         Network Throughput: %6.3f Gbps*\n" +
		" - PUTs: %v Operations (%v Failures)\n" +
		"         %v MB in compressed store data\n" +
		"         %v MB in uncompressed application data\n" +
		"         Network Throughput: %6.3f Gbps*\n" +
		" - Total Network Throughput: %6.3f Gbps*\n\n" +
		"* Figure includes key & value traffic but not database protocol overhead.\n\n"

	const (
		oneGigabit = float64(1024*1024*1024) / 8
		oneMB      = (1024 * 1024)
	)
	getThroughput := float64(fullStats.networkGetData + fullStats.successfulGets*uint64(config.keysize))
	getThroughput /= (oneGigabit * config.duration.Seconds())
	totalPuts := fullStats.successfulPuts + fullStats.failedPuts
	putThroughput := float64(fullStats.networkPutData + totalPuts*uint64(config.keysize))
	putThroughput /= (oneGigabit * config.duration.Seconds())

	fmt.Printf(display,
		fullStats.totalOps,
		fullStats.successfulGets, fullStats.missedGets, fullStats.failedGets,
		fullStats.networkGetData/oneMB,
		fullStats.rawGetData/oneMB,
		getThroughput,
		fullStats.successfulPuts, fullStats.failedPuts,
		fullStats.networkPutData/oneMB,
		fullStats.rawPutData/oneMB,
		putThroughput,
		getThroughput+putThroughput)

	// 2. Performance statistics
	fmt.Print(HORIZONTAL_RULE)
	fmt.Println(" Client Workload Statistics")
	fmt.Println(HORIZONTAL_RULE)

	fmt.Printf("Performed %v ops in %v seconds (%0.0f ops/second)\n",
		fullStats.totalOps, timeElapsed.Seconds(),
		float64(fullStats.totalOps)/timeElapsed.Seconds())
}

func main() {
	flag.StringVar(&cpuprofile, "cpuprofile", "", "name of profile file to write")
	setupProfiler()
	defer teardownProfiler()

	var err error
	config, err = NewKVConfig()
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}

	bm, _ := NewBenchmark()
	bm.runBenchmark()
}

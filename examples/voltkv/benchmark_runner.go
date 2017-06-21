/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
	"database/sql"
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
)

// horizontalRule is handy to use, rather than typing this out several times
const horizontalRule = "----------" + "----------" + "----------" + "----------" +
	"----------" + "----------" + "----------" + "----------" + "\n"

// voltDBDriver for use sql/driver
const voltDBDriver = "voltdb"

// benchmark stats
type benchmarkStats struct {
	totalOps, successfulGets, missedGets, failedGets, rawGetData, networkGetData, successfulPuts, failedPuts, rawPutData, networkPutData uint64
}

// helper function for clearing content of any type
func clear(v interface{}) {
	p := reflect.ValueOf(v).Elem()
	p.Set(reflect.Zero(p.Type()))
}

var (
	periodicStats, fullStats *benchmarkStats
	cpuprofile               = ""
	config                   *kvConfig
	ticker                   *time.Ticker
	timeStart                time.Time
	bm                       *benchmark
)

type benchmark struct {
	proc *payLoadProcessor
	conn *voltdbclient.Conn
}

// newBenchmark creates a new benchmark
func newBenchmark() (*benchmark, error) {
	bmTemp := new(benchmark)
	bmTemp.proc = newPayloadProcessor(
		config.keysize, config.minvaluesize, config.maxvaluesize, config.entropy,
		config.poolsize, config.usecompression)
	periodicStats = new(benchmarkStats)
	fullStats = new(benchmarkStats)
	fmt.Print(horizontalRule)
	fmt.Println(" Command Line Configuration")
	fmt.Println(horizontalRule)
	fmt.Printf("%+v\n", *config)
	return bmTemp, nil
}

func (bm *benchmark) runBenchmark() {
	fmt.Print(horizontalRule)
	fmt.Println(" Setup & Initialization")
	fmt.Println(horizontalRule)
	bm.conn = connect(config.servers)
	defer bm.conn.Close()

	// preload keys if requested
	fmt.Println()
	if config.preload {
		fmt.Println("Preloading data store...")
		for i := 0; i < config.poolsize; i++ {
			_, _, storeValue := bm.proc.generateForStore()
			bm.conn.ExecAsync(putCallBack{bm.proc}, "STORE.upsert", []driver.Value{fmt.Sprintf(bm.proc.keyFormat, i), storeValue})
		}
		bm.conn.Drain()
		fmt.Print("Preloading complete.\n\n")
	}

	fmt.Print(horizontalRule)
	fmt.Println(" Starting Benchmark")
	fmt.Println(horizontalRule)

	// Run the benchmark loop for the requested warmup time
	// The throughput may be throttled depending on client configuration
	fmt.Println("Warming up...")
	switch config.runtype {
	case ASYNC:
		runGetPut(config.goroutines, config.getputratio, config.warmup, runGetPutAsync)
	case SYNC:
		runGetPut(config.goroutines, config.getputratio, config.warmup, runGetPutSync)
	case SQL:
		runGetPut(config.goroutines, config.getputratio, config.warmup, runGetPutSQL)
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
		runGetPut(config.goroutines, config.getputratio, config.duration, runGetPutAsync)
	case SYNC:
		runGetPut(config.goroutines, config.getputratio, config.duration, runGetPutSync)
	case SQL:
		runGetPut(config.goroutines, config.getputratio, config.duration, runGetPutSQL)
	}

	timeElapsed := time.Now().Sub(timeStart)
	// print the summary results
	printResults(timeElapsed)
}

func openAndPingDB(servers string) *sql.DB {
	db, err := sql.Open(voltDBDriver, servers)
	if err != nil {
		fmt.Println("open")
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		fmt.Println("open")
		log.Fatal(err)
	}
	return db
}

func runGetPutSQL(join chan int, getputratio float64, duration time.Duration) {
	volt := openAndPingDB(config.servers)
	defer volt.Close()

	timeout := time.After(duration)
	// with prepared statement
	getStmt, getErr := volt.Prepare("select value from store where key = ?;")
	if getErr != nil {
		log.Fatal(getErr)
	}
	defer getStmt.Close()

	putStmt, putErr := volt.Prepare("upsert into store (key, value) values (?,?) ")
	if putErr != nil {
		log.Fatal(putErr)
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
				rows, err := getStmt.Query(bm.proc.generateRandomKeyForRetrieval())
				ops += handleSQLRows(rows, err)
			} else {
				key, rawValue, storeValue := bm.proc.generateForStore()
				atomic.AddUint64(&(fullStats.networkPutData), uint64(len(storeValue)))
				atomic.AddUint64(&(fullStats.rawPutData), uint64(len(rawValue)))
				if res, err := putStmt.Exec(key, storeValue); err != nil {
					ops += handleResultsError(err)
				} else {
					ops += handleResults(res)
				}
			}
		}

	}
}

func runGetPutSync(join chan int, getputratio float64, duration time.Duration) {
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
				if rows, err := volt.Query("STORE.select", []driver.Value{bm.proc.generateRandomKeyForRetrieval()}); err != nil {
					ops += handleDriverRowsError(err)
				} else {
					ops += handleDriverRows(bm.proc, rows)
				}
			} else {
				key, rawValue, storeValue := bm.proc.generateForStore()
				atomic.AddUint64(&(fullStats.networkPutData), uint64(len(storeValue)))
				atomic.AddUint64(&(fullStats.rawPutData), uint64(len(rawValue)))
				if res, err := volt.Exec("STORE.upsert", []driver.Value{key, storeValue}); err != nil {
					ops += handleResultsError(err)
				} else {
					ops += handleResults(res)
				}
			}
		}

	}

}

func runGetPutAsync(join chan int, getputratio float64, duration time.Duration) {
	volt := connect(config.servers)
	defer volt.Close()

	timeout := time.After(duration)

	gcb := newGetCallBack(bm.proc)
	pcb := newPutCallBack(bm.proc)
	ops := 0
	for {
		select {
		case <-timeout:
			volt.Drain()
			join <- ops
			return
		default:
			if rand.Float64() < getputratio {
				volt.QueryAsync(gcb, "STORE.select", []driver.Value{bm.proc.generateRandomKeyForRetrieval()})
			} else {
				key, rawValue, storeValue := bm.proc.generateForStore()
				atomic.AddUint64(&(fullStats.networkPutData), uint64(len(storeValue)))
				atomic.AddUint64(&(fullStats.rawPutData), uint64(len(rawValue)))
				volt.ExecAsync(pcb, "STORE.upsert", []driver.Value{key, storeValue})
			}
		}

	}
}

func runGetPut(gorotines int, getputratio float64, duration time.Duration,
	fn func(join chan int, getputratio float64, duration time.Duration)) {
	var joiners = make([]chan int, 0)
	for i := 0; i < gorotines; i++ {
		var joinchan = make(chan int)
		joiners = append(joiners, joinchan)
		go fn(joinchan, getputratio, duration)
	}

	// var totalCount = 0
	for _, join := range joiners {
		<-join
		// ops := <-join
		// totalCount += ops
		//fmt.Printf("kver %v finished and acted %v ops.\n", v, ops)
	}
	// fmt.Println(totalCount, fullStats.totalOps)
	return
}

// define Get Method Callback
type getCallBack struct {
	proc *payLoadProcessor
}

func newGetCallBack(proc *payLoadProcessor) getCallBack {
	gcb := new(getCallBack)
	gcb.proc = proc
	return *gcb
}

func (gcb getCallBack) ConsumeError(err error) {
	handleDriverRowsError(err)
}

// shouldn't call this
func (gcb getCallBack) ConsumeResult(res driver.Result) {
}

func (gcb getCallBack) ConsumeRows(rows driver.Rows) {
	handleDriverRows(gcb.proc, rows)
}

// define put Method Callback
type putCallBack struct {
	proc *payLoadProcessor
}

func newPutCallBack(proc *payLoadProcessor) putCallBack {
	pcb := new(putCallBack)
	pcb.proc = proc
	return *pcb
}

func (pcb putCallBack) ConsumeError(err error) {
	handleResultsError(err)
}

func (pcb putCallBack) ConsumeResult(res driver.Result) {
	handleResults(res)
}

// shouldn't call this
func (pcb putCallBack) ConsumeRows(rows driver.Rows) {
}

func handleSQLRows(rows *sql.Rows, err error) (success int) {
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			atomic.AddUint64(&(fullStats.successfulGets), 1)
			var val string
			if err = rows.Scan(&val); err == nil {
				rawValue, storeValue := bm.proc.retrieveFromStore([]byte(val))
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
		return 1
	}
	log.Panic(err)
	atomic.AddUint64(&(fullStats.failedGets), 1)
	return 0
}

func handleDriverRowsError(err error) (success int) {
	log.Panic(err)
	atomic.AddUint64(&(fullStats.failedGets), 1)
	return 0
}

func handleDriverRows(proc *payLoadProcessor, rows driver.Rows) (success int) {
	defer rows.Close()
	if voltRows := rows.(voltdbclient.VoltRows); voltRows.AdvanceRow() {
		atomic.AddUint64(&(fullStats.successfulGets), 1)
		if val, err := voltRows.GetVarbinary(1); err == nil {
			rawValue, storeValue := proc.retrieveFromStore(val.([]byte))
			atomic.AddUint64(&(fullStats.networkGetData), uint64(len(storeValue)))
			atomic.AddUint64(&(fullStats.rawGetData), uint64(len(rawValue)))
		} else {
			// shouldn't be here
			log.Panic(err)
			return 0
		}
	} else {
		atomic.AddUint64(&(fullStats.missedGets), 1)
	}
	atomic.AddUint64(&(fullStats.totalOps), 1)
	atomic.AddUint64(&(periodicStats.totalOps), 1)
	return 1
}

func handleResultsError(err error) (success int) {
	log.Panic(err)
	atomic.AddUint64(&(fullStats.failedPuts), 1)
	return 0
}

func handleResults(res driver.Result) (success int) {
	atomic.AddUint64(&(fullStats.successfulPuts), 1)
	atomic.AddUint64(&(fullStats.totalOps), 1)
	atomic.AddUint64(&(periodicStats.totalOps), 1)
	return 1
}

func connect(servers string) *voltdbclient.Conn {
	conn, err := voltdbclient.OpenConn(servers)
	if err != nil {
		log.Fatal(err)
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
	display := "\n" + horizontalRule +
		" KV Store Results\n" +
		horizontalRule +
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
	fmt.Print(horizontalRule)
	fmt.Println(" Client Workload Statistics")
	fmt.Println(horizontalRule)

	fmt.Printf("Performed %v ops in %v seconds (%0.0f ops/second)\n",
		fullStats.totalOps, timeElapsed.Seconds(),
		float64(fullStats.totalOps)/timeElapsed.Seconds())
}

func main() {
	flag.StringVar(&cpuprofile, "cpuprofile", "", "name of profile file to write")
	var err error
	config, err = newKVConfig()
	if err != nil {
		log.Fatal(err)
	}

	setupProfiler()
	defer teardownProfiler()

	bm, _ = newBenchmark()
	bm.runBenchmark()
}

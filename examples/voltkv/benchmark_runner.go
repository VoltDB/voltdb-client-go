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
	"reflect"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/VoltDB/voltdb-client-go/voltdbclient"
	"flag"
)

// handy, rather than typing this out several times
const HORIZONTAL_RULE = "----------" + "----------" + "----------" + "----------" +
	"----------" + "----------" + "----------" + "----------" + "\n"

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
	// os.Exit(0)
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
		runGetPutSync(config.goroutines, bm.processor, config.getputratio, config.warmup)
	}

	//reset the stats after warmup
	clear(fullStats)
	clear(periodicStats)

	// print periodic statistics to the console
	ticker = time.NewTicker(config.displayinterval)
	defer ticker.Stop()
	go printStatistics()
	timeStart := time.Now()

	// Run the benchmark loop for the requested duration
	// The throughput may be throttled depending on client configuration
	fmt.Print("\nRunning benchmark...\n")
	switch config.runtype {
	case ASYNC:
		runGetPutAsync(bm.conn, bm.processor, config.getputratio, config.duration)
		// block until all outstanding txns return
		// results := bm.conn.DrainAll()
	case SYNC:
		runGetPutSync(config.goroutines, bm.processor, config.getputratio, config.duration)
	}

	timeElapsed := time.Now().Sub(timeStart)
	// print the summary results
	printResults(timeElapsed)
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

	var totalCount = 0
	for _, join := range joiners {
		ops := <-join
		totalCount += ops
		//fmt.Printf("kver %v finished and acted %v ops.\n", v, ops)
	}
	// fmt.Println(totalCount, fullStats.totalOps)
	return
}

func runGetPut(join chan int, proc *payLoadProcessor, getputratio float64, duration time.Duration) {
	volt := connect(config.servers)
	defer volt.Close()

	ops := 0
	timeout := time.After(duration)
	for {
		select {
		case <-timeout:
			join <- ops
			return
		default:
			if rand.Float64() < getputratio {
				rows, err := volt.Query("STORE.select", []driver.Value{proc.generateRandomKeyForRetrieval()})
				ops += handleRows(proc, rows, err)
			} else {
				key, rawValue, storeValue := proc.generateForStore()
				atomic.AddUint64(&(fullStats.networkPutData), uint64(len(storeValue)))
				atomic.AddUint64(&(fullStats.rawPutData), uint64(len(rawValue)))
				res, err := volt.Exec("STORE.upsert", []driver.Value{key, storeValue})
				ops += handleResult(proc, res, err)
			}
		}

	}

}

func handleRows(proc *payLoadProcessor, rows driver.Rows, err error) (success int) {
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

func handleResult(proc *payLoadProcessor, res driver.Result, err error) (success int) {
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
	setupProfiler()
	defer teardownProfiler()

	var err error
	config, err = NewKVConfig()
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	flag.StringVar(&cpuprofile, "cpuprofile", "", "name of profile file to write")
	bm, _ := NewBenchmark()
	bm.runBenchmark()
}

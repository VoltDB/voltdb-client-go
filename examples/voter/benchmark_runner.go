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

// Initialize some common constants and variables
const contestantNamesCSV = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway," +
	"Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster," +
	"Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli"

// potential return codes (synced with Vote procedure)
const (
	voteSuccessful        int64 = 0
	errInvalidContestant  int64 = 1
	errVoterOverVoteLimit int64 = 2
)

// voter benchmark state
type benchmarkStats struct {
	totalVotes, acceptedVotes, badContestantVotes, badVoteCountVotes, failedVotes uint64
}

// helper function for clearing content of any type
func clear(v interface{}) {
	p := reflect.ValueOf(v).Elem()
	p.Set(reflect.Zero(p.Type()))
}

var (
	periodicStats, fullStats *benchmarkStats
	cpuprofile               = ""
	memprofile               = ""
	config                   *voterConfig
	ticker                   *time.Ticker
	timeStart                time.Time
	bm                       *benchmark
)

type benchmark struct {
	switchboard phoneCallGenerator
	conn        *voltdbclient.Conn
}

func newBenchmark() (*benchmark, error) {
	bmTemp := new(benchmark)
	bmTemp.switchboard = newPhoneCallGenerator(config.contestants)
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

	// connect to one or more servers, loop until success
	bm.conn = connect(config.servers)
	defer bm.conn.Close()

	// initialize using synchronous call
	fmt.Print("\nPopulating Static Tables\n")
	bm.conn.Exec("Initialize", []driver.Value{int32(config.contestants), contestantNamesCSV})

	fmt.Print(horizontalRule)
	fmt.Println(" Starting Benchmark")
	fmt.Println(horizontalRule)

	// Run the benchmark loop for the requested warmup time
	// The throughput may be throttled depending on client configuration
	fmt.Println("Warming up...")
	switch config.runtype {
	case ASYNC:
		vote(config.goroutines, config.warmup, placeVotesAsync)
	case SYNC:
		vote(config.goroutines, config.warmup, placeVotesSync)
	case SQL:
		vote(config.goroutines, config.warmup, placeVotesSQL)
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
		vote(config.goroutines, config.duration, placeVotesAsync)
	case SYNC:
		vote(config.goroutines, config.duration, placeVotesSync)
	case SQL:
		vote(config.goroutines, config.duration, placeVotesSQL)
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

func placeVotesSQL(join chan int, duration time.Duration) {
	volt := openAndPingDB(config.servers)
	defer volt.Close()

	timeout := time.After(duration)
	// don't support prepare statement with store procedure

	ops := 0
	for {
		select {
		case <-timeout:
			join <- ops
			return
		default:
			contestantNumber, phoneNumber := bm.switchboard.receive()
			rows, err := volt.Query("Vote", phoneNumber, contestantNumber, int64(config.maxvotes))
			ops += handleSQLRows(rows, err)
		}

	}
}

func placeVotesSync(join chan int, duration time.Duration) {
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
			contestantNumber, phoneNumber := bm.switchboard.receive()
			rows, err := volt.Query("Vote", []driver.Value{phoneNumber, contestantNumber, int64(config.maxvotes)})
			if err != nil {
				ops += handleVoteError(err)
			} else {
				ops += handleVoteReturnCode(rows)
			}
		}

	}

}

func placeVotesAsync(join chan int, duration time.Duration) {
	volt := connect(config.servers)
	defer volt.Close()
	// volt := bm.conn

	timeout := time.After(duration)

	vcb := newVoteCallBack()
	ops := 0
	for {
		select {
		case <-timeout:
			volt.Drain()
			join <- ops
			return
		default:
			contestantNumber, phoneNumber := bm.switchboard.receive()
			volt.QueryAsync(vcb, "Vote", []driver.Value{phoneNumber, contestantNumber, int64(config.maxvotes)})
		}

	}
}

func vote(gorotines int, duration time.Duration,
	fn func(join chan int, duration time.Duration)) {
	var joiners = make([]chan int, 0)
	for i := 0; i < gorotines; i++ {
		var joinchan = make(chan int)
		joiners = append(joiners, joinchan)
		go fn(joinchan, duration)
	}

	// var totalCount = 0
	for _, join := range joiners {
		<-join
		// ops := <-join
		// totalCount += ops
		//fmt.Printf("kver %v finished and acted %v ops.\n", v, ops)
	}
	return
}

type voteCallBack struct {
}

func newVoteCallBack() voteCallBack {
	vcb := new(voteCallBack)
	return *vcb
}

func (vcb voteCallBack) ConsumeError(err error) {
	handleVoteError(err)
}

// shouldn't call this
func (vcb voteCallBack) ConsumeResult(res driver.Result) {
}

func (vcb voteCallBack) ConsumeRows(rows driver.Rows) {
	handleVoteReturnCode(rows)
}

func handleSQLRows(rows *sql.Rows, err error) (success int) {
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			atomic.AddUint64(&(fullStats.totalVotes), 1)
			atomic.AddUint64(&(periodicStats.totalVotes), 1)
			var resultCode int64
			if err = rows.Scan(&resultCode); err == nil {
				switch resultCode {
				case errInvalidContestant:
					atomic.AddUint64(&(fullStats.badContestantVotes), 1)
				case errVoterOverVoteLimit:
					atomic.AddUint64(&(fullStats.badVoteCountVotes), 1)
				case voteSuccessful:
					atomic.AddUint64(&(fullStats.acceptedVotes), 1)
				}
			} else {
				log.Panic(err)
				// shouldn't be here
			}
			return 1
		}
		log.Panic(err)
		atomic.AddUint64(&(fullStats.failedVotes), 1)
		return 0
	}
	return 0
}

func handleVoteError(err error) (success int) {
	log.Panic(err)
	atomic.AddUint64(&(fullStats.failedVotes), 1)
	return 0
}

func handleVoteReturnCode(rows driver.Rows) (success int) {
	defer rows.Close()
	if voltRows := rows.(voltdbclient.VoltRows); voltRows.AdvanceRow() {
		resultCode, err := voltRows.GetBigInt(0)
		if err != nil {
			return handleVoteError(err)
		}
		atomic.AddUint64(&(fullStats.totalVotes), 1)
		atomic.AddUint64(&(periodicStats.totalVotes), 1)
		switch resultCode {
		case errInvalidContestant:
			atomic.AddUint64(&(fullStats.badContestantVotes), 1)
		case errVoterOverVoteLimit:
			atomic.AddUint64(&(fullStats.badVoteCountVotes), 1)
		case voteSuccessful:
			atomic.AddUint64(&(fullStats.acceptedVotes), 1)
		}
		return 1
	}
	return 0
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

func takeMemProfile() {
	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
		return
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
		fmt.Printf(" Throughput %v/s\n", float64(periodicStats.totalVotes)/config.displayinterval.Seconds())
		// fmt.Printf("Aborts/Failure %v/%v\n", periodicStats.aborts, periodicStats.failures)
		// fmt.Printf("Avg/95%% Latency %.2f/%.2fms\n")
		clear(periodicStats)
	}
}

func printResults(timeElapsed time.Duration) {
	// 1. Voting Board statistics, Voting results and performance statistics
	display := "\n" +
		horizontalRule +
		" Voting Results\n" +
		horizontalRule +
		"\nA total of %9d votes were received during the benchmark...\n" +
		" - %9d Accepted\n" +
		" - %9d Rejected (Invalid Contestant)\n" +
		" - %9d Rejected (Maximum Vote Count Reached)\n" +
		" - %9d Failed (Transaction Error)\n\n"

	fmt.Printf(display,
		fullStats.totalVotes,
		fullStats.acceptedVotes,
		fullStats.badContestantVotes,
		fullStats.badVoteCountVotes,
		fullStats.failedVotes)

	// 2. Voting results
	rows, err := bm.conn.Query("Results", []driver.Value{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Contestant Name\t\tVotes Received")
	voltRows := rows.(voltdbclient.VoltRows)
	defer voltRows.Close()

	for voltRows.AdvanceRow() {
		contestantName, contestantNameErr := voltRows.GetString(0)
		if contestantNameErr != nil {
			log.Fatal(contestantNameErr)
		}
		totalVotes, totalVotesErr := voltRows.GetBigIntByName("total_votes")
		if totalVotesErr != nil {
			log.Fatal(totalVotesErr)
		}
		fmt.Printf("%s\t\t%14d\n", contestantName, totalVotes)
	}
	if voltRows.AdvanceToRow(0) {
		winnerName, winnerErr := voltRows.GetString(0)
		if winnerErr != nil {
			log.Fatal(winnerErr)
		}

		fmt.Printf("\nThe Winner is: %s\n\n", winnerName)
	}

	// 3. Performance statistics

	fmt.Print(horizontalRule)
	fmt.Println(" Client Workload Statistics")
	fmt.Println(horizontalRule)

	fmt.Printf("Generated %v votes in %v seconds (%0.0f ops/second)\n",
		fullStats.totalVotes, timeElapsed.Seconds(),
		float64(fullStats.totalVotes)/timeElapsed.Seconds())
}

func main() {
	flag.StringVar(&cpuprofile, "cpuprofile", "", "name of profile file to write")
	flag.StringVar(&memprofile, "memprofile", "", "write memory profile to this file")
	var err error
	config, err = newVoterConfig()
	if err != nil {
		log.Fatal(err)
	}

	setupProfiler()
	defer teardownProfiler()

	bm, _ = newBenchmark()
	bm.runBenchmark()
}

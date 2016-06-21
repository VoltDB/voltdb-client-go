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
	"github.com/VoltDB/voltdb-client-go/voltdbclient"
	"log"
	"math/rand"
	"os"
)

// handy, rather than typing this out several times
const HORIZONTAL_RULE = "----------" + "----------" + "----------" + "----------" +
	"----------" + "----------" + "----------" + "----------" + "\n"

type benchmark struct {
	config kvConfig
	conn   *voltdbclient.VoltConn
	// time time.Timer
	// benchmarkStartTS
	processor *payLoadProcessor
	r         rand.Rand

	// Statistics manager objects from the client
	// periodicStatsContext
	// fullStatsContext

	// kv benchmark state
	successfulGets,
	missedGets,
	failedGets,
	rawGetData,
	networkGetData,
	successfulPuts,
	failedPuts,
	rawPutData,
	networkPutData uint64
}

func NewBenchmark(kv kvConfig) (*benchmark, error) {
	bm := new(benchmark)
	bm.config = kv
	bm.processor = NewPayloadProcessor(kv.keysize, kv.minvaluesize, kv.maxvaluesize, kv.entropy,
		kv.poolsize, kv.usecompression)

	return bm, nil
}

func (bm *benchmark) runBenchmark() {
	fmt.Print(HORIZONTAL_RULE)
	fmt.Println(" Setup & Initialization")
	fmt.Println(HORIZONTAL_RULE)
	bm.conn = connect(bm.config.servers)

	// preload keys if requested
	fmt.Println()
	if bm.config.preload {
		fmt.Println("Preloading data store...")
		for i:=0;i<bm.config.poolsize;i++ {
			if _, rawValue, storeValue := bm.processor.generateForStore(); storeValue != nil {
				bm.conn.ExecAsync("STORE.upsert", []driver.Value{fmt.Sprintf(bm.processor.keyFormat, i), storeValue})
			} else {
				bm.conn.ExecAsync("STORE.upsert", []driver.Value{fmt.Sprintf(bm.processor.keyFormat, i), rawValue})
			}
		}
		bm.conn.DrainAll()
		fmt.Println("Preloading complete.")
		fmt.Println()
	}

}

func connect(servers string) *voltdbclient.VoltConn {
	conn, err := voltdbclient.OpenConn("localhost:21212")
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
	bm,_ := NewBenchmark(*kv)
	bm.runBenchmark()
}

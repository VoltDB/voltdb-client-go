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
	"errors"
	"flag"
	"time"
	"fmt"
)

type runType string
const (
	ASYNC runType = "async"
	SYNC runType = "sync"
	STATEMENT runType = "statement"
)
func (p *runType) Set(s string) error {
	*p = runType(s)
	fmt.Println(s, p)
	return nil
}
func (p *runType) String() string {
	return fmt.Sprintf("%s", *p)
}

type kvConfig struct {
	displayinterval time.Duration
	duration        time.Duration
	servers         string
	poolsize        int
	preload         bool
	getputratio     float64
	keysize         int
	minvaluesize    int
	maxvaluesize    int
	entropy         int
	usecompression  bool
	goroutines      int
	runtype		runType
}

func NewKVConfig() (*kvConfig, error) {
	kv := new(kvConfig)
	flag.DurationVar(&(kv.displayinterval), "displayinterval", 5*time.Second, "Interval for performance feedback, in seconds.")
	flag.DurationVar(&(kv.duration), "duration", 15*time.Second, "Benchmark duration, in seconds.")
	flag.StringVar(&(kv.servers), "servers", "localhost", "Comma separated list of the form server[:port] to connect to.")
	flag.IntVar(&(kv.poolsize), "poolsize", 10000, "Number of keys to preload.")
	flag.BoolVar(&(kv.preload), "preload", true, "Whether to preload a specified number of keys and values.")
	flag.Float64Var(&(kv.getputratio), "getputratio", 0.90, "Fraction of ops that are gets (vs puts).")
	flag.IntVar(&(kv.keysize), "keysize", 32, "Size of keys in bytes.")
	flag.IntVar(&(kv.minvaluesize), "minvaluesize", 1024, "Minimum value size in bytes.")
	flag.IntVar(&(kv.maxvaluesize), "maxvaluesize", 1024, "Maximum value size in bytes.")
	flag.IntVar(&(kv.entropy), "entropy", 127, "Number of values considered for each value byte.")
	flag.BoolVar(&(kv.usecompression), "usecompression", false, "Compress values on the client side.")
	flag.IntVar(&(kv.goroutines), "goroutines", 40, "Number of concurrent goroutines synchronously calling procedures.")
	flag.Var(&(kv.runtype), "runtype", "Type of the client calling procedures.")
	flag.Parse()

	// validate
	switch {
	case (kv.displayinterval <= 0):
		return nil, errors.New("displayinterval must be > 0")
	case (kv.duration <= 0):
		return nil, errors.New("duration must be > 0")
	case (kv.poolsize <= 0):
		return nil, errors.New("poolsize must be > 0")
	case (kv.getputratio < 0) || (kv.getputratio > 1):
		return nil, errors.New("getputratio must be >= 0 and <= 1")
	case (kv.keysize <= 0) || (kv.keysize > 250):
		return nil, errors.New("keysize must be > 0 and <= 250")
	case (kv.minvaluesize <= 0):
		return nil, errors.New("minvaluesize must be > 0")
	case (kv.maxvaluesize <= 0):
		return nil, errors.New("maxvaluesize must be > 0")
	case (kv.entropy <= 0) || (kv.entropy > 127):
		return nil, errors.New("entropy must be > 0 and <= 127")
	case (kv.goroutines <= 0):
		return nil, errors.New("goroutines must be > 0")
	case (kv.runtype != ASYNC) && (kv.runtype != SYNC) && (kv.runtype != STATEMENT):
		return nil, errors.New("runtype should be async,sync or statement")

	default:
		return kv, nil
	}
}

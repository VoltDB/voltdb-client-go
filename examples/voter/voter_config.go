/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
	"fmt"
	"time"
)

type runType string

// runTypes
const (
	ASYNC runType = "async"
	SYNC  runType = "sync"
	SQL   runType = "sql"
)

func (p *runType) Set(s string) error {
	*p = runType(s)
	return nil
}
func (p *runType) String() string {
	return fmt.Sprintf("%s", *p)
}

type voterConfig struct {
	displayinterval time.Duration
	duration        time.Duration
	warmup          time.Duration
	servers         string

	contestants int
	maxvotes    int
	statsfile   string

	goroutines int
	runtype    runType
}

func newVoterConfig() (*voterConfig, error) {
	voter := new(voterConfig)
	flag.DurationVar(&(voter.displayinterval), "displayinterval", 5*time.Second, "Interval for performance feedback, in seconds.")
	flag.DurationVar(&(voter.duration), "duration", 120*time.Second, "Benchmark duration, in seconds.")
	flag.DurationVar(&(voter.warmup), "warmup", 5*time.Second, "Benchmark duration, in seconds.")
	flag.StringVar(&(voter.servers), "servers", "localhost:21212", "Comma separated list of the form server[:port] to connect to.")
	flag.IntVar(&(voter.contestants), "contestants", 6, "Number of contestants in the voting contest (from 1 to 10).")
	flag.IntVar(&(voter.maxvotes), "maxvotes", 2, "Maximum number of votes cast per voter.")
	flag.StringVar(&(voter.statsfile), "statsfile", "", "Filename to write raw summary statistics to.")
	flag.IntVar(&(voter.goroutines), "goroutines", 10, "Number of concurrent goroutines synchronously calling procedures.")
	flag.Var(&(voter.runtype), "runtype", "Type of the client calling procedures.")
	flag.Parse()
	if voter.runtype == "async" {
		if voter.goroutines == 10 {
			voter.goroutines = 1
		}
	}
	// validate
	switch {
	case (voter.displayinterval <= 0):
		return nil, errors.New("displayinterval must be > 0")
	case (voter.duration <= 0):
		return nil, errors.New("duration must be > 0")
	case (voter.contestants <= 0):
		return nil, errors.New("contestants must be > 0")
	case (voter.maxvotes <= 0):
		return nil, errors.New("maxvotes must be > 0")
	case (voter.goroutines <= 0):
		return nil, errors.New("goroutines must be > 0")
	case (voter.runtype != ASYNC) && (voter.runtype != SYNC) && (voter.runtype != SQL):
		return nil, errors.New("runtype should be async,sync or sql")

	default:
		return voter, nil
	}
}

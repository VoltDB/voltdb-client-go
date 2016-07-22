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

package voltdbclient

import (
	"errors"
	"math"
	"sync"
	"time"
)

const (
	BLOCK_DURATION = time.Millisecond * 100
	OUT_TXNS_LIMIT = 20 // as many outstanding transactions as we ever want.
)

type rateLimiter interface {
	limit(timeout time.Duration) error
	responseReceived(int32)
}

// latency limiter limits the number of outstanding transactions based on a desired latency.
// outstanding transactions are transactions that have been sent to the server and for which
// no response has been received.
type latencyLimiter struct {
	blockStart time.Time
	maxOutTxns int
	outTxns    int
	mutex      sync.RWMutex

	// latency for completed transactions in block so far
	latency       int32 // total latency of txn completed in this block
	latencyTxns   int   // number of txns contributing to latency (completed in this block)
	tune          bool
	latencyTarget int32
}

func newLatencyLimiter(latencyTarget int32) *latencyLimiter {
	var ll = new(latencyLimiter)
	ll.blockStart = time.Now()
	ll.maxOutTxns = OUT_TXNS_LIMIT
	ll.outTxns = 0
	ll.latencyTarget = latencyTarget
	return ll
}

func (ll *latencyLimiter) limit(timeout time.Duration) error {
	start := time.Now()
	for !ll.getPermit() {
		time.Sleep(10 * time.Millisecond)
		if time.Since(start).Nanoseconds() > timeout.Nanoseconds() {
			return errors.New("timeout")
		}
	}
	return nil
}

func (ll *latencyLimiter) responseReceived(latency int32) {
	ll.mutex.Lock()
<<<<<<< HEAD
	defer ll.mutex.Unlock()
	for ll.nextBlock() {
		ll.calcLatency()
	}
	if latency == -1 {
		// no value for latency received.
		ll.outTxns--
	} else {
		ll.latency += latency
		ll.latencyTxns++
		ll.outTxns--
	}
=======
	for ll.nextBlock() {
		ll.calcLatency()
	}
	ll.latency += latency
	ll.latencyTxns++
	ll.outTxns--
	ll.mutex.Unlock()
>>>>>>> origin/master
}

func (ll *latencyLimiter) getPermit() bool {
	var permit bool
	ll.mutex.Lock()
	permit = ll.outTxns <= ll.maxOutTxns
	if permit {
		ll.outTxns++
	}
	ll.mutex.Unlock()
	return permit
}

func (ll *latencyLimiter) nextBlock() bool {
	var nextBlock bool = false
	for time.Since(ll.blockStart).Nanoseconds() > BLOCK_DURATION.Nanoseconds() {
		ll.blockStart = ll.blockStart.Add(BLOCK_DURATION)
		nextBlock = true
	}
	return nextBlock
}

func (ll *latencyLimiter) calcLatency() {
	// if there weren't any transactions then have no input about latency, do nothing.
	if ll.latencyTxns == 0 {
		return
	}
	avgLatency := int32(math.Ceil(float64(ll.latency) / float64(ll.latencyTxns)))
	if avgLatency > ll.latencyTarget {
		ll.maxOutTxns = int(math.Floor(float64(ll.maxOutTxns) * 0.9))
		if ll.maxOutTxns == 0 {
			ll.maxOutTxns = 1
		}
	} else {
		if ll.maxOutTxns < OUT_TXNS_LIMIT {
			ll.maxOutTxns = int(math.Ceil(float64(ll.maxOutTxns) * 1.1))
			if ll.maxOutTxns > OUT_TXNS_LIMIT {
				ll.maxOutTxns = OUT_TXNS_LIMIT
			}
		}
	}
	ll.latency = 0
	ll.latencyTxns = 0
}

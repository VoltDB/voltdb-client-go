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
	"time"
)

type empty struct{}
type semaphore chan empty

type txnLimiter struct {
	txnSems semaphore
}

func newTxnLimiter() *txnLimiter {
	var tl = new(txnLimiter)
	tl.txnSems = make(semaphore, 10)
	return tl
}

func newTxnLimiterWithMaxOutTxns(maxOutTxns int) *txnLimiter {
	var tl = new(txnLimiter)
	tl.txnSems = make(semaphore, maxOutTxns)
	return tl
}

// interface for rateLimiter
func (tl *txnLimiter) limit(timeout time.Duration) error {
	select {
	case tl.txnSems <- empty{}:
		return nil
	case <-time.After(timeout):
		return errors.New("timeout waiting for transaction permit.")
	}
}

// interface for rateLimiter
func (tl *txnLimiter) responseReceived(latency int32) {
	_ = <-tl.txnSems
}

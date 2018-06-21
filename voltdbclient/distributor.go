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

package voltdbclient

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// DefaultQueryTimeout time out for queries.
	DefaultQueryTimeout time.Duration = 2 * time.Minute
)

var handle int64
var sHandle int64 = -1

var ErrMissingServerArgument = errors.New("voltdbclient: missing voltdb connection string")

// ProtocolVersion lists the version of the voltdb wire protocol to use.
// For VoltDB releases of version 5.2 and later use version 1. For releases
// prior to that use version 0.
var ProtocolVersion = 1

// Conn holds the set of currently active connections.
type Conn struct {
	closeCh                                  chan chan bool
	open                                     atomic.Value
	rl                                       rateLimiter
	useClientAffinity                        bool
	sendReadsToReplicasBytDefaultIfCAEnabled bool
	connected                                []*nodeConn
	ctx                                      context.Context
	cancel                                   func()
	PartitionDetails                         *PartitionDetails
	hostIDToConnection                       map[int]*nodeConn
}

func newConn(cis []string) (*Conn, error) {
	ctx, cancel := context.WithCancel(context.Background())
	var c = &Conn{
		closeCh:            make(chan chan bool),
		rl:                 newTxnLimiter(),
		useClientAffinity:  true,
		ctx:                ctx,
		cancel:             cancel,
		hostIDToConnection: make(map[int]*nodeConn),
	}
	c.open.Store(true)
	if err := c.start(cis); err != nil {
		return nil, err
	}
	return c, nil
}

// OpenConn returns a new connection to the VoltDB server.  The name is a string
// in a driver-specific format.  The returned connection can be used by only one
// goroutine at a time.
//
// By default voltdb doesn't require authentication,
// clients connecting to un secured database have access to everything.
// Supplying connection credentials doesn't affect for non secured databases
//
// Here we authenticate if username and password are supplied, if they are not
// then a connection is established without doing the authentication
//
// Connection string is similar to postgres, default port is 21212
//
// voltdb://
// voltdb://localhost
// voltdb://localhost:21212
// voltdb://user@localhost
// voltdb://user:secret@localhost
// voltdb://other@localhost?some_param=some_value
//
// You can omit the port,and the default port of 21212 will be automatically
// added for you.
//
// Additionally you can fine tune behavior of connections when in cluster mode
// using query parameters.
//
// Example localhost:21212?max_retries=10&retry=true&retry_interval=1s
//
// retry - if true will try to reconnect with the node when the connection is
// lost.
//
// max_retries - in the number of times you want to retry to connect to a node.
// This has no effect when retry is false.
//
// retry_interval is the duration of time to wait until the next retry.
func OpenConn(ci string) (*Conn, error) {
	ci = strings.TrimSpace(ci)
	if ci == "" {
		return nil, ErrMissingServerArgument
	}
	cis := strings.Split(ci, ",")
	return newConn(cis)
}

// OpenConnWithLatencyTarget returns a new connection to the VoltDB server.
// This connection will try to meet the specified latency target, potentially by
// throttling the rate at which asynchronous transactions are submitted.
func OpenConnWithLatencyTarget(ci string, latencyTarget int32) (*Conn, error) {
	ci = strings.TrimSpace(ci)
	if ci == "" {
		return nil, ErrMissingServerArgument
	}
	cis := strings.Split(ci, ",")
	c, err := newConn(cis)
	if err != nil {
		return nil, err
	}
	c.rl = newLatencyLimiter(latencyTarget)
	return c, nil
}

// OpenConnWithMaxOutstandingTxns returns a new connection to the VoltDB server.
// This connection will limit the number of outstanding transactions as
// indicated. An outstanding transaction is a transaction that has been sent to
// the server but for which no response has been received.
func OpenConnWithMaxOutstandingTxns(ci string, maxOutTxns int) (*Conn, error) {
	ci = strings.TrimSpace(ci)
	if ci == "" {
		return nil, ErrMissingServerArgument
	}
	cis := strings.Split(ci, ",")
	c, err := newConn(cis)
	if err != nil {
		return nil, err
	}
	c.rl = newTxnLimiterWithMaxOutTxns(maxOutTxns)
	return c, nil
}

func (c *Conn) start(cis []string) error {
	var (
		err          error
		disconnected []*nodeConn
	)
	for _, ci := range cis {
		nc := newNodeConn(ci)
		if err = nc.connect(c.ctx, ProtocolVersion); err != nil {
			disconnected = append(disconnected, nc)
			continue
		}
		c.connected = append(c.connected, nc)
		if c.useClientAffinity {
			c.hostIDToConnection[int(nc.connData.HostID)] = nc
		}
	}
	if len(c.connected) == 0 {
		return fmt.Errorf("No valid connections %v", err)
	}
	return nil
}

//Returns a node connection that is not closed.
func (c *Conn) getConn() *nodeConn {
	if len(c.connected) == 1 {
		return c.connected[0]
	}
	size := len(c.connected)
	idx := rand.Intn(size)
	nc := c.connected[idx]
	if nc.isClosed() {
		for {
			n := rand.Intn(size)
			nc = c.connected[n]
			if !nc.isClosed() {
				return nc
			}
		}
	}
	return nc
}

func (c *Conn) availableConn() *nodeConn {
	return c.getConn()
}

// Submit the pi to an available connection. If the client affinity was enabled
// then we use client affinity to select the node connection.
//
// In the event of client affinity picks a dead node, we fallback to picking a
// random available connection.
func (c *Conn) submit(ctx context.Context, pi *procedureInvocation) (int, error) {
	var nc *nodeConn
	if c.useClientAffinity && len(c.connected) > 1 {
		if c.PartitionDetails == nil {
			nc = c.availableConn()
			details, err := c.GetPartitionDetails(nc)
			if err != nil {
				return 0, err
			}
			c.PartitionDetails = details
		}
		conn, _, err := c.getConnByCA(c.PartitionDetails, pi.query, pi.params)
		if err != nil {
			return 0, err
		}
		if conn != nil {
			nc = conn
			if conn.isClosed() {
				if nc == nil {
					// we only do this if we didn't get available connection yet.
					nc = c.availableConn()

				}
			}
		}

	} else {
		nc = c.availableConn()
	}
	if nc == nil {
		nc = c.availableConn()
	}
	return nc.submit(ctx, pi)
}

// Begin starts a transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	return nil, nil
}

// Close closes the connection to the VoltDB server.  Connections to the server
// are meant to be long lived; it should not be necessary to continually close
// and reopen connections.  Close would typically be called using a defer.
// Operations using a closed connection cause a panic.
func (c *Conn) Close() error {
	if c.isClosed() {
		return nil
	}
	for _, nc := range c.connected {
		err := nc.Close()
		if err != nil {
			return err
		}
	}
	c.cancel()
	c.setClosed()
	return nil
}

// Drain blocks until all outstanding asynchronous requests have been satisfied.
// Asynchronous requests are processed in a background goroutine; this call blocks
// the current thread until that background goroutine has finished with all
// asynchronous requests.
func (c *Conn) Drain() error {
	if !c.isClosed() {
		for _, nc := range c.connected {
			log.Println("start draining ", nc.connInfo)
			err := nc.Drain(c.ctx)
			if err != nil {
				return err
			}
			log.Println("done draining ", nc.connInfo)
		}
	}
	return nil
}

func (c *Conn) assertOpen() {
	if !(c.open.Load().(bool)) {
		panic("Tried to use closed connection pool")
	}
}

func (c *Conn) isClosed() bool {
	return !(c.open.Load().(bool))
}

func (c *Conn) setClosed() {
	c.open.Store(false)
}

func (c *Conn) getNextHandle() int64 {
	return atomic.AddInt64(&handle, 1)
}

func (c *Conn) getNextSystemHandle() int64 {
	return atomic.AddInt64(&sHandle, -1)
}

type procedure struct {
	SinglePartition        bool `json:"singlePartition"`
	ReadOnly               bool `json:"readOnly"`
	PartitionParameter     int  `json:"partitionParameter"`
	PartitionParameterType int  `json:"partitionParameterType"`
}

func (proc *procedure) setDefaults() {
	const ParameterNone = -1
	if !proc.SinglePartition {
		proc.PartitionParameter = ParameterNone
		proc.PartitionParameterType = ParameterNone
	}
}

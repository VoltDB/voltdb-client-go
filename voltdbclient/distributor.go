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
	"database/sql/driver"
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

// ProtocolVersion lists the version of the voltdb wire protocol to use.
// For VoltDB releases of version 5.2 and later use version 1. For releases
// prior to that use version 0.
var ProtocolVersion = 1

// Conn holds the set of currently active connections.
type Conn struct {
	inPiCh                                   chan *procedureInvocation
	allNcsPiCh                               chan *procedureInvocation
	closeCh                                  chan chan bool
	open                                     atomic.Value
	rl                                       rateLimiter
	drainCh                                  chan chan bool
	useClientAffinity                        bool
	sendReadsToReplicasBytDefaultIfCAEnabled bool
}

func newConn(cis []string) (*Conn, error) {
	var c = new(Conn)
	c.inPiCh = make(chan *procedureInvocation, 1000)
	c.allNcsPiCh = make(chan *procedureInvocation, 1000)
	c.closeCh = make(chan chan bool)
	c.open = atomic.Value{}
	c.open.Store(true)
	c.rl = newTxnLimiter()
	c.drainCh = make(chan chan bool)
	c.useClientAffinity = true
	c.sendReadsToReplicasBytDefaultIfCAEnabled = false

	err := c.start(cis)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// OpenConn returns a new connection to the VoltDB server.  The name is a
// string in a driver-specific format.  The returned connection can be used by
// only one goroutine at a time.
func OpenConn(ci string) (*Conn, error) {
	cis := strings.Split(ci, ",")
	return newConn(cis)
}

// OpenConnWithLatencyTarget returns a new connection to the VoltDB server.
// This connection will try to meet the specified latency target, potentially by
// throttling the rate at which asynchronous transactions are submitted.
func OpenConnWithLatencyTarget(ci string, latencyTarget int32) (*Conn, error) {
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
	cis := strings.Split(ci, ",")
	c, err := newConn(cis)
	if err != nil {
		return nil, err
	}
	c.rl = newTxnLimiterWithMaxOutTxns(maxOutTxns)
	return c, nil
}

func (c *Conn) start(cis []string) error {

	var connected []*nodeConn
	var disconnected []*nodeConn
	hostIDToConnection := make(map[int]*nodeConn)
	var err error
	for _, ci := range cis {
		ncPiCh := make(chan *procedureInvocation, 1000)
		nc := newNodeConn(ci, ncPiCh)

		err = nc.connect(ProtocolVersion, c.allNcsPiCh)
		if err == nil {
			connected = append(connected, nc)
			if c.useClientAffinity {
				hostIDToConnection[int(nc.connData.hostID)] = nc
			}
		} else {
			disconnected = append(disconnected, nc)
		}
	}
	if len(connected) == 0 {
		return fmt.Errorf("%v %v", "No valid connections", err)
	}
	go c.loop(connected, disconnected, &hostIDToConnection)
	return nil
}

func (c *Conn) loop(connected []*nodeConn, disconnected []*nodeConn, hostIDToConnection *map[int]*nodeConn) {

	// TODO: resubsribe when we lose the subscribed connection
	var (
		subscribedConnection *nodeConn
		subTopoCh            <-chan voltResponse
		topoStatsCh          <-chan voltResponse
		hasTopoStats         bool
		prInfoCh             <-chan voltResponse
		fetchedCatalog       bool

		closeRespCh           chan bool
		closingNcsCh          chan bool
		outstandingCloseCount int

		draining              bool
		drainRespCh           chan bool
		drainingNcsCh         chan bool
		outstandingDrainCount int

		hnator            hashinator
		partitionReplicas *map[int][]*nodeConn
		partitionMasters  = make(map[int]*nodeConn)

		procedureInfos *map[string]procedure
	)

	for {
		if draining {
			if len(c.inPiCh) == 0 && outstandingDrainCount == 0 {
				drainRespCh <- true
				drainingNcsCh = nil
				draining = false
			}
		}

		if c.useClientAffinity && subscribedConnection == nil && len(connected) > 0 {
			nc := connected[rand.Intn(len(connected))]
			subTopoCh = c.subscribeTopo(nc)
			subscribedConnection = nc
		}

		if c.useClientAffinity && !hasTopoStats && len(connected) > 0 {
			nc := connected[rand.Intn(len(connected))]
			topoStatsCh = c.getTopoStatistics(nc)
			hasTopoStats = true
		}
		if c.useClientAffinity && !fetchedCatalog && len(connected) > 0 {
			nc := connected[rand.Intn(len(connected))]
			prInfoCh = c.getProcedureInfo(nc)
			fetchedCatalog = true

		}

		select {
		case closeRespCh = <-c.closeCh:
			c.inPiCh = nil
			c.allNcsPiCh = nil
			c.drainCh = nil
			c.closeCh = nil
			if len(connected) == 0 {
				closeRespCh <- true
			} else {
				outstandingCloseCount = len(connected)
				closingNcsCh = make(chan bool, len(connected))
				for _, connectedNc := range connected {
					responseCh := connectedNc.close()
					go func() { closingNcsCh <- <-responseCh }()
				}
			}
		case <-closingNcsCh:
			outstandingCloseCount--
			if outstandingCloseCount == 0 {
				closeRespCh <- true
				return
			}
		case topoResp := <-subTopoCh:
			switch topoResp.(type) {
			// handle an error, otherwise the subscribe succeeded.
			case VoltError:
				if ResponseStatus(topoResp.getStatus()) == ConnectionLost {
					// TODO: handle this.  Move the connection out of connected, try again.
					// TODO: try to reconnect to the host in a separate go routine.
					// TODO: subscribe to topo a second time
				}
				subscribedConnection = nil
			default:
				subTopoCh = nil
			}
		case topoStatsResp := <-topoStatsCh:
			switch topoStatsResp.(type) {
			case VoltRows:
				tmpHnator, tmpPartitionReplicas, err := c.updateAffinityTopology(topoStatsResp.(VoltRows))
				if err == nil {
					hnator = tmpHnator
					partitionReplicas = tmpPartitionReplicas
					topoStatsCh = nil
				} else {
					hasTopoStats = false
				}
			default:
				hasTopoStats = false
			}
		case prInfoResp := <-prInfoCh:
			switch prInfoResp.(type) {
			case VoltRows:
				tmpProcedureInfos, err := c.updateProcedurePartitioning(prInfoResp.(VoltRows))
				if err != nil {
					procedureInfos = tmpProcedureInfos
					prInfoCh = nil
				} else {
					fetchedCatalog = false
				}
			default:
				fetchedCatalog = false
			}
		case pi := <-c.inPiCh:
			var nc *nodeConn
			var backpressure = true
			var err error
			if c.useClientAffinity && hnator != nil && partitionReplicas != nil && procedureInfos != nil {
				nc, backpressure, err = c.getConnByCA(connected, hnator, &partitionMasters, partitionReplicas, procedureInfos, pi)
			}
			if err != nil && !backpressure && nc != nil {
				nc.submit(pi)
			} else {
				c.allNcsPiCh <- pi
			}
		case drainRespCh = <-c.drainCh:
			if !draining {
				if len(connected) == 0 {
					drainRespCh <- true
				} else {
					draining = true
					outstandingDrainCount = len(connected)
					drainingNcsCh = make(chan bool, len(connected))
					for _, connectedNc := range connected {
						responseCh := make(chan bool, 1)
						connectedNc.drain(responseCh)
						go func() { drainingNcsCh <- <-responseCh }()
					}
				}
			}
		case <-drainingNcsCh:
			outstandingDrainCount--
		}
	}

	// have the set of ncs.
	// I have some data structures that go with client affinity.

	// each time through the loop.
	// look for new pis, assign to some nc
	// for reconnectings nc's, see if reconnected.
	// check error channel to see if any lost connections.
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
	respCh := make(chan bool)
	c.closeCh <- respCh
	<-respCh
	return nil
}

// Drain blocks until all outstanding asynchronous requests have been satisfied.
// Asynchronous requests are processed in a background thread; this call blocks
// the current thread until that background thread has finished with all
// asynchronous requests.
func (c *Conn) Drain() {
	drainRespCh := make(chan bool, 1)
	c.drainCh <- drainRespCh
	<-drainRespCh
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

func panicIfnotNil(str string, err error) {
	if err != nil {
		log.Panic(str, err)
	}
}

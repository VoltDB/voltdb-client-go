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
	closeCh                                  chan chan bool
	open                                     atomic.Value
	rl                                       rateLimiter
	drainCh                                  chan chan bool
	useClientAffinity                        bool
	sendReadsToReplicasBytDefaultIfCAEnabled bool
	subscribedConnection                     *nodeConn
	connected                                []*nodeConn
	hasTopoStats                             bool
	subTopoCh                                <-chan voltResponse
	topoStatsCh                              <-chan voltResponse
	prInfoCh                                 <-chan voltResponse
	fetchedCatalog                           bool
	hnator                                   hashinator
	partitionReplicas                        *map[int][]*nodeConn
	procedureInfos                           *map[string]procedure
	partitionMasters                         map[int]*nodeConn
}

func newConn(cis []string) (*Conn, error) {
	var c = &Conn{
		closeCh:           make(chan chan bool),
		rl:                newTxnLimiter(),
		drainCh:           make(chan chan bool),
		useClientAffinity: true,
		partitionMasters:  make(map[int]*nodeConn),
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
	var (
		err                error
		disconnected       []*nodeConn
		hostIDToConnection = make(map[int]*nodeConn)
	)

	for _, ci := range cis {
		nc := newNodeConn(ci)
		if err = nc.connect(ProtocolVersion); err != nil {
			disconnected = append(disconnected, nc)
			continue
		}
		c.connected = append(c.connected, nc)
		if c.useClientAffinity {
			hostIDToConnection[int(nc.connData.HostID)] = nc
		}
	}

	if len(c.connected) == 0 {
		return fmt.Errorf("No valid connections %v", err)
	}

	go c.loop(disconnected, &hostIDToConnection)
	return nil
}

//Returns a node connection that is not closed.
func (c *Conn) getConn() *nodeConn {
	size := len(c.connected)
	idx := rand.Intn(size)
	nc := c.connected[idx]
	if nc.isClosed() {
		for {
			n := rand.Intn(size)
			if n != idx {
				return c.connected[n]
			}
		}
	}
	return nc
}

func (c *Conn) availableConn() *nodeConn {
	nc := c.getConn()
	c.subscribedConnection = nc
	if c.useClientAffinity && c.subscribedConnection == nil && len(c.connected) > 0 {
		c.subTopoCh = c.subscribeTopo(nc)
	}
	if c.useClientAffinity && !c.hasTopoStats && len(c.connected) > 0 {
		c.topoStatsCh = c.getTopoStatistics(nc)
		c.hasTopoStats = true
	}
	if c.useClientAffinity && !c.fetchedCatalog && len(c.connected) > 0 {
		c.prInfoCh = c.getProcedureInfo(nc)
		c.fetchedCatalog = true
	}
	return c.subscribedConnection
}

func (c *Conn) loop(disconnected []*nodeConn, hostIDToConnection *map[int]*nodeConn) {

	// TODO: resubsribe when we lose the subscribed connection
	var (
		closeRespCh           chan bool
		closingNcsCh          chan bool
		outstandingCloseCount int

		draining              bool
		drainRespCh           chan bool
		drainingNcsCh         chan bool
		outstandingDrainCount int
	)

	for {
		if draining {
			if outstandingDrainCount == 0 {
				drainRespCh <- true
				drainingNcsCh = nil
				draining = false
			}
		}
		select {
		case closeRespCh = <-c.closeCh:
			c.drainCh = nil
			c.closeCh = nil
			if len(c.connected) == 0 {
				closeRespCh <- true
			} else {
				outstandingCloseCount = len(c.connected)
				closingNcsCh = make(chan bool, len(c.connected))
				for _, connectedNc := range c.connected {
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
		case topoResp := <-c.subTopoCh:
			switch topoResp.(type) {
			// handle an error, otherwise the subscribe succeeded.
			case VoltError:
				if ResponseStatus(topoResp.getStatus()) == ConnectionLost {
					// TODO: handle this.  Move the connection out of connected, try again.
					// TODO: try to reconnect to the host in a separate go routine.
					// TODO: subscribe to topo a second time
				}
				c.subscribedConnection = nil
			default:
				c.subTopoCh = nil
			}
		case topoStatsResp := <-c.topoStatsCh:
			switch topoStatsResp.(type) {
			case VoltRows:
				tmpHnator, tmpPartitionReplicas, err := c.updateAffinityTopology(topoStatsResp.(VoltRows))
				if err == nil {
					c.hnator = tmpHnator
					c.partitionReplicas = tmpPartitionReplicas
					c.topoStatsCh = nil
				} else {
					if err.Error() != errLegacyHashinator.Error() {
						c.hasTopoStats = false
					}
				}
			default:
				c.hasTopoStats = false
			}
		case prInfoResp := <-c.prInfoCh:
			switch prInfoResp.(type) {
			case VoltRows:
				tmpProcedureInfos, err := c.updateProcedurePartitioning(prInfoResp.(VoltRows))
				if err == nil {
					c.procedureInfos = tmpProcedureInfos
					c.prInfoCh = nil
				} else {
					c.fetchedCatalog = false
				}
			default:
				c.fetchedCatalog = false
			}
		case drainRespCh = <-c.drainCh:
			if !draining {
				if len(c.connected) == 0 {
					drainRespCh <- true
				} else {
					draining = true
					outstandingDrainCount = len(c.connected)
					drainingNcsCh = make(chan bool, len(c.connected))
					for _, connectedNc := range c.connected {
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

func (c *Conn) submit(pi *procedureInvocation) (int, error) {
	nc := c.availableConn()
	// var nc *nodeConn
	// var backpressure = true
	// var err error
	// if c.useClientAffinity && c.hnator != nil && c.partitionReplicas != nil && c.procedureInfos != nil {
	// 	nc, backpressure, err =
	// 		c.getConnByCA(c.connected, c.hnator, &c.partitionMasters, c.partitionReplicas, c.procedureInfos, pi)
	// }
	// if err != nil && !backpressure && nc != nil {
	// 	// nc.submit(pi)
	// } else {
	// 	// c.allNcsPiCh <- pi
	// }
	return nc.submit(pi)
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

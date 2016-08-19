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
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Default time out for queries.
	DEFAULT_QUERY_TIMEOUT time.Duration = 2 * time.Minute
)

var handle int64 = 0
var sHandle int64 = -1

// the set of currently active connections
type Conn struct {
	ncs []*nodeConn
	// next conn to look at when finding by round robin.
	ncIndex int
	ncLen   int
	ncMutex sync.Mutex
	piCh    chan *procedureInvocation
	open    atomic.Value
	h       hashinator
	rl      rateLimiter

	subscribedConnection       *nodeConn // The connection we have issued our subscriptions to.
	subscriptionRequestPending bool

	fetchedCatalog                           bool
	ignoreBackpressure                       bool
	useClientAffinity                        bool
	sendReadsToReplicasBytDefaultIfCAEnabled bool
	partitonMutex                            sync.RWMutex
	partitionMasters                         map[int]*nodeConn
	partitionReplicas                        map[int][]*nodeConn
	hostIdToConnection                       map[int]*nodeConn
	procedureInfos                           map[string]procedure
}

func newConn(cis []string) (*Conn, error) {
	var c = new(Conn)
	c.ncIndex = 0
	c.open = atomic.Value{}
	c.open.Store(true)
	c.rl = newTxnLimiter()
	c.useClientAffinity = true
	c.fetchedCatalog = true
	c.ignoreBackpressure = false
	c.sendReadsToReplicasBytDefaultIfCAEnabled = false
	c.partitionMasters = make(map[int]*nodeConn)
	c.partitionReplicas = make(map[int][]*nodeConn)
	c.hostIdToConnection = make(map[int]*nodeConn)
	c.procedureInfos = make(map[string]procedure)

	err := c.makeNodeConns(cis)
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

// OpenConn returns a new connection to the VoltDB server.  This connection
// will try to meet the specified latency target, potentially by throttling
// the rate at which asynchronous transactions are submitted.
func OpenConnWithLatencyTarget(ci string, latencyTarget int32) (*Conn, error) {
	cis := strings.Split(ci, ",")
	c, err := newConn(cis)
	if err != nil {
		return nil, err
	}
	c.rl = newLatencyLimiter(latencyTarget)
	return c, nil
}

// OpenConn returns a new connection to the VoltDB server.  This connection
// will limit the number of outstanding transactions as indicated.  An
// outstanding transaction is a transaction that has been sent to the server
// but for which no response has been received.
func OpenConnWithMaxOutstandingTxns(ci string, maxOutTxns int) (*Conn, error) {
	cis := strings.Split(ci, ",")
	c, err := newConn(cis)
	if err != nil {
		return nil, err
	}
	c.rl = newTxnLimiterWithMaxOutTxns(maxOutTxns)
	return c, nil
}

func (c *Conn) makeNodeConns(cis []string) error {
	ncs := make([]*nodeConn, len(cis))
	c.piCh = make(chan *procedureInvocation, 1000)
	for i, ci := range cis {
		ncPiCh := make(chan *procedureInvocation, 1000)
		nc := newNodeConn(ci, c, ncPiCh)
		ncs[i] = nc
		err := nc.connect(c.piCh)
		if err != nil {
			return err
		}
		if c.useClientAffinity {
			c.hostIdToConnection[int(nc.connData.hostId)] = nc
		}
	}
	c.ncs = ncs
	c.ncLen = len(ncs)
	if c.useClientAffinity {
		c.subscribeToNewNode()
	}
	return nil
}

// Begin starts a transaction.  VoltDB runs in auto commit mode, and so Begin
// returns an error.
func (c *Conn) Begin() (driver.Tx, error) {
	c.assertOpen()
	return nil, errors.New("VoltDB does not support client side transaction control.")
}

// Close closes the connection to the VoltDB server.  Connections to the server
// are meant to be long lived; it should not be necessary to continually close
// and reopen connections.  Close would typically be called using a defer.
// Operations using a closed connection cause a panic.
func (c *Conn) Close() (err error) {
	if c.isClosed() {
		return
	}
	c.setClosed()

	// once this is closed there shouldn't be any additional activity against
	// the connections.  They're touched here without getting a lock.
	for _, nc := range c.ncs {
		err := nc.close()
		if err != nil {
			log.Printf("Failed to close connection with %v\n", err)
		}
	}
	c.ncs = nil
	return nil
}

// Drain blocks until all outstanding asynchronous requests have been satisfied.
// Asynchronous requests are processed in a background thread; this call blocks the
// current thread until that background thread has finished with all asynchronous requests.
func (c *Conn) Drain() {
	c.assertOpen()

	responseChs := make([]chan bool, c.ncLen)
	for i, nc := range c.ncs {
		responseCh := make(chan bool)
		responseChs[i] = responseCh
		nc.drain(responseCh)
	}
	for _, responseCh := range responseChs {
		<-responseCh
	}
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

// Try to find optimal connection using client affinity
// return picked connection if found else nil
// also return backpressure
// this method is not thread safe
func (c *Conn) getConnByCA(pi *procedureInvocation) (cxn *nodeConn, backpressure bool, err error) {
	backpressure = true

	if c.ncLen == 0 {
		return cxn, backpressure, errors.New("No connections.")
	}

	// Check if the master for the partition is known.
	if c.useClientAffinity && c.h != nil {
		var hashedPartition int = -1

		if procedureInfo, ok := c.procedureInfos[pi.query]; ok {
			hashedPartition = MP_INIT_PID
			// User may have passed too few parameters to allow dispatching.
			if procedureInfo.SinglePartition && procedureInfo.PartitionParameter < pi.getPassedParamCount() {
				if hashedPartition, err = c.h.getHashedPartitionForParameter(procedureInfo.PartitionParameterType,
					pi.getPartitionParamValue(procedureInfo.PartitionParameter)); err != nil {
					return
				}

			}

			// If the procedure is read only and single part, load balance across replicas
			// This is probably slower for SAFE consistency.
			if procedureInfo.SinglePartition && procedureInfo.ReadOnly && c.sendReadsToReplicasBytDefaultIfCAEnabled {
				c.partitonMutex.RLock()
				partitionReplica := c.partitionReplicas[hashedPartition]
				c.partitonMutex.RUnlock()
				if len(partitionReplica) > 0 {
					cxn = partitionReplica[rand.Intn(len(partitionReplica))]
					if cxn.hasBP() {
						//See if there is one without backpressure, make sure it's still connected
						for _, nc := range partitionReplica {
							if !nc.hasBP() && nc.isOpen() {
								cxn = nc
								break
							}
						}
					}
					if !cxn.hasBP() || c.ignoreBackpressure {
						backpressure = false
					}
				}
			} else {
				// Writes and Safe Reads have to go to the master
				c.partitonMutex.RLock()
				cxn = c.partitionMasters[hashedPartition]
				c.partitonMutex.RUnlock()
				if (cxn != nil && !cxn.hasBP()) || c.ignoreBackpressure {
					backpressure = false
				}
			}
		}

		// TODO Update clientAffinityStats
	}
	return
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
	const PARAMETER_NONE = -1
	if !proc.SinglePartition {
		proc.PartitionParameter = PARAMETER_NONE
		proc.PartitionParameterType = PARAMETER_NONE
	}
}

/**
 * Handles subscrible update
 */
type SubscribeTopoRC struct {
	c *Conn
}

func (rc SubscribeTopoRC) ConsumeError(err error) {
	rc.c.handleSubscribeError(err)
}

func (rc SubscribeTopoRC) ConsumeResult(res driver.Result) {
}

func (rc SubscribeTopoRC) ConsumeRows(rows driver.Rows) {
}

func (c *Conn) handleSubscribeError(err error) {
	rsp := err.(VoltError)
	log.Printf("Subscribe received error, %#v", rsp)
	//Fast path subscribing retry if the connection was lost before getting a response
	if ResponseStatus(rsp.getStatus()) == CONNECTION_LOST {
		if c.ncLen > 0 {
			//TODO rate limit resent
			c.subscribeToNewNode()
		} else {
			return
		}
	}

	//Slow path, god knows why it didn't succeed, server could be paused and in admin mode. Don't firehose attempts.
	// if (response.getStatus() != ClientResponse.SUCCESS && !m_ex.isShutdown())

	// d.subscribeToNewNode()

	// TODO subscriptionRequestPending should be atomic
	c.subscriptionRequestPending = false
}

func (c *Conn) handleSubscribeRow(rows VoltRows) {
	//TODO go client current don't understand the binary_format hash config
	// need to fetch again
	c.getTopoStatistics()
}

/**
 * Handles procedure updates for client affinity
 */
type TopoStatisticsRC struct {
	c *Conn
}

func (rc TopoStatisticsRC) ConsumeError(err error) {
	log.Panic(err)
}

func (rc TopoStatisticsRC) ConsumeResult(res driver.Result) {
}

func (rc TopoStatisticsRC) ConsumeRows(rows driver.Rows) {
	rc.c.updateAffinityTopology(rows.(VoltRows))
}

func (c *Conn) updateAffinityTopology(rows VoltRows) (err error) {
	if !rows.isValidTable() {
		return errors.New("Not a validated topo statistic.")
	}

	if !rows.AdvanceTable() {
		// Just in case the new client connects to the old version of Volt that only returns 1 topology table
		return errors.New("Not support Legacy hashinator.")
	} else if !rows.AdvanceRow() { //Second table contains the hash function
		return errors.New("Topology description received from Volt was incomplete " +
			"performance will be lower because transactions can't be routed at this client")
	}
	hashType, hashTypeErr := rows.GetString(0)
	panicIfnotNil("Error get hashtype ", hashTypeErr)
	hashConfig, hashConfigErr := rows.GetVarbinary(1)
	panicIfnotNil("Error get hashConfig ", hashConfigErr)
	switch hashType.(string) {
	case ELASTIC:
		configFormat := JSON_FORMAT
		cooked := true // json format is by default cooked
		if c.h, err = newHashinatorElastic(configFormat, cooked, hashConfig.([]byte)); err != nil {
			return err
		}
	default:
		return errors.New("Not support Legacy hashinator.")
	}
	c.partitonMutex.Lock()
	c.partitionMasters = make(map[int]*nodeConn)
	c.partitionReplicas = make(map[int][]*nodeConn)

	//First table contains the description of partition ids master/slave relationships
	rows.AdvanceToTable(0)

	// The MPI's partition ID is 16383 (MpInitiator.MP_INIT_PID), so we shouldn't inadvertently
	// hash to it.  Go ahead and include it in the maps, we can use it at some point to
	// route MP transactions directly to the MPI node.

	// TODO GetXXXBYName seems broken
	for rows.AdvanceRow() {
		// partition, partitionErr := rows.GetBigIntByName("Partition")
		partition, partitionErr := rows.GetInteger(0)
		panicIfnotNil("Error get partition ", partitionErr)
		// sites, sitesErr := rows.GetStringByName("Sites")
		sites, sitesErr := rows.GetString(1)
		panicIfnotNil("Error get sites ", sitesErr)

		connections := make([]*nodeConn, 0)
		for _, site := range strings.Split(sites.(string), ",") {
			site = strings.TrimSpace(site)
			hostId, hostIdErr := strconv.Atoi(strings.Split(site, ":")[0])
			panicIfnotNil("Error get hostId", hostIdErr)
			if _, ok := c.hostIdToConnection[hostId]; ok {
				connections = append(connections, c.hostIdToConnection[hostId])
			}
		}
		c.partitionReplicas[int(partition.(int32))] = connections

		// leaderHost, leaderHostErr := rows.GetStringByName("Leader")
		leaderHost, leaderHostErr := rows.GetString(2)
		panicIfnotNil("Error get leaderHost", leaderHostErr)
		leaderHostId, leaderHostIdErr := strconv.Atoi(strings.Split(leaderHost.(string), ":")[0])
		panicIfnotNil("Error get leaderHostId", leaderHostIdErr)
		if _, ok := c.hostIdToConnection[leaderHostId]; ok {
			c.partitionMasters[int(partition.(int32))] = c.hostIdToConnection[leaderHostId]
		}
	}
	c.partitonMutex.Unlock()
	return nil
}

/**
 * Handles procedure updates for client affinity
 */
type ProcedureInfoRC struct {
	c *Conn
}

func (rc ProcedureInfoRC) ConsumeError(err error) {
	log.Panic(err)
}

func (rc ProcedureInfoRC) ConsumeResult(res driver.Result) {
}

func (rc ProcedureInfoRC) ConsumeRows(rows driver.Rows) {
	rc.c.updateProcedurePartitioning(rows.(VoltRows))
	rc.c.fetchedCatalog = false
}

func (c *Conn) updateProcedurePartitioning(rows VoltRows) error {
	c.procedureInfos = make(map[string]procedure)
	for rows.AdvanceRow() {
		// proc information embedded in JSON object in remarks column
		remarks, remarksErr := rows.GetVarbinary(6)
		panicIfnotNil("Error get Remarks column", remarksErr)
		procedureName, procedureNameErr := rows.GetString(2)
		panicIfnotNil("Error get procedureName column", procedureNameErr)
		proc := procedure{}
		procErr := json.Unmarshal(remarks.([]byte), &proc)
		// log.Println("remarks", string(remarks.([]byte)), "proc after unmarshal", proc)
		panicIfnotNil("Error parse remarks ", procErr)
		proc.setDefaults()
		c.procedureInfos[procedureName.(string)] = proc
	}
	return nil
}

// Subscribe to receive async updates on a new node connection.
func (c *Conn) subscribeToNewNode() {
	c.subscriptionRequestPending = true
	c.subscribedConnection = c.getConnByRand()

	//Subscribe to topology updates before retrieving the current topo
	//so there isn't potential for lost updates
	c.subscribeTopo()

	c.getTopoStatistics()

	c.getProcedureInfo()
}

func (c *Conn) getConnByRand() (cxn *nodeConn) {
	if c.ncLen > 0 {
		cxn = c.ncs[rand.Intn(c.ncLen)]
	}
	return
}

func (c *Conn) subscribeTopo() {
	if c.subscribedConnection == nil || !c.subscribedConnection.isOpen() {
		c.subscribeToNewNode()
		return
	}
	responseCh := make(chan voltResponse, 1)
	SubscribeTopoPi := newAsyncProcedureInvocation(c.getNextSystemHandle(), true, "@Subscribe", []driver.Value{"TOPOLOGY"}, responseCh, DEFAULT_QUERY_TIMEOUT, SubscribeTopoRC{c})
	c.subscribedConnection.submit(SubscribeTopoPi)
}

func (c *Conn) getTopoStatistics() {
	// TODO add sysHandle to procedureInvocation
	// system call procedure should bypass timeout and backpressure
	if c.subscribedConnection == nil || !c.subscribedConnection.isOpen() {
		c.subscribeToNewNode()
		return
	}
	responseCh := make(chan voltResponse, 1)
	topoStatisticsPi := newAsyncProcedureInvocation(c.getNextSystemHandle(), true, "@Statistics", []driver.Value{"TOPO", int32(JSON_FORMAT)}, responseCh, DEFAULT_QUERY_TIMEOUT, TopoStatisticsRC{c})
	c.subscribedConnection.submit(topoStatisticsPi)
}

func (c *Conn) getProcedureInfo() {
	if c.subscribedConnection == nil || !c.subscribedConnection.isOpen() {
		c.subscribeToNewNode()
		return
	}
	//Don't need to retrieve procedure updates every time we do a new subscription
	if c.fetchedCatalog {
		responseCh := make(chan voltResponse, 1)

		procedureInfoPi := newAsyncProcedureInvocation(c.getNextSystemHandle(), true, "@SystemCatalog", []driver.Value{"PROCEDURES"}, responseCh, DEFAULT_QUERY_TIMEOUT, ProcedureInfoRC{c})
		c.subscribedConnection.submit(procedureInfoPi)
	}
}

func panicIfnotNil(str string, err error) {
	if err != nil {
		log.Panic(str, err)
	}
}

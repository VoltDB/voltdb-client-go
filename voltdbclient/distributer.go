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
)

// the set of currently active connections
type distributer struct {
	// the set of active connections
	acs      []*nodeConn
	acsMutex sync.RWMutex
	// next conn to look at when finding by round robin.
	acsNextI int
	open     atomic.Value
	h        hashinater

	subscribedConnection       *nodeConn // The connection we have issued our subscriptions to.
	subscriptionRequestPending bool

	fetchedCatalog     bool
	ignoreBackpressure bool
	useClientAffinity  bool
	partitionMasters   map[int]*nodeConn
	partitionReplicas  map[int][]*nodeConn
	hostIdToConnection map[int]*nodeConn
	procedureInfos     map[string]procedure
}

func newDistributer() *distributer {
	var d = new(distributer)
	d.acs = make([]*nodeConn, 0)
	// d.acsMutex = sync.RWMutex{}
	d.acsNextI = 0
	d.open = atomic.Value{}
	d.open.Store(true)
	d.h = nil
	d.useClientAffinity = true
	d.fetchedCatalog = true
	d.ignoreBackpressure = true
	d.partitionMasters = make(map[int]*nodeConn)
	d.partitionReplicas = make(map[int][]*nodeConn)
	d.hostIdToConnection = make(map[int]*nodeConn)
	d.procedureInfos = make(map[string]procedure)
	return d
}

// add a conn that is newly connected
func (d *distributer) addConn(ac *nodeConn) {
	d.assertOpen()
	d.acsMutex.Lock()
	d.acs = append(d.acs, ac)
	d.acsMutex.Unlock()
}

// Begin starts a transaction.  VoltDB runs in auto commit mode, and so Begin
// returns an error.
func (d *distributer) Begin() (driver.Tx, error) {
	d.assertOpen()
	return nil, errors.New("VoltDB does not support transactions, VoltDB autocommits")
}

// Close closes the connection to the VoltDB server.  Connections to the server
// are meant to be long lived; it should not be necessary to continually close
// and reopen connections.  Close would typically be called using a defer.
// Operations using a closed connection cause a panic.
func (d *distributer) Close() (err error) {
	if d.isClosed() {
		return
	}
	d.setClosed()

	// once this is closed there shouldn't be any additional activity against
	// the connections.  They're touched here without getting a lock.
	for _, ac := range d.acs {
		err := ac.close()
		if err != nil {
			log.Printf("Failed to close connection with %v\n", err)
		}
	}
	d.acs = nil
	return nil
}

func (d *distributer) removeConn(ci string) {
	d.assertOpen()
	d.acsMutex.Lock()
	for i, ac := range d.acs {
		if ac.cs.connInfo == ci {
			d.acs = append(d.acs[:i], d.acs[i+1:]...)
			break
		}
	}
	d.acsMutex.Unlock()
}

// Drain blocks until all outstanding asynchronous requests have been satisfied.
// Asynchronous requests are processed in a background thread; this call blocks the
// current thread until that background thread has finished with all asynchronous requests.
func (d *distributer) Drain() {
	d.assertOpen()

	// drain can't work if the set of connections can change
	// while it's running.  Hold the lock for the duration, some
	// asyncs may time out.  The user thread is blocked on drain.
	d.acsMutex.Lock()
	for _, ac := range d.acs {
		ac.drain()
	}
	d.acsMutex.Unlock()
}

func (d *distributer) assertOpen() {
	if !(d.open.Load().(bool)) {
		panic("Tried to use closed connection pool")
	}
}

func (d *distributer) isClosed() bool {
	return !(d.open.Load().(bool))
}

func (d *distributer) setClosed() {
	d.open.Store(false)
}

func (d *distributer) numConns() int {
	d.assertOpen()
	d.acsMutex.RLock()
	num := len(d.acs)
	d.acsMutex.RUnlock()
	return num
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// Exec is available on both VoltConn and on VoltStatement.
func (d *distributer) Exec(query string, args []driver.Value) (driver.Result, error) {
	pi := newProcedureInvocation(query, args)
	ac := d.getConn(pi)
	return ac.exec(pi)
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
// ExecAsync is analogous to Exec but is run asynchronously.  That is, an
// invocation of this method blocks only until a request is sent to the VoltDB
// server.
func (d *distributer) ExecAsync(resCons AsyncResponseConsumer, query string, args []driver.Value) error {
	pi := newProcedureInvocation(query, args)
	ac := d.getConn(pi)
	return ac.execAsync(resCons, pi)
}

// Prepare creates a prepared statement for later queries or executions.
// The Statement returned by Prepare is bound to this VoltConn.
func (d *distributer) Prepare(query string) (driver.Stmt, error) {
	stmt := newVoltStatement(d, query)
	return *stmt, nil
}

// Query executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
func (d *distributer) Query(query string, args []driver.Value) (driver.Rows, error) {
	pi := newProcedureInvocation(query, args)
	ac := d.getConn(pi)
	return ac.query(pi)
}

// QueryAsync executes a query asynchronously.  The invoking thread will block
// until the query is sent over the network to the server.  The eventual
// response will be handled by the given AsyncResponseConsumer, this processing
// happens in the 'response' thread.
func (d *distributer) QueryAsync(rowsCons AsyncResponseConsumer, query string, args []driver.Value) error {
	pi := newProcedureInvocation(query, args)
	ac := d.getConn(pi)
	return ac.queryAsync(rowsCons, pi)
}

// Get a connection from the hashinator.  If not, get one by round robin.  If not return nil.
// TODO return nil cause Exec not working?
func (d *distributer) getConn(pi *procedureInvocation) *nodeConn {

	d.assertOpen()
	d.acsMutex.RLock()
	c, _, _ := d.getConnByCA(pi)
	if c == nil {
		c = d.getConnByRR()
	}
	d.acsMutex.RUnlock()
	return c
}

// Try to find optimal connection using client affinity
// return picked connection if found else nil
// also return backpressure
// this method is not thread safe
func (d *distributer) getConnByCA(pi *procedureInvocation) (cxn *nodeConn, backpressure bool, err error) {
	cxn = nil
	backpressure = true

	if len(d.acs) == 0 {
		return cxn, backpressure, errors.New("No connections.")
	}

	// Check if the master for the partition is known.
	if d.useClientAffinity && d.h != nil {
		var hashedPartition int = -1

		if procedureInfo, ok := d.procedureInfos[pi.query]; ok {
			hashedPartition = MP_INIT_PID
			// User may have passed too few parameters to allow dispatching.
			if procedureInfo.SinglePartition && procedureInfo.PartitionParameter < pi.getPassedParamCount() {
				if hashedPartition, err = d.h.getHashedPartitionForParameter(procedureInfo.PartitionParameterType,
					pi.getPartitionParamValue(procedureInfo.PartitionParameter)); err != nil {
					return
				}

			}

			// If the procedure is read only and single part, load balance across replicas
			if procedureInfo.SinglePartition && procedureInfo.ReadOnly {
				partitionReplica := d.partitionReplicas[hashedPartition]
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
					if !cxn.hasBP() || d.ignoreBackpressure {
						backpressure = false
					}
				}
			} else {
				// Writes have to go to the master
				cxn = d.partitionMasters[hashedPartition]
				if (cxn != nil && !cxn.hasBP()) || d.ignoreBackpressure {
					backpressure = false
				}
			}
		}
		// if connection closed, reset to nil and let the round robin pick a connection
		if cxn != nil && !cxn.isOpen() {
			cxn = nil
		}

		// TODO Update clientAffinityStats
	}
	return
}

func (d *distributer) getConnByRR() *nodeConn {
	currLen := len(d.acs)
	for i := 0; i < currLen; i++ {
		if d.acsNextI >= currLen {
			d.acsNextI = 0
		}
		c := d.acs[d.acsNextI]
		d.acsNextI++
		if !c.hasBP() && c.isOpen() {
			return c
		}
	}
	return nil
}

type procedureInvocation struct {
	query  string
	params []driver.Value
}

func newProcedureInvocation(query string, params []driver.Value) *procedureInvocation {
	var pi = new(procedureInvocation)
	pi.query = query
	pi.params = params
	return pi
}

func (pi procedureInvocation) getPassedParamCount() int {
	return len(pi.params)
}

func (pi procedureInvocation) getPartitionParamValue(index int) driver.Value {
	return pi.params[index]
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

func (d *distributer) handleSubscribe(rsp voltResponse) {
	switch rsp.(type) {
	case VoltError:
		log.Printf("Subscribe received error, %#v", rsp)
		//Fast path subscribing retry if the connection was lost before getting a response
		if ResponseStatus(rsp.getStatus()) == CONNECTION_LOST {
			if len(d.acs) > 0 {
				d.subscribeToNewNode()
			} else {
				return
			}
		}

		//Slow path, god knows why it didn't succeed, server could be paused and in admin mode. Don't firehose attempts.
		// if (response.getStatus() != ClientResponse.SUCCESS && !m_ex.isShutdown())
		//TODO rate limit resent
		// d.subscribeToNewNode()

		// TODO subscriptionRequestPending should be atomic
		d.subscriptionRequestPending = false
	case VoltRows:
		log.Printf("Subscribe received rows, %#v", rsp)
		// TODO go client current don't understand the binary_format hash config
		// need to fetch again
		go d.getTopoStatistics()

	default:
		log.Panic("Unrecongized response type, %#v", rsp)
	}
}

/**
 * Handles topology updates for client affinity
 */
func (d *distributer) updateAffinityTopology(rows VoltRows) (err error) {
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
		if d.h, err = newHashinaterElastic(configFormat, cooked, hashConfig.([]byte)); err != nil {
			return err
		}
	default:
		return errors.New("Not support Legacy hashinator.")
	}

	d.partitionMasters = make(map[int]*nodeConn)
	d.partitionReplicas = make(map[int][]*nodeConn)

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
			if _, ok := d.hostIdToConnection[hostId]; ok {
				connections = append(connections, d.hostIdToConnection[hostId])
			}
		}
		d.partitionReplicas[int(partition.(int32))] = connections

		// leaderHost, leaderHostErr := rows.GetStringByName("Leader")
		leaderHost, leaderHostErr := rows.GetString(2)
		panicIfnotNil("Error get leaderHost", leaderHostErr)
		leaderHostId, leaderHostIdErr := strconv.Atoi(strings.Split(leaderHost.(string), ":")[0])
		panicIfnotNil("Error get leaderHostId", leaderHostIdErr)
		if _, ok := d.hostIdToConnection[leaderHostId]; ok {
			d.partitionMasters[int(partition.(int32))] = d.hostIdToConnection[leaderHostId]
		}
	}
	return nil
}

/**
 * Handles procedure updates for client affinity
 */
func (d *distributer) updateProcedurePartitioning(rows VoltRows) error {
	d.procedureInfos = make(map[string]procedure)
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
		d.procedureInfos[procedureName.(string)] = proc
	}
	return nil
}

// Subscribe to receive async updates on a new node connection.
func (d *distributer) subscribeToNewNode() {
	d.subscriptionRequestPending = true
	d.subscribedConnection = d.getConnByRand()

	//Subscribe to topology updates before retrieving the current topo
	//so there isn't potential for lost updates
	go d.subscribeTopo()

	go d.getTopoStatistics()

	go d.getProcedureInfo()
}

func (d *distributer) getConnByRand() (cxn *nodeConn) {
	if len(d.acs) > 0 {
		cxn = d.acs[rand.Intn(len(d.acs))]
	}
	return
}

func (d *distributer) subscribeTopo() {
	if d.subscribedConnection == nil || !d.subscribedConnection.isOpen() {
		d.subscribeToNewNode()
		return
	}
	if rows, err := d.subscribedConnection.query(newProcedureInvocation("@Subscribe", []driver.Value{"TOPOLOGY"})); err != nil {
		d.handleSubscribe(err.(VoltError))
	} else {
		d.handleSubscribe(rows.(VoltRows))
	}
}

func (d *distributer) getTopoStatistics() {
	// TODO add sysHandle to procedureInvocation
	// system call procedure should bypass timeout and backpressure
	if d.subscribedConnection == nil || !d.subscribedConnection.isOpen() {
		d.subscribeToNewNode()
		return
	}
	if rows, err := d.subscribedConnection.query(newProcedureInvocation("@Statistics", []driver.Value{"TOPO", int32(JSON_FORMAT)})); err != nil {
		log.Panic(err)
	} else {
		d.updateAffinityTopology(rows.(VoltRows))
	}
}

func (d *distributer) getProcedureInfo() {
	if d.subscribedConnection == nil || !d.subscribedConnection.isOpen() {
		d.subscribeToNewNode()
		return
	}
	//Don't need to retrieve procedure updates every time we do a new subscription
	if d.fetchedCatalog {
		if rows, err := d.subscribedConnection.query(newProcedureInvocation("@SystemCatalog", []driver.Value{"PROCEDURES"})); err != nil {
			log.Panic(err)
		} else {
			d.updateProcedurePartitioning(rows.(VoltRows))
			d.fetchedCatalog = true
		}
	}
}

func panicIfnotNil(str string, err error) {
	if err != nil {
		log.Panic(str, err)
	}
}

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
	"encoding/json"
	"errors"
	"math/rand"
)

var errLegacyHashinator = errors.New("Not support Legacy hashinator.")

func (c *Conn) subscribeTopo(ctx context.Context, nc *nodeConn) <-chan voltResponse {
	responseCh := make(chan voltResponse, 1)
	subscribeTopoPi := newSyncProcedureInvocation(c.getNextSystemHandle(), true, "@Subscribe", []driver.Value{"TOPOLOGY"}, responseCh, DefaultQueryTimeout)
	nctx, cancel := context.WithTimeout(ctx, DefaultQueryTimeout)
	subscribeTopoPi.cancel = cancel
	nc.submit(nctx, subscribeTopoPi)
	return responseCh
}

type PartitionDetails struct {
	HN         hashinator
	Replicas   map[int][]*nodeConn
	Procedures map[string]procedure
	Masters    map[int]*nodeConn
}

func (c *Conn) GetPartitionDetails(nc *nodeConn) (*PartitionDetails, error) {
	details, err := c.MustGetTopoStatistics(c.ctx, nc)
	if err != nil {
		return nil, err
	}
	i, err := c.MustGetPTInfo(c.ctx, nc)
	if err != nil {
		return nil, err
	}
	details.Procedures = i
	return details, nil
}

func (c *Conn) MustGetTopoStatistics(ctx context.Context, nc *nodeConn) (*PartitionDetails, error) {
	responseCh := make(chan voltResponse, 1)
	pi := newSyncProcedureInvocation(c.getNextSystemHandle(), true, "@Statistics", []driver.Value{"TOPO", int32(JSONFormat)}, responseCh, DefaultQueryTimeout)
	nctx, cancel := context.WithTimeout(ctx, DefaultQueryTimeout)
	pi.cancel = cancel
	nc.submit(nctx, pi)
	v := <-responseCh
	if err, ok := v.(VoltError); ok {
		return nil, err
	}
	tmpHnator, tmpPartitionReplicas, err := c.updateAffinityTopology(v.(VoltRows))
	if err != nil {
		return nil, err
	}
	details := &PartitionDetails{
		HN:       tmpHnator,
		Replicas: *tmpPartitionReplicas,
	}
	return details, nil
}

func (c *Conn) MustGetPTInfo(ctx context.Context, nc *nodeConn) (map[string]procedure, error) {
	responseCh := make(chan voltResponse, 1)
	procedureInfoPi := newSyncProcedureInvocation(c.getNextSystemHandle(), true, "@SystemCatalog", []driver.Value{"PROCEDURES"}, responseCh, DefaultQueryTimeout)
	nctx, cancel := context.WithTimeout(ctx, DefaultQueryTimeout)
	procedureInfoPi.cancel = cancel
	nc.submit(nctx, procedureInfoPi)
	v := <-responseCh
	if err, ok := v.(VoltError); ok {
		return nil, err
	}
	i, err := c.updateProcedurePartitioning(v.(VoltRows))
	if err != nil {
		return nil, err
	}
	return *i, nil

}

func (c *Conn) updateAffinityTopology(rows VoltRows) (hashinator, *map[int][]*nodeConn, error) {
	if !rows.isValidTable() {
		return nil, nil, errors.New("Not a validated topo statistic.")
	}

	if !rows.AdvanceTable() {
		// Just in case the new client connects to the old version of Volt that only
		// returns 1 topology table
		return nil, nil, errLegacyHashinator
	} else if !rows.AdvanceRow() { //Second table contains the hash function
		return nil, nil, errors.New("Topology description received from Volt was incomplete " +
			"performance will be lower because transactions can't be routed at this client")
	}
	hashType, hashTypeErr := rows.GetString(0)
	panicIfnotNil("Error get hashtype ", hashTypeErr)
	hashConfig, hashConfigErr := rows.GetVarbinary(1)
	panicIfnotNil("Error get hashConfig ", hashConfigErr)
	var hnator hashinator
	var err error
	switch hashType.(string) {
	case Elastic:
		configFormat := JSONFormat
		cooked := true // json format is by default cooked
		if hnator, err = newHashinatorElastic(configFormat, cooked, hashConfig.([]byte)); err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, errors.New("Not support Legacy hashinator.")
	}
	partitionReplicas := make(map[int][]*nodeConn)

	// First table contains the description of partition ids master/slave
	// relationships
	rows.AdvanceToTable(0)

	// The MPI's partition ID is 16383 (MpInitiator.MPInitPID), so we shouldn't
	// inadvertently hash to it. Go ahead and include it in the maps, we can use
	// it at some point to route MP transactions directly to the MPI node.

	// TODO GetXXXBYName seems broken
	for rows.AdvanceRow() {
		// partition, partitionErr := rows.GetBigIntByName("Partition")
		partition, partitionErr := rows.GetInteger(0)
		panicIfnotNil("Error get partition ", partitionErr)
		// sites, sitesErr := rows.GetStringByName("Sites")
		_, sitesErr := rows.GetString(1) //sites, sitesErr := rows.GetString(1)
		panicIfnotNil("Error get sites ", sitesErr)

		var connections []*nodeConn
		//for _, site := range strings.Split(sites.(string), ",") {
		//site = strings.TrimSpace(site)
		////hostId, hostIdErr := strconv.Atoi(strings.Split(site, ":")[0])
		//panicIfnotNil("Error get hostId", hostIdErr)
		//if _, ok := c.hostIdToConnection[hostId]; ok {
		//	connections = append(connections, c.hostIdToConnection[hostId])
		//}
		//}
		partitionReplicas[int(partition.(int32))] = connections

		// leaderHost, leaderHostErr := rows.GetStringByName("Leader")
		//leaderHost, leaderHostErr := rows.GetString(2)
		//panicIfnotNil("Error get leaderHost", leaderHostErr)
		//leaderHostId, leaderHostIdErr := strconv.Atoi(strings.Split(leaderHost.(string), ":")[0])
		//panicIfnotNil("Error get leaderHostId", leaderHostIdErr)
		//if _, ok := c.hostIdToConnection[leaderHostId]; ok {
		//	c.partitionMasters[int(partition.(int32))] = c.hostIdToConnection[leaderHostId]
		//}
	}
	return hnator, &partitionReplicas, nil
}

func (c *Conn) updateProcedurePartitioning(rows VoltRows) (*map[string]procedure, error) {
	procedureInfos := make(map[string]procedure)
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
		procedureInfos[procedureName.(string)] = proc
	}
	return &procedureInfos, nil
}

// Try to find optimal connection using client affinity
// return picked connection if found else nil
// also return backpressure
// this method is not thread safe
func (c *Conn) getConnByCA(nodeConns []*nodeConn, hnator hashinator, partitionMasters *map[int]*nodeConn, partitionReplicas *map[int][]*nodeConn, procedureInfos *map[string]procedure, pi *procedureInvocation) (cxn *nodeConn, backpressure bool, err error) {
	backpressure = true

	// Check if the master for the partition is known.
	var hashedPartition = -1

	if procedureInfo, ok := (*procedureInfos)[pi.query]; ok {
		hashedPartition = MPInitPID
		// User may have passed too few parameters to allow dispatching.
		if procedureInfo.SinglePartition && procedureInfo.PartitionParameter < pi.getPassedParamCount() {
			if hashedPartition, err = hnator.getHashedPartitionForParameter(procedureInfo.PartitionParameterType,
				pi.getPartitionParamValue(procedureInfo.PartitionParameter)); err != nil {
				return
			}
		}

		// If the procedure is read only and single part, load balance across replicas
		// This is probably slower for SAFE consistency.
		if procedureInfo.SinglePartition && procedureInfo.ReadOnly && c.sendReadsToReplicasBytDefaultIfCAEnabled {
			partitionReplica := (*partitionReplicas)[hashedPartition]
			if len(partitionReplica) > 0 {
				cxn = partitionReplica[rand.Intn(len(partitionReplica))]
				if cxn.hasBP() {
					//See if there is one without backpressure, make sure it's still connected
					for _, nc := range partitionReplica {
						if !nc.hasBP() {
							cxn = nc
							break
						}
					}
				}
				if !cxn.hasBP() {
					backpressure = false
				}
			}
		} else {
			// Writes and Safe Reads have to go to the master
			cxn = (*partitionMasters)[hashedPartition]
			if cxn != nil && !cxn.hasBP() {
				backpressure = false
			}
		}
	}

	// TODO Update clientAffinityStats
	return
}

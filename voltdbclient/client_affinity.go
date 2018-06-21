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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
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

// PartitionDetails contains information about the cluster. Used to implement
// client affinity.
type PartitionDetails struct {
	HN         hashinator
	Replicas   map[int][]*nodeConn
	Procedures map[string]procedure
	Masters    map[int]*nodeConn
}

// GetPartitionDetails communicates with voltdb cluster and returns partition
// details.
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

// MustGetTopoStatistics returns *PartitionDetails with topology data about the
// voltdb cluster.
//
//This retrieves Replicas, and Master nodes.
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
	return c.updateAffinityTopology(v.(VoltRows))
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
	return c.updateProcedurePartitioning(v.(VoltRows))
}

func (c *Conn) updateAffinityTopology(rows VoltRows) (*PartitionDetails, error) {
	if !rows.isValidTable() {
		return nil, errors.New("Not a validated topo statistic.")
	}

	if !rows.AdvanceTable() {
		// Just in case the new client connects to the old version of Volt that only
		// returns 1 topology table
		return nil, errLegacyHashinator
	} else if !rows.AdvanceRow() { //Second table contains the hash function
		return nil, errors.New("Topology description received from Volt was incomplete " +
			"performance will be lower because transactions can't be routed at this client")
	}
	details := &PartitionDetails{
		Replicas: make(map[int][]*nodeConn),
		Masters:  make(map[int]*nodeConn),
	}
	hashType, err := rows.GetString(0)
	if err != nil {
		return nil, fmt.Errorf("error get hashtype :%v", err)
	}
	hashConfig, err := rows.GetVarbinary(1)
	if err != nil {
		return nil, fmt.Errorf("error get hashConfig :%v", err)
	}
	switch hashType.(string) {
	case Elastic:
		configFormat := JSONFormat
		cooked := true // json format is by default cooked
		hnator, err := newHashinatorElastic(configFormat, cooked, hashConfig.([]byte))
		if err != nil {
			return nil, err
		}
		details.HN = hnator
	default:
		return nil, errors.New("not support legacy hashinator")
	}

	// First table contains the description of partition ids master/slave
	// relationships
	rows.AdvanceToTable(0)

	// The MPI's partition ID is 16383 (MpInitiator.MPInitPID), so we shouldn't
	// inadvertently hash to it. Go ahead and include it in the maps, we can use
	// it at some point to route MP transactions directly to the MPI node.

	// TODO GetXXXBYName seems broken
	for rows.AdvanceRow() {
		partition, err := rows.GetInteger(0)
		if err != nil {
			return nil, fmt.Errorf("error get partition :%v ", err)
		}
		sites, err := rows.GetString(1)
		if err != nil {
			return nil, fmt.Errorf("error get site :%v ", err)
		}
		_, err = rows.GetString(1)
		if err != nil {
			return nil, fmt.Errorf("error get sites :%v ", err)
		}
		var connections []*nodeConn
		for _, site := range strings.Split(sites.(string), ",") {
			site = strings.TrimSpace(site)
			hostID, err := strconv.Atoi(strings.Split(site, ":")[0])
			if err != nil {
				return nil, fmt.Errorf("error get hostID :%v ", err)
			}
			if v, ok := c.hostIDToConnection[hostID]; ok {
				connections = append(connections, v)
			}
		}
		details.Replicas[int(partition.(int32))] = connections
		leaderHost, err := rows.GetString(2)
		if err != nil {
			return nil, fmt.Errorf("error get leaderHost :%v ", err)
		}
		leaderHostID, err := strconv.Atoi(strings.Split(leaderHost.(string), ":")[0])
		if err != nil {
			return nil, fmt.Errorf("error get leaderHostId :%v ", err)
		}
		if c.hostIDToConnection != nil {
			if nc, ok := c.hostIDToConnection[leaderHostID]; ok {
				details.Masters[int(partition.(int32))] = nc
			}
		}
	}
	return details, nil
}

func (c *Conn) updateProcedurePartitioning(rows VoltRows) (map[string]procedure, error) {
	procedureInfos := make(map[string]procedure)
	for rows.AdvanceRow() {
		// proc information embedded in JSON object in remarks column
		remarks, err := rows.GetVarbinary(6)
		if err != nil {
			return nil, fmt.Errorf("error get Remarks column :%v", err)
		}
		procedureName, err := rows.GetString(2)
		if err != nil {
			return nil, fmt.Errorf("error get procedureName column :%v", err)
		}
		proc := procedure{}
		err = json.Unmarshal(remarks.([]byte), &proc)
		// log.Println("remarks", string(remarks.([]byte)), "proc after unmarshal", proc)
		if err != nil {
			return nil, fmt.Errorf("error parse remarks :%v", err)
		}
		proc.setDefaults()
		procedureInfos[procedureName.(string)] = proc
	}
	return procedureInfos, nil
}

// Try to find optimal connection using client affinity
// return picked connection if found else nil
// also return backpressure
// this method is not thread safe
func (c *Conn) getConnByCA(details *PartitionDetails, query string, params []driver.Value) (cxn *nodeConn, err error) {
	// Check if the master for the partition is known.
	var hashedPartition = -1
	if info, ok := details.Procedures[query]; ok {
		hashedPartition = MPInitPID
		// User may have passed too few parameters to allow dispatching.
		if info.SinglePartition && info.PartitionParameter < len(params) {
			if hashedPartition, err = details.HN.getHashedPartitionForParameter(info.PartitionParameterType,
				params[info.PartitionParameter]); err != nil {
				return
			}
		}
		// If the procedure is read only and single part, load balance across replicas
		// This is probably slower for SAFE consistency.
		if info.SinglePartition && info.ReadOnly && c.sendReadsToReplicasBytDefaultIfCAEnabled {
			partitionReplica := details.Replicas[hashedPartition]
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
			}
		} else {
			if details.Masters != nil {
				// Writes and Safe Reads have to go to the master
				cxn = details.Masters[hashedPartition]
			}
		}
	}
	// TODO Update clientAffinityStats
	return
}

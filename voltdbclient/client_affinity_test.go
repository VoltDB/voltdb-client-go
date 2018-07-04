package voltdbclient

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"text/tabwriter"
)

func TestClientAffinity(t *testing.T) {
	servers := "localhost:21212,localhost:21222,localhost:21232"
	conn, err := OpenConn(servers)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.sendReadsToReplicasBytDefaultIfCAEnabled = true
	t.Run("should enable client affinity by default", func(ts *testing.T) {
		if !conn.useClientAffinity {
			ts.Error("expected useClientAffinity to be true")
		}
	})
	t.Run("must get topology stats", func(ts *testing.T) {
		nc := conn.getConn()
		_, err := conn.MustGetTopoStatistics(context.Background(), nc)
		if err != nil {
			ts.Fatal(err)
		}
	})
	t.Run("must get partition info ", func(ts *testing.T) {
		nc := conn.getConn()
		_, err := conn.MustGetPTInfo(context.Background(), nc)
		if err != nil {
			ts.Fatal(err)
		}
	})
	t.Run("must get pertition details ", func(ts *testing.T) {
		nc := conn.getConn()
		_, err := conn.GetPartitionDetails(nc)
		if err != nil {
			ts.Fatal(err)
		}
	})
	t.Run("must set partiion details", func(ts *testing.T) {
		r, err := conn.Query("@SystemCatalog", []driver.Value{"PROCEDURES"})
		if err != nil {
			ts.Fatal(err)
		}
		r.Close()
		if conn.PartitionDetails == nil {
			t.Error("expected partition details to be set")
		}
		b, err := conn.PartitionDetails.Dump()
		if err != nil {
			ts.Fatal(err)
		}
		err = ioutil.WriteFile("topology.json", b, 0600)
		if err != nil {
			ts.Error(err)
		}
	})
	t.Run("must pick the right master", func(ts *testing.T) {
		sample :=
			[]struct {
				query string
				args  []driver.Value
				hash  int
				conn  string
			}{
				{
					query: "Vote",
					args: []driver.Value{
						int64(4127351526),
						int32(5),
						int64(2),
					},
					hash: 0,
					conn: "localhost:21222",
				},
				{
					query: "Vote",
					args: []driver.Value{
						int64(2295722013),
						int32(2),
						int64(2),
					},
					hash: 4,
					conn: "localhost:21232",
				},
				{
					query: "Vote",
					args: []driver.Value{
						int64(5621008000),
						int32(5),
						int64(2),
					},
					hash: 0,
					conn: "localhost:21222",
				},
				{
					query: "Vote",
					args: []driver.Value{
						int64(2105510900),
						int32(5),
						int64(2),
					},
					hash: 4,
					conn: "localhost:21232",
				},
				{
					query: "Vote",
					args: []driver.Value{
						int64(2105510900),
						int32(5),
						int64(2),
					},
					hash: 4,
					conn: "localhost:21232",
				},
				{
					query: "Vote",
					args: []driver.Value{
						int64(7088159255),
						int32(1),
						int64(2),
					},
					hash: 2,
					conn: "localhost:21222",
				},
			}
		for _, v := range sample {
			c, err := conn.getConnByCA(conn.PartitionDetails, v.query, v.args)
			if err != nil {
				ts.Fatal(err)
			}
			if c != nil {
				if c.connInfo != v.conn {
					t.Errorf("expected %s got %s", v.conn, c.connInfo)
				}
			} else {
				t.Error("expected connection")
			}
		}
	})
}

// This executes queries in a 3 node cluster and verifies that the query
// distribution matches the topology details of the cluster.
//
// Only partion id and host id are considered.To setup for this test you need to
// prepare a 3 node cluster. And  load the following ddl
//
// -- CREATE TABLE Customer (
// --   CustomerID INTEGER UNIQUE NOT NULL,
// --   FirstName VARCHAR(15),
// --   LastName VARCHAR (15),
// --   PRIMARY KEY(CustomerID)
// --   );
//
// -- PARTITION TABLE Customer ON COLUMN CustomerID;
//
// -- CREATE PROCEDURE add_customer AS INSERT INTO Customer VALUES ?,?,?;
//
// -- PARTITION PROCEDURE add_customer ON TABLE Customer COLUMN CustomerID;
//
// You need to export AFFINITY_SERVERS environment variable with a comma
// separated list of the cluster node.
//	export AFFINITY_SERVERS="localhost:21212,localhost:21222,localhost:21232"
func TestVerifyClientAffinity(t *testing.T) {
	servers := os.Getenv("AFFINITY_SERVERS")
	if servers == "" {
		t.Skip("AFFINITY_SERVERS is not set")
	}
	conn, err := OpenConn(servers)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var nodes []procedureStat

	conn.selectedNode = func(connInfo string, hostID int, partitionID int, query string) {
		fmt.Printf("sending query %s to %s %d:%d\n",
			query, connInfo, hostID, partitionID)
		nodes = append(nodes, procedureStat{
			HostID:      hostID,
			PartitionID: partitionID,
			Procedure:   query,
		})
	}
	_, err = conn.Exec("@AdHoc", []driver.Value{"truncate table customer"})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 10; index++ {
		_, err := conn.Exec("add_customer", []driver.Value{
			int64(index), "john", "doe",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println(conn.PartitionDetails.HostMappingTable())
	s := conn.PartitionDetails.HostMapping()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
	fmt.Fprintln(w, "Table of partition id to host id mapping")
	fmt.Fprintln(w, "procedure \tpartition \thost_id")
	for _, v := range nodes {
		fmt.Fprintf(w, "%s \t%d:\t%d\n", v.Procedure, v.PartitionID, v.HostID)
	}
	w.Flush()
	for _, v := range nodes {
		f := fmt.Sprintf("%d:%d", v.PartitionID, v.HostID)
		if !strings.Contains(s, f) {
			t.Errorf("can't find mapping %s in %s", f, s)
		}
	}

	i, err := getInitiator(servers, 1)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(w, "Table of Initiator")
	fmt.Fprintln(w, "host_id\t procedure name\t invocations")
	hosts := ""
	for _, v := range i {
		hosts += "," + fmt.Sprint(v.HostID)
		fmt.Fprintf(w, "%d\t %s\t %d\n", v.HostID, v.Procedure, v.Invocations)
	}
	w.Flush()
	for _, v := range nodes {
		f := fmt.Sprint(v.HostID)
		if !strings.Contains(hosts, f) {
			t.Errorf("can't find host %s in %s", f, s)
		}
	}
}

func getProcedureStats(servers string, idx int64) ([]procedureStat, error) {
	conn, err := sql.Open("voltdb", servers)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	rows, err := conn.Query("@Statistics",
		"PROCEDURE", idx,
	)
	if err != nil {
		return nil, err
	}
	var o []procedureStat
	defer rows.Close()
	for rows.Next() {
		var p procedureStat
		err := rows.Scan(&p.TimeSstamp, &p.HostID, &p.HostName, &p.SiteID, &p.PartitionID,
			&p.Procedure, &p.Invocations, &p.TimedInvocations, &p.MinExTime, &p.MaxExTime,
			&p.AvgMaxExTime, &p.MinResultSize, &p.MaxResultSize,
			&p.AvgResultSize, &p.MinParamSetSize, &p.MaxParamSetSize, &p.AvgParamSetSize,
			&p.Aborts, &p.Failure, &p.Transactional,
		)
		if err != nil {
			return nil, err
		}
		if p.Procedure == "add_customer" {
			o = append(o, p)
		}
	}
	return o, nil
}

func getInitiator(servers string, idx int64) ([]procedureStat, error) {
	conn, err := sql.Open("voltdb", servers)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	rows, err := conn.Query("@Statistics",
		"INITIATOR", idx,
	)
	if err != nil {
		return nil, err
	}
	var o []procedureStat
	defer rows.Close()
	for rows.Next() {
		var p procedureStat
		var pass string
		var passInt int
		err := rows.Scan(&p.TimeSstamp, &p.HostID, &p.HostName, &p.SiteID, &pass, &pass,
			&p.Procedure, &p.Invocations, &passInt, &passInt, &passInt, &passInt, &passInt,
		)
		if err != nil {
			return nil, err
		}
		if p.Procedure == "add_customer" {
			o = append(o, p)
		}
	}
	return o, nil
}

type procedureStat struct {
	TimeSstamp       int64
	HostID           int
	HostName         string
	SiteID           int
	PartitionID      int
	Procedure        string
	Invocations      int
	TimedInvocations int
	MinExTime        int64
	MaxExTime        int64
	AvgMaxExTime     int64
	MinResultSize    int
	MaxResultSize    int
	AvgResultSize    int
	MinParamSetSize  int
	MaxParamSetSize  int
	AvgParamSetSize  int
	Aborts           int
	Failure          int
	Transactional    int
}

package voltdbclient

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

func TestVerifyClientAffinity(t *testing.T) {
	servers := "localhost:21212,localhost:21222,localhost:21232"
	conn, err := OpenConn(servers)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var nodes []procedureStat
	conn.selectedNode = func(nc *nodeConn, pi *procedureInvocation) {
		pid, ok := conn.PartitionDetails.GetMasterID(nc.connInfo)
		if !ok {
			t.Errorf("can't find master partition id for node %s", nc.connInfo)
			return
		}
		host := nc.connData.HostID
		fmt.Printf("sending query %s to %s <host_id>%d:%d<partition_id>\n",
			pi.query, nc.connInfo, host, pid)
		nodes = append(nodes, procedureStat{
			HostID:      int(host),
			PartitionID: pid,
			Procedure:   pi.query,
		})
	}
	_, err = conn.Exec("@AdHoc", []driver.Value{"delete from customer"})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 10; index++ {
		_, err = conn.Exec("add_customer", []driver.Value{
			int64(index), "john", "doe",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].HostID < nodes[j].HostID
	})
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

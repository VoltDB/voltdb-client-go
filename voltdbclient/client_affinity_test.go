package voltdbclient

import (
	"context"
	"database/sql/driver"
	"io/ioutil"
	"testing"
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

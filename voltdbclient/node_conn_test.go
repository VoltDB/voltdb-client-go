package voltdbclient

import (
	"strings"
	"testing"
)

func TestNodeConn_Close(t *testing.T) {
	conn := "localhost:21212"
	c := newNodeConn(conn)
	err := c.connect(1)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-c.close():
		if !v {
			t.Fatal("expected boolean true")
		}

		// To make sure the connection is closed we are sending a small chunk of
		// insignificant payload
		_, err := c.tcpConn.Write([]byte("hello"))
		if err == nil {
			t.Fatal("expected an error")
		}
		e := "use of closed network connection"
		if !strings.Contains(err.Error(), e) {
			t.Errorf("expected %s to be in %v", e, err)
		}
	}
}

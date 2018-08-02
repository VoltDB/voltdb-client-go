package voltdbclient

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestNodeConn_Close(t *testing.T) {
	conn := "localhost:21212"
	c := newNodeConn(conn)
	err := c.connect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Close()
	if err != nil {
		t.Fatal(err)
	}

	// To make sure the connection is closed we are sending a small chunk of
	// insignificant payload
	_, err = c.tcpConn.Write([]byte("hello"))
	if err == nil {
		t.Fatal("expected an error")
	}
	e := "use of closed network connection"
	if !strings.Contains(err.Error(), e) {
		t.Errorf("expected %s to be in %v", e, err)
	}
}
func TestNodeRetries(t *testing.T) {
	n := 10
	query := make(url.Values)
	query.Set("retry", "true")
	query.Set("retry_interval", time.Second.String())
	query.Set("max_retries", fmt.Sprint(n))
	conn := "localhost:21212?" + query.Encode()
	c := newNodeConn(conn)
	err := c.connect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		c.Close()
	}()
	if !c.retry {
		t.Error("expected retry to be true")
	}
	if c.retryInterval != time.Second {
		t.Errorf("expected %v got %v", time.Second, c.retryInterval)
	}
	if c.maxRetries != n {
		t.Errorf("expected %d got %d", n, c.maxRetries)
	}
}

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/VoltDB/voltdb-client-go/voltdbclient"
)

func TestMain(t *testing.T) {

	conn, err := voltdbclient.OpenTLSConn("127.0.0.1", voltdbclient.ClientConfig{"foo.pem", false})
	assert.NotNil(t, err)
	assert.Nil(t, conn)

	conn, err = voltdbclient.OpenTLSConn("127.0.0.1", voltdbclient.ClientConfig{"foo.pem", true})
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	vr, err := conn.Query("@Statistics", nil)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	rows, ok := vr.(*voltdbclient.VoltRows)
	assert.Equal(t, true, ok)
	assert.NotNil(t, rows)
}

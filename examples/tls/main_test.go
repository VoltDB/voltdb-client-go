package main

import (
	"testing"

	"database/sql/driver"

	"github.com/kr/pretty"
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

	var params []driver.Value

	for _, s := range []interface{}{"PAUSE_CHECK", int32(0)} {
		params = append(params, s)
	}

	vr, err := conn.Query("@Statistics", params)
	assert.Nil(t, err)
	assert.NotNil(t, vr)

	pretty.Print(vr)
}

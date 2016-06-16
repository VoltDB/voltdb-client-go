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
/*
Package voltdbclient is a VoltDB client driver for the go language.
The api is consistent with the client api implemented in other languages, notably java and c++.
The main user facing api in accessible from Client and includes:
NewClient(username string, password string)
CreateConnection(hostAndPort string)
Call(procedure string, params ...interface{})
CallAsync(procedure string, params ...interface{})
Close()
MultiplexCallbacks()
*/
package voltdbclient

import (
	"bytes"
	"fmt"
	"io"
	"net"
)

// Client is a single connection to a single node of a VoltDB database
type Client struct {
	config       *ClientConfig
	reader       io.Reader
	writer       io.Writer
	connData     *connectionData
	netListener  *NetworkListener
	clientHandle int64
}

type ClientConfig struct {
	username string
	password string
}

type NullValue struct {
	colType int8
}

type Callback struct {
	Channel <-chan *VoltRows
	Handle  int64
}

// NewClient creates an initialized, authenticated Client.
func NewClient(username string, password string) *Client {
	var client = new(Client)
	client.config = &ClientConfig{username, password}
	return client
}

func NewNullValue(colType int8) *NullValue {
	var nv = new(NullValue)
	nv.colType = colType
	return nv
}

func (client *Client) CreateConnection(hostAndPort string) error {
	raddr, err := net.ResolveTCPAddr("tcp", hostAndPort)
	if err != nil {
		return fmt.Errorf("Error resolving %v.", hostAndPort)
	}
	var tcpConn *net.TCPConn
	if tcpConn, err = net.DialTCP("tcp", nil, raddr); err != nil {
		return err
	}
	client.reader = tcpConn
	client.writer = tcpConn
	login, err := serializeLoginMessage(client.config.username, client.config.password)
	if err != nil {
		return err
	}
	writeLoginMessage(client.writer, &login)
	if client.connData, err = readLoginResponse(client.reader); err != nil {
		return err
	}
	client.clientHandle = 0
	client.netListener = NewListener(client.reader)
	client.netListener.start()
	return nil
}

// Close a client if open. A Client, once closed, has no further use.
// To open a new client, use NewClient.
func (client *Client) Close() error {
	var err error = nil
	if client.reader != nil {
		tcpConn := client.reader.(*net.TCPConn)
		tcpConn.Close()
	}
	client.reader = nil
	client.writer = nil
	client.connData = nil
	return err
}

// GoString provides a default printable format for Client.
func (client *Client) GoString() string {
	if client.connData != nil {
		return fmt.Sprintf("hostId:%v, connId:%v, leaderAddr:%v buildString:%v",
			client.connData.hostId, client.connData.connId,
			client.connData.leaderAddr, client.connData.buildString)
	}
	return "uninitialized"
}

// MultiplexCallbacks 'fans in' callbacks - listens for the given set of callbacks on one channel
func (client *Client) MultiplexCallbacks(callbacks []*Callback) <-chan *VoltRows {
	c := make(chan *VoltRows)
	for _, callback := range callbacks {
		ch := callback.Channel
		go func() {
			c <- <-ch
		}()
	}
	return c
}

func (client *Client) getNetworkListener() *NetworkListener {
	return client.netListener
}

func (client *Client) getReader() io.Reader {
	return client.reader
}

func (client *Client) getWriter() io.Writer {
	return client.writer
}

// functions private to this package.

// readLoginResponse parses the login response message.
func readLoginResponse(reader io.Reader) (*connectionData, error) {
	buf, err := readMessage(reader)
	if err != nil {
		return nil, err
	}
	connData, err := deserializeLoginResponse(buf)
	return connData, err
}

// writeLoginMessage writes a login message to the connection.
func writeLoginMessage(writer io.Writer, buf *bytes.Buffer) {
	// length includes protocol version.
	length := buf.Len() + 2
	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(length))
	writeProtoVersion(&netmsg)
	writePasswordHashVersion(&netmsg)
	// 1 copy + 1 n/w write benchmarks faster than 2 n/w writes.
	io.Copy(&netmsg, buf)
	io.Copy(writer, &netmsg)
}

// writeProcedureCall serializes a procedure call and writes it to a tcp connection.
func (client *Client) writeProcedureCall(writer io.Writer, procedure string, handle int64, params []interface{}) error {

	var call bytes.Buffer
	var err error

	// Serialize the procedure call and its params.
	// Use 0 for handle; it's not necessary in pure sync client.
	if call, err = serializeCall(procedure, handle, params); err != nil {
		return err
	}

	// todo: should prefer byte[] in all cases.
	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(call.Len()))
	io.Copy(&netmsg, &call)
	io.Copy(writer, &netmsg)
	return nil
}

func (nv *NullValue) ColType() int8 {
	return nv.colType
}

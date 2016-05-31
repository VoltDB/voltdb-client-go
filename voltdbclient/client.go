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
	"sync/atomic"
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

// connectionData are the values returned by a successful login.
type connectionData struct {
	hostId      int32
	connId      int64
	leaderAddr  int32
	buildString string
}

type ClientConfig struct {
	username string
	password string
}

type Callback struct {
	Channel <-chan *Response
	Handle  int64
}

// NewClient creates an initialized, authenticated Client.
func NewClient(username string, password string) *Client {
	var client = new(Client)
	client.config = &ClientConfig{username, password}
	return client
}

func NewCallback(channel <-chan *Response, handle int64) *Callback {
	var callback = new(Callback)
	callback.Channel = channel
	callback.Handle = handle
	return callback
}

// Call invokes the procedure 'procedure' with parameter values 'params'
// and returns a pointer to the received Response.
func (client *Client) Call(procedure string, params ...interface{}) (*Response, error) {
	if client.writer == nil {
		return nil, fmt.Errorf("Can not call procedure on closed Client.")
	}
	handle := atomic.AddInt64(&client.clientHandle, 1)
	cb := client.netListener.registerCallback(handle)
	if err := client.writeProcedureCall(procedure, handle, params); err != nil {
		client.netListener.removeCallback(handle)
		return nil, err
	}
	return <-cb.Channel, nil
}

// CallAsync asynchronously invokes the procedure 'procedure' with parameter values 'params'.
// A pointer to the Response from the server will be put on the returned channel.
func (client *Client) CallAsync(procedure string, params ...interface{}) (*Callback, error) {
	if client.writer == nil {
		return nil, fmt.Errorf("Can not call procedure on closed Client.")
	}
	handle := atomic.AddInt64(&client.clientHandle, 1)
	cb := client.netListener.registerCallback(handle)
	if err := client.writeProcedureCall(procedure, handle, params); err != nil {
		client.netListener.removeCallback(handle)
		return nil, err
	}
	return cb, nil
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
	client.writeLoginMessage(&login)
	if client.connData, err = client.readLoginResponse(); err != nil {
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
func (client *Client) MultiplexCallbacks(callbacks []*Callback) <-chan *Response {
	c := make(chan *Response)
	for _, callback := range callbacks {
		ch := callback.Channel
		go func() {
			c <- <-ch
		}()
	}
	return c
}

// Ping the database for liveness.
func (client *Client) PingConnection() bool {
	if client.writer == nil {
		return false
	}
	rsp, err := client.Call("@Ping")
	if err != nil {
		return false
	}
	return rsp.Status() == SUCCESS
}

func (client *Client) setWriter(writer io.Writer) {
	client.writer = writer
}

// functions private to this package.

// readLoginResponse parses the login response message.
func (client *Client) readLoginResponse() (*connectionData, error) {
	buf, err := readMessage(client.reader)
	if err != nil {
		return nil, err
	}
	connData, err := deserializeLoginResponse(buf)
	return connData, err
}

// writeLoginMessage writes a login message to the connection.
func (client *Client) writeLoginMessage(buf *bytes.Buffer) {
	// length includes protocol version.
	length := buf.Len() + 2
	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(length))
	writeProtoVersion(&netmsg)
	writePasswordHashVersion(&netmsg)
	// 1 copy + 1 n/w write benchmarks faster than 2 n/w writes.
	io.Copy(&netmsg, buf)
	io.Copy(client.writer, &netmsg)
}

// writeProcedureCall serializes a procedure call and writes it to a tcp connection.
func (client *Client) writeProcedureCall(procedure string, handle int64, params []interface{}) error {

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
	io.Copy(client.writer, &netmsg)
	return nil
}

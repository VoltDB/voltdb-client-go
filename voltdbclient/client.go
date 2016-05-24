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
package voltdbclient

import (
	"bytes"
	"fmt"
	"io"
	"net"
)

// Client is a single connection to a single node of a VoltDB database
type Client struct {
	tcpConn  *net.TCPConn
	connData *connectionData
}

// connectionData are the values returned by a successful login.
type connectionData struct {
	hostId      int32
	connId      int64
	leaderAddr  int32
	buildString string
}

// NewClient creates an initialized, authenticated Client.
func NewClient(user string, passwd string, hostAndPort string) (*Client, error) {
	var client = new(Client)
	var err error
	var raddr *net.TCPAddr
	var login bytes.Buffer

	if raddr, err = net.ResolveTCPAddr("tcp", hostAndPort); err != nil {
		return nil, fmt.Errorf("Error resolving %v.", hostAndPort)
	}
	if client.tcpConn, err = net.DialTCP("tcp", nil, raddr); err != nil {
		return nil, err
	}
	if login, err = serializeLoginMessage(user, passwd); err != nil {
		return nil, err
	}
	if err = client.writeLoginMessage(login); err != nil {
		return nil, err
	}
	if client.connData, err = client.readLoginResponse(); err != nil {
		return nil, err
	}
	return client, nil
}

// Call invokes the procedure 'procedure' with parameter values 'params'
// and returns a pointer to the received Response.
func (client *Client) Call(procedure string, params ...interface{}) (*Response, error) {
	var resp *bytes.Buffer
	var err error

	if client.tcpConn == nil {
		return nil, fmt.Errorf("Can not call procedure on closed Client.")
	}
	if err := client.writeProcedureCall(procedure, 0, params); err != nil {
		return nil, err
	}
	if resp, err = client.readMessage(); err != nil {
		return nil, err
	}
	return deserializeCallResponse(resp)
}

// Close a client if open. A Client, once closed, has no further use.
// To open a new client, use NewClient.
func (client *Client) Close() error {
	var err error = nil
	if client.tcpConn != nil {
		err = client.tcpConn.Close()
	}
	client.tcpConn = nil
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

// Ping the database for liveness.
func (client *Client) PingConnection() bool {
	if client.tcpConn == nil {
		return false
	}
	rsp, err := client.Call("@Ping")
	if err != nil {
		return false
	}
	return rsp.Status() == SUCCESS
}

// functions private to this package.

func (client *Client) readLoginResponse() (*connectionData, error) {
	buf, err := client.readMessage()
	if err != nil {
		return nil, err
	}
	connData, err := deserializeLoginResponse(buf)
	return connData, err
}

// readLoginResponse parses the login response message.
func (client *Client) readMessage() (*bytes.Buffer, error) {
	size, err := client.readMessageHdr()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err = io.ReadFull(client.tcpConn, data); err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)

	// Version Byte 1
	// TODO: error on incorrect version.
	if _, err = readByte(buf); err != nil {
		return nil, err
	}

	return buf, nil
}

// readMessageHdr reads the standard wireprotocol header.
func (client *Client) readMessageHdr() (size int32, err error) {
	// Total message length Integer  4
	size, err = readInt(client.tcpConn)
	if err != nil {
		return
	}
	return (size), nil
}

// writeLoginMessage writes a login message to the connection.
func (client *Client) writeLoginMessage(buf bytes.Buffer) error {
	// length includes protocol version.
	length := buf.Len() + 2
	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(length))
	writeProtoVersion(&netmsg)
	writePasswordHashVersion(&netmsg)
	// 1 copy + 1 n/w write benchmarks faster than 2 n/w writes.
	io.Copy(&netmsg, &buf)
	io.Copy(client.tcpConn, &netmsg)
	// TODO: obviously wrong
	return nil
}

// writeProcedureCall serializes a procedure call and writes it to the tcp connection.
func (client *Client) writeProcedureCall(procedure string, ud int64, params []interface{}) error {

	var call bytes.Buffer
	var err error

	// Serialize the procedure call and its params.
	// Use 0 for handle; it's not necessary in pure sync client.
	if call, err = serializeCall(procedure, 0, params); err != nil {
		return err
	}

	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(call.Len()))
	io.Copy(&netmsg, &call)
	io.Copy(client.tcpConn, &netmsg)
	// TODO: obviously wrong
	return nil
}

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
	"database/sql/driver"
	"fmt"
	"io"
	"time"
)

// NetworkListener listens for responses for asynchronous procedure calls from
// the server.  If a callback (channel) is registered for the procedure, the
// listener puts the response on the channel (calls back).
type NetworkListener struct {
	reader  io.Reader
	execs   map[int64]chan driver.Result
	queries map[int64]chan driver.Rows
}

func NewListener(reader io.Reader) *NetworkListener {
	var l = new(NetworkListener)
	l.reader = reader
	l.execs = make(map[int64]chan driver.Result)
	l.queries = make(map[int64]chan driver.Rows)
	return l
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (l *NetworkListener) listen() {
	for {
		rows, err := l.readOneMsg(l.reader)
		if err == nil {
			voltRows := rows.(VoltRows)
			handle := voltRows.clientHandle
			c, ok := l.queries[handle]
			if ok {
				l.removeQuery(handle)
				select {
				case c <- rows:
				case <-time.After(10 * time.Second):
					fmt.Printf("Client failed to read response from server on handle %v\n", handle)
				}
			} else {
				// todo: should log?
				fmt.Println("listener doesn't have callback for server response on client handle %v.", handle)
			}
		} else {
			// todo: should log?
			fmt.Println("error reading from server %v", err.Error())
		}
	}
}

// break this out to support testing
func (l *NetworkListener) readOneMsg(reader io.Reader) (driver.Rows, error) {
	return readResponse(reader)
}

func (l *NetworkListener) registerExec(handle int64) <-chan driver.Result {
	c := make(chan driver.Result)
	l.execs[handle] = c
	return c
}

func (l *NetworkListener) registerQuery(handle int64) <-chan driver.Rows {
	c := make(chan driver.Rows)
	l.queries[handle] = c
	return c
}

func (l *NetworkListener) removeExec(handle int64) {
	delete(l.execs, handle)
}

func (l *NetworkListener) removeQuery(handle int64) {
	delete(l.queries, handle)
}

func (l *NetworkListener) start() {
	go l.listen()
}

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
	"sync"
)

// NetworkListener listens for responses for asynchronous procedure calls from
// the server.  If a callback (channel) is registered for the procedure, the
// listener puts the response on the channel (calls back).
type NetworkListener struct {
	reader  io.Reader
	execs   map[int64]chan driver.Result
	execsMutex sync.Mutex
	queries map[int64]chan driver.Rows
	queriesMutex sync.Mutex
}

func NewListener(reader io.Reader) *NetworkListener {
	var l = new(NetworkListener)
	l.reader = reader
	l.execs = make(map[int64]chan driver.Result)
	l.execsMutex = sync.Mutex{}
	l.queries = make(map[int64]chan driver.Rows)
	l.queriesMutex = sync.Mutex{}
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

			l.queriesMutex.Lock()
			c, ok := l.queries[handle]
			if ok {
				l.removeQuery(handle)
			}
			l.queriesMutex.Unlock()

			if ok {
				c <- rows
			}
		} else {
			// TODO:  The error needs to go on the channel
			fmt.Println("error reading from server %v", err.Error())
		}
	}
}

// break this out to support testing
func (l *NetworkListener) readOneMsg(reader io.Reader) (driver.Rows, error) {
	return readResponse(reader)
}

func (l *NetworkListener) registerExec(handle int64) <-chan driver.Result {
	c := make(chan driver.Result, 1)
	l.execs[handle] = c
	return c
}

func (l *NetworkListener) registerQuery(handle int64) <-chan driver.Rows {
	c := make(chan driver.Rows, 1)
	l.queriesMutex.Lock()
	l.queries[handle] = c
	l.queriesMutex.Unlock()
	return c
}

// need to have a lock on the map when this is invoked.
func (l *NetworkListener) removeExec(handle int64) {
	delete(l.execs, handle)
}

// need to have a lock on the map when this is invoked.
func (l *NetworkListener) removeQuery(handle int64) {
	delete(l.queries, handle)
}

func (l *NetworkListener) start() {
	go l.listen()
}

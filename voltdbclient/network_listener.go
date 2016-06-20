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
	"sync/atomic"
)

// NetworkListener listens for responses for asynchronous procedure calls from
// the server.  If a callback (channel) is registered for the procedure, the
// listener puts the response on the channel (calls back).
type NetworkListener struct {
	reader         io.Reader
	execs          map[int64]chan driver.Result
	execsMutex     sync.Mutex
	queries        map[int64]chan driver.Rows
	queriesMutex   sync.Mutex
	wg             sync.WaitGroup
	hasBeenStopped int32
}

func NewListener(reader io.Reader, wg sync.WaitGroup) *NetworkListener {
	var l = new(NetworkListener)
	l.reader = reader
	l.execs = make(map[int64]chan driver.Result)
	l.execsMutex = sync.Mutex{}
	l.queries = make(map[int64]chan driver.Rows)
	l.queriesMutex = sync.Mutex{}
	l.wg = wg
	l.hasBeenStopped = 0
	return l
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (l *NetworkListener) listen() {
	for {
		if !atomic.CompareAndSwapInt32(&l.hasBeenStopped, 0, 0) {
			l.wg.Done()
			break
		}
		l.readResponse(l.reader)
	}
}

// read the client handle
func (l *NetworkListener) readHandle(r io.Reader) (handle int64, err error) {
	handle, err = readLong(r)
	if err != nil {
		return 0, err
	}
	return handle, nil
}

// reads and deserializes a response from the server.
func (l *NetworkListener) readResponse(r io.Reader) {
	buf, err := readMessage(r)
	if err != nil {
		fmt.Println("Failed to read response from server")
		return
	}

	handle, err := l.readHandle(buf)
	if err != nil {
		fmt.Println("Failed to read handle on response from server")
		return
	}
	var isQuery bool
	var isExec bool = false
	var queryChan chan driver.Rows
	var execChan chan driver.Result

	l.queriesMutex.Lock()
	queryChan, isQuery = l.queries[handle]
	if isQuery {
		l.removeQuery(handle)
	}
	l.queriesMutex.Unlock()

	if !isQuery {
		l.execsMutex.Lock()
		execChan, isExec = l.execs[handle]
		if isExec {
			l.removeExec(handle)
		}
		l.execsMutex.Unlock()
	}

	if isQuery {
		rows, err := deserializeRows(buf, handle)
		if err != nil {
			// TODO: put the error on the response
		}
		queryChan <- rows
	} else if isExec {
		result, err := deserializeResult(buf, handle)
		if err != nil {
			// TODO: put the error on the response
		}
		execChan <- result
	} else {
		fmt.Printf("Unexpected response from server, not Query or Exec\n")
	}
}

func (l *NetworkListener) registerExec(handle int64) <-chan driver.Result {
	c := make(chan driver.Result, 1)
	l.execsMutex.Lock()
	l.execs[handle] = c
	l.execsMutex.Unlock()
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
	l.wg.Add(1)
	go l.listen()
}

func (l *NetworkListener) stop() {
	for {
		if atomic.CompareAndSwapInt32(&l.hasBeenStopped, 0, 1) {
			break
		}
	}
}

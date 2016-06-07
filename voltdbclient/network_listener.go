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
	"fmt"
	"io"
	"time"
)

// NetworkListener listens for responses for asynchronous procedure calls from
// the server.  If a callback (channel) is registered for the procedure, the
// listener puts the response on the channel (calls back).
type NetworkListener struct {
	reader    io.Reader
	callbacks map[int64]chan *Response
}

func NewListener(reader io.Reader) *NetworkListener {
	var l = new(NetworkListener)
	l.reader = reader
	l.callbacks = make(map[int64]chan *Response)
	return l
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (l *NetworkListener) listen() {
	for {
		resp, err := l.readOneMsg(l.reader)
		if err == nil {
			handle := resp.clientHandle
			c, ok := l.callbacks[handle]
			if ok {
				l.removeCallback(handle)
				select {
				case c <- resp:
				case <-time.After(10 * time.Millisecond):
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
func (l *NetworkListener) readOneMsg(reader io.Reader) (*Response, error) {
	return readResponse(reader)
}

func (l *NetworkListener) registerCallback(handle int64) *Callback {
	c := make(chan *Response)
	l.callbacks[handle] = c
	return NewCallback(c, handle)
}

func (l *NetworkListener) removeCallback(handle int64) {
	delete(l.callbacks, handle)
}

func (l *NetworkListener) start() {
	go l.listen()
}

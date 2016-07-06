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
	"io"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type networkWriter struct {
	writer         io.Writer
	piCh           <-chan *procedureInvocation
	wg             *sync.WaitGroup
	hasBeenStopped int32
	bp             bool
	bpMutex        sync.RWMutex
}

func newNetworkWriter(writer io.Writer, ch <-chan *procedureInvocation, wg *sync.WaitGroup) *networkWriter {
	var nr = new(networkWriter)
	nr.writer = writer
	nr.piCh = ch
	nr.wg = wg
	nr.hasBeenStopped = 0
	nr.bp = false
	nr.bpMutex = sync.RWMutex{}
	return nr
}

func (nr *networkWriter) writePIs() {

	for {
		if !atomic.CompareAndSwapInt32(&nr.hasBeenStopped, 0, 0) {
			nr.wg.Done()
			break
		}

		select {
		case pi := <-nr.piCh:
			nr.serializePI(pi)
			// reading the length of a channel is thread safe, though racy
			qw := len(nr.piCh)
			if qw > piWriteChannelSize {
				nr.bp = true
			} else {
				nr.bp = false
			}
		case <-time.After(time.Millisecond * 100):
			continue // give the thread a chance to exit - it's okay to block for a bit, applicable on close
		}
	}
}

func (nr *networkWriter) serializePI(pi *procedureInvocation) {
	var call bytes.Buffer
	var err error

	// Serialize the procedure call and its params.
	if call, err = nr.serializeStatement(pi); err != nil {
		log.Printf("Error serializing procedure call %v\n", err)
	} else {
		var netmsg bytes.Buffer
		writeInt(&netmsg, int32(call.Len()))
		io.Copy(&netmsg, &call)
		io.Copy(nr.writer, &netmsg)
	}
}

func (nr *networkWriter) serializeStatement(pi *procedureInvocation) (msg bytes.Buffer, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()

	// batch timeout type
	if err = writeByte(&msg, 0); err != nil {
		return
	}
	if err = writeString(&msg, pi.query); err != nil {
		return
	}
	if err = writeLong(&msg, pi.handle); err != nil {
		return
	}
	serializedArgs, err := serializeArgs(pi.params)
	if err != nil {
		return
	}
	io.Copy(&msg, &serializedArgs)
	return
}

func (nr *networkWriter) start() {
	nr.wg.Add(1)
	go nr.writePIs()
}

func (nr *networkWriter) stop() {
	for {
		if atomic.CompareAndSwapInt32(&nr.hasBeenStopped, 0, 1) {
			break
		}
	}
}

func (nr *networkWriter) hasBP() bool {
	nr.bpMutex.RLock()
	bp := nr.bp
	nr.bpMutex.RUnlock()
	return bp
}

func (nr *networkWriter) setBP() {
	nr.bpMutex.Lock()
	nr.bp = true
	nr.bpMutex.Unlock()
}

func (nr *networkWriter) unsetBP() {
	nr.bpMutex.Lock()
	nr.bp = false
	nr.bpMutex.Unlock()
}

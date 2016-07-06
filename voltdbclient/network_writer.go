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
	"database/sql/driver"
	"io"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type networkWriter struct {
	nc             *nodeConn
	writer         io.Writer
	piCh           <-chan *procedureInvocation
	wg             *sync.WaitGroup
	hasBeenStopped int32
	bp             bool
	bpMutex        sync.RWMutex
}

func newNetworkWriter(nc *nodeConn, writer io.Writer, ch <-chan *procedureInvocation, wg *sync.WaitGroup) *networkWriter {
	var nw = new(networkWriter)
	nw.nc = nc
	nw.writer = writer
	nw.piCh = ch
	nw.wg = wg
	nw.hasBeenStopped = 0
	nw.bp = false
	nw.bpMutex = sync.RWMutex{}
	return nw
}

func (nw *networkWriter) writePIs() {

	for {
		if !atomic.CompareAndSwapInt32(&nw.hasBeenStopped, 0, 0) {
			nw.wg.Done()
			break
		}

		select {
		case pi := <-nw.piCh:
			nw.serializePI(pi)
			nw.nc.decrementQueuedBytes(pi.getLen())
		case <-time.After(time.Millisecond * 100):
			continue // give the thread a chance to exit - it's okay to block for a bit, applicable on close
		}
	}
}

func (nw *networkWriter) serializePI(pi *procedureInvocation) {
	var call bytes.Buffer
	var err error

	writeInt(&call, int32(pi.getLen()))

	// Serialize the procedure call and its params.
	if err = nw.serializeStatement(&call, pi); err != nil {
		log.Printf("Error serializing procedure call %v\n", err)
	} else {
		io.Copy(nw.writer, &call)
	}
}

func (nw *networkWriter) serializeStatement(writer io.Writer, pi *procedureInvocation) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()

	// batch timeout type
	if err = writeByte(writer, 0); err != nil {
		return
	}
	if err = writeString(writer, pi.query); err != nil {
		return
	}
	if err = writeLong(writer, pi.handle); err != nil {
		return
	}
	err = nw.serializeArgs(writer, pi.params)
	if err != nil {
		return
	}
	return
}

func (nw *networkWriter) serializeArgs(writer io.Writer, args []driver.Value) (err error) {
	// parameter_count short
	// (type byte, parameter)*
	if err = writeShort(writer, int16(len(args))); err != nil {
		return
	}
	for _, arg := range args {
		if err = marshallParam(writer, arg); err != nil {
			return
		}
	}
	return
}

func (nw *networkWriter) start() {
	nw.wg.Add(1)
	go nw.writePIs()
}

func (nw *networkWriter) stop() {
	for {
		if atomic.CompareAndSwapInt32(&nw.hasBeenStopped, 0, 1) {
			break
		}
	}
}

func (nw *networkWriter) hasBP() bool {
	nw.bpMutex.RLock()
	bp := nw.bp
	nw.bpMutex.RUnlock()
	return bp
}

func (nw *networkWriter) setBP() {
	nw.bpMutex.Lock()
	nw.bp = true
	nw.bpMutex.Unlock()
}

func (nw *networkWriter) unsetBP() {
	nw.bpMutex.Lock()
	nw.bp = false
	nw.bpMutex.Unlock()
}

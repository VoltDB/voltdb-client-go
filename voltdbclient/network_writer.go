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
	"time"
)

type networkWriter struct {
	writer  io.Writer
	piCh    <-chan *procedureInvocation
	closeCh *chan bool
	wg      *sync.WaitGroup
	bp      bool
	bpMutex sync.RWMutex
}

func newNetworkWriter(writer io.Writer, ch <-chan *procedureInvocation, closeCh *chan bool, wg *sync.WaitGroup) *networkWriter {
	var nw = new(networkWriter)
	nw.writer = writer
	nw.piCh = ch
	nw.closeCh = closeCh
	nw.wg = wg
	nw.bp = false
	return nw
}

// the state on the writer that gets reset when the connection is lost and then re-established.
func (nw *networkWriter) onReconnect(writer io.Writer, closeCh *chan bool) {
	nw.writer = writer
	nw.closeCh = closeCh
	nw.bp = false
}

func (nw *networkWriter) writePIs() {

	for {
		select {
		case pi := <-nw.piCh:
			nw.serializePI(pi)
		case <-time.After(time.Microsecond * 10):
			if nw.readClose() {
				nw.wg.Done()
				return
			}
		}
	}
}

func (nw *networkWriter) readClose() bool {
	select {
	case <-*nw.closeCh:
		return true
	case <-time.After(5 * time.Millisecond):
		return false
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
	go nw.writePIs()
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

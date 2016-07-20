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
)

type networkWriter struct {
	bp      bool
	bpMutex sync.RWMutex
}

func newNetworkWriter() *networkWriter {
	var nw = new(networkWriter)
	nw.bp = false
	return nw
}

func (nw *networkWriter) writePIs(writer io.Writer, piCh <-chan *procedureInvocation, wg *sync.WaitGroup) {
	for pi := range piCh {
		nw.serializePI(writer, pi)
	}
	wg.Done()
}

func (nw *networkWriter) serializePI(writer io.Writer, pi *procedureInvocation) {
	var call bytes.Buffer
	var err error

	writeInt(&call, int32(pi.getLen()))

	// Serialize the procedure call and its params.
	if err = nw.serializeStatement(&call, pi); err != nil {
		log.Printf("Error serializing procedure call %v\n", err)
	} else {
		io.Copy(writer, &call)
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

func (nw *networkWriter) connect(writer io.Writer, piCh <-chan *procedureInvocation, wg *sync.WaitGroup) {
	go nw.writePIs(writer, piCh, wg)
}

func (nw *networkWriter) disconnect(piCh chan *procedureInvocation) {
	close(piCh)

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

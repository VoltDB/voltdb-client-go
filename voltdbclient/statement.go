package voltdbclient

import (
	"database/sql/driver"
	"fmt"
	"sync/atomic"
	"io"
	"bytes"
)

var handle int64 = 0

type VoltStatement struct {
	closed bool
	numInput int
	stmt string

	netListener *NetworkListener
	writer *io.Writer
}

func NewVoltStatement(writer *io.Writer, netListener *NetworkListener, stmt string) *VoltStatement {
	var vs = new(VoltStatement)
	vs.writer = writer
	vs.netListener = netListener

	vs.stmt = stmt
	vs.closed = false
	vs.numInput = 0
	return vs
}

func (vs *VoltStatement) Close() error {
	vs.closed = true
	return nil
}

func (vs *VoltStatement) NumInput() int {
	return 0
}

func (vs *VoltStatement) Exec(args []driver.Value) (driver.Result, error) {
	return NewVoltResult(), nil
}

func (vs *VoltStatement) Query(args []driver.Value) (driver.Rows, error) {
	// TODO: need to lock client for this stuff...

	if vs.writer == nil {
		return VoltRows{}, fmt.Errorf("Can not execute statement on closed Client.")
	}
	stHandle := atomic.AddInt64(&handle, 1)
	cb := vs.netListener.registerCallback(handle)
	if err := vs.serializeStatement(vs.writer, vs.stmt, stHandle, args); err != nil {
		vs.netListener.removeCallback(handle)
		return VoltRows{}, err
	}
	rows := <-cb.Channel
	return *rows, nil
}

// I have two methods with this name, one top scope and one here.
// I want to make this one top scope so that the async methods can call it.
// TODO: the asyncs should implement an interface so we can multiplex over them.
func (vs *VoltStatement) serializeStatement(writer *io.Writer, procedure string, handle int64, args []driver.Value) error {

	var call bytes.Buffer
	var err error

	// Serialize the procedure call and its params.
	// Use 0 for handle; it's not necessary in pure sync client.
	if call, err = serializeStatement(procedure, handle, args); err != nil {
		return err
	}

	// todo: should prefer byte[] in all cases.
	var netmsg bytes.Buffer
	writeInt(&netmsg, int32(call.Len()))
	io.Copy(&netmsg, &call)
	io.Copy(*writer, &netmsg)
	return nil
}


package voltdbclient

import (
	"fmt"
	"net"
	"time"
)

// NetworkListener listens for responses for asynchronous procedure calls from
// the server.  If a callback (channel) is registered for the procedure, the
// listener puts the response on the channel (calls back).
type NetworkListener struct {
	tcpConn   *net.TCPConn
	callbacks map[int64]chan *Response
}

func NewListener(tcpConn *net.TCPConn) *NetworkListener {
	var l = new(NetworkListener)
	l.tcpConn = tcpConn
	l.callbacks = make(map[int64]chan *Response)
	return l
}

// listen listens for messages from the server and calls back a registered listener.
// listen blocks on input from the server and should be run as a go routine.
func (l *NetworkListener) listen() {
	for {
		resp, err := readResponse(l.tcpConn)
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

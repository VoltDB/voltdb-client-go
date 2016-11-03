[![GoDoc](https://godoc.org/github.com/VoltDB/voltdb-client-go/voltdbclient?status.svg)](https://godoc.org/github.com/VoltDB/voltdb-client-go/voltdbclient) 

VoltDB Golang Client Library
=========================

The VoltDB golang client library implements the native VoltDB wire protocol. You can
use the library to connect to a VoltDB cluster, invoke stored procedures and
read responses.

Client API
=========================

The VoltDB golang client implements the interfaces specified in the database/sql/driver
package.  The interfaces specified by database/sql/driver are typically used to implement
a driver to be used with the golang database/sql package.

The code here can be used as a driver in this manner; it can also be used as a stand
alone client.

The VoltDB golang client supports a VoltDB specific asynchronous api in addition to the
database/sql/driver api.  The VoltDB api also includes column accessors for VoltDB
specific types.  The details of the VoltDB api can be found in the godoc.

Building
=========================

The VoltDB golang client is built in the standard go manner.

Examples
=========================
The VoltDB golang client includes three examples.  There is a simple hello world example
and a similar example that uses the asynchronous api.  There is also an example that
uses the client with the database/sql/driver api only.

Wire Protocol
=========================
By default, version 1 of the VoltDB wire protocol is used; this is suitable for
VoltDB server version 5.2 or later.  Set the wire protocol version to 0 for use
with an older server.  See the 'driver_hello_world' example.

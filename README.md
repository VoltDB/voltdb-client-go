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

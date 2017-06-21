[![GoDoc](https://godoc.org/github.com/VoltDB/voltdb-client-go/voltdbclient?status.svg)](https://godoc.org/github.com/VoltDB/voltdb-client-go/voltdbclient) 

# VoltDB Golang Client Library

The VoltDB golang client library implements the native VoltDB wire protocol. You can
use the library to connect to a VoltDB cluster, invoke stored procedures and
read responses.

# Installation

    go get -v -u github.com/VoltDB/voltdb-client-go/voltdbclient

# Usage

As `database/sql` driver

```go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/VoltDB/voltdb-client-go/voltdbclient"
)

func main() {
	// If using a version of VoltDB server prior to 5.2, then
	// set the version of the wire protocol to 0.  The default
	// value 1, indicates a server version of 5.2 or later.
	// voltdbclient.ProtocolVersion = 0

	db, err := sql.Open("voltdb", "localhost:21212")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	rows, err := db.Query("HELLOWORLD.select", "French")
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// with prepared statement
	stmt, err := db.Prepare("select * from HELLOWORLD where dialect = ?")
	if err != nil {
		log.Fatal(err)
	}

	rows, err = stmt.Query("French")
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

}

func printRows(rows *sql.Rows) {
	for rows.Next() {
		var hello string
		var world string
		var dialect string
		err := rows.Scan(&hello, &world, &dialect)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Printf("SUCCESS: %s %s %s\n", hello, world, dialect)
	}
}
```

# Client API

The VoltDB golang client implements the interfaces specified in the database/sql/driver
package.  The interfaces specified by database/sql/driver are typically used to implement
a driver to be used with the golang database/sql package.

The code here can be used as a driver in this manner; it can also be used as a stand
alone client.

The VoltDB golang client supports a VoltDB specific asynchronous api in addition to the
database/sql/driver api.  The VoltDB api also includes column accessors for VoltDB
specific types.  The details of the VoltDB api can be found in the godoc.


# Examples
The VoltDB golang client includes three examples.  There is a simple hello world example
and a similar example that uses the asynchronous api.  There is also an example that
uses the client with the database/sql/driver api only.

# Wire Protocol

By default, version 1 of the VoltDB wire protocol is used; this is suitable for
VoltDB server version 5.2 or later.  Set the wire protocol version to 0 for use
with an older server.  See the 'driver_hello_world' example.


## FAQ

### How do I manage connection pools?

When using `database/sql` driver, connection pools are automatically managed for
you. However there is a couple of options offered for custom control such as
[SetConnMaxLifetime](https://golang.org/pkg/database/sql/#DB.SetConnMaxLifetime),
[SetMaxIdleConns](https://golang.org/pkg/database/sql/#DB.SetMaxIdleConns) and
[SetMaxOpenConns](https://golang.org/pkg/database/sql/#DB.SetMaxOpenConns)
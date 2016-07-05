The Hello World, Voter and VoltKV examples included here are intended to be used with example projects shipped with the main distribution. You will need to have a server running with the appropriate catalog or ddl loaded for these examples to work.

Hello World
===========

First, follow the instructions in the VoltDB kit's doc/tutorials/helloworld folder to start the database and load the schema.

Then, go to hello-world folder, run one the following commands to start the go helloworld client.  
This requires no arguments and connects to localhost.

```
go run async_hello_world.go (for using low-level async call API)
go run sync_hello_world.go  (for using low-level sync call API)
go run driver_hello_world.go (for using high-level go sql API)
```

Voter
=====

First, follow the instructions in the VoltDB kit's examples/voter folder to start the database and load the schema.

Then, go to voter folder, run following command to build go voter client.

`go build github.com/VoltDB/voltdb-client-go/examples/voter`

The voter client has following arguments:

```
  $./voter -h
  Usage of ./voter:
  -contestants int
    	Number of contestants in the voting contest (from 1 to 10). (default 6)
  -displayinterval duration
    	Interval for performance feedback, in seconds. (default 5s)
  -duration duration
    	Benchmark duration, in seconds. (default 2m0s)
  -goroutines int
    	Number of concurrent goroutines synchronously calling procedures. (default 40)
  -maxvotes int
    	Maximum number of votes cast per voter. (default 2)
  -runtype value
    	Type of the client calling procedures.
  -servers string
    	Comma separated list of the form server[:port] to connect to. (default "localhost:21212")
  -statsfile string
    	Filename to write raw summary statistics to.
  -warmup duration
    	Benchmark duration, in seconds. (default 5s)
```

A reasonable default invocation that connects to localhost using sql API is:

```
./voter --runtype=sql
```

VoltKV
======

First, follow the instructions in the VoltDB kit's examples/voltkv folder to start the database and load the schema.

Then, go to voltkv folder, run following command to build go voltkv client.

`go build github.com/VoltDB/voltdb-client-go/examples/voltkv`

The voltkv client has following arguments:

```
Usage of ./voltkv:
-displayinterval duration
    Interval for performance feedback, in seconds. (default 5s)
-duration duration
    Benchmark duration, in seconds. (default 2m0s)
-entropy int
    Number of values considered for each value byte. (default 127)
-getputratio float
    Fraction of ops that are gets (vs puts). (default 0.9)
-goroutines int
    Number of concurrent goroutines synchronously calling procedures. (default 40)
-keysize int
    Size of keys in bytes. (default 32)
-maxvaluesize int
    Maximum value size in bytes. (default 1024)
-minvaluesize int
    Minimum value size in bytes. (default 1024)
-poolsize int
    Number of keys to preload. (default 100000)
-preload
    Whether to preload a specified number of keys and values. (default true)
-runtype value
    Type of the client calling procedures.
-servers string
    Comma separated list of the form server[:port] to connect to. (default "localhost:21212")
-usecompression
    Compress values on the client side.
-warmup duration
    Benchmark duration, in seconds. (default 5s)

```

A reasonable default invocation that connects to localhost using sql API is:

```
./voltkv --runtype=sql  
```

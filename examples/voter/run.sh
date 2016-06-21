#!/usr/bin/env bash

# find voltdb binaries
if [ -e ../../bin/voltdb ]; then
    # assume this is the examples folder for a kit
    VOLTDB_BIN="$(dirname $(dirname $(pwd)))/bin"
elif [ -n "$(which voltdb 2> /dev/null)" ]; then
    # assume we're using voltdb from the path
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    echo "Unable to find VoltDB installation."
    echo "Please add VoltDB's bin directory to your path."
    exit -1
fi

# call script to set up paths, including
# java classpaths and binary paths
source $VOLTDB_BIN/voltenv

# leader host for startup purposes only
# (once running, all nodes are the same -- no leaders)
STARTUPLEADERHOST="localhost"
# list of cluster nodes separated by commas in host:[port] format
SERVERS="localhost"

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf voltdbroot log procedures/voter/*.class  *.log
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf voter-procs.jar
    rm -rf voter
}

# compile the source code for procedures and the client into jarfiles
function build() {
    # compile java source
    javac -classpath $APPCLASSPATH procedures/voter/*.java
    # build procedure and client jars
    jar cf voter-procs.jar -C procedures voter
    # remove compiled .class files
    rm -rf procedures/voter/*.class

    # compile go source
    go build github.com/VoltDB/voltdb-client-go/examples/voter/client/voter
}

# compile the procedure jarfiles and client if they don't exist
function build-ifneeded() {
    if [ ! -e voter-procs.jar ] || [ ! -e voter ]; then
        build;
    fi
}

# run the voltdb server locally
function server() {
    # note: "create --force" will delete any existing data
    # use "recover" to start from an existing voltdbroot folder with data
    voltdb create --force -H $STARTUPLEADERHOST
}

# load schema and procedures
function init() {
    build-ifneeded
    sqlcmd < ddl.sql
}

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function benchmark-help() {
    build-ifneeded
    ./voter --help
}

# latencyreport: default is OFF
# ratelimit: must be a reasonable value if lantencyreport is ON
# Disable the comments to get latency report
function async-benchmark() {
    build-ifneeded
    ./voter \
        --runtype=async
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=$SERVERS \
        --contestants=6 \
        --maxvotes=2    \
        --goroutines=1
}

# Multi-goroutines synchronous benchmark sample
function sync-benchmark() {
    build-ifneeded
    ./voter \
        --runtype=sync
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=$SERVERS \
        --contestants=6 \
        --maxvotes=2 \
        --goroutines=40
}

# Sql API benchmark sample
function sql-benchmark() {
    build-ifneeded
    ./voter   \
        --runtype=sql
        --displayinterval=5 \
        --duration=120 \
        --servers=$SERVERS \
        --maxvotes=2 \
        --contestants=6 \
        --goroutines=40
}

function help() {
    echo "Usage: ./run.sh {clean|cleanall|build|server|init|benchmark-help|...}"
    echo "       {...|client|async-benchmark|sync-benchmark|sql-benchmark}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done

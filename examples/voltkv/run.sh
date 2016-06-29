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

# remove binaries, logs, runtime artifacts, etc... but keep the client jar
function clean() {
    rm -rf client/voltkv/*.class voltdbroot log
}

# remove everything from "clean" as well as the jarfile
function cleanall() {
    clean
    rm -rf voltkv
}

# compile the source code for the client
function build() {
    # compile go source
    go build github.com/VoltDB/voltdb-client-go/examples/voltkv/client/voltkv
}

# compile the client if it doesn't exist
function build-ifneeded() {
    if [ ! -e voltkv ]; then
        build;
    fi
}

# run the voltdb server locally
function server() {
    voltdb create -H $STARTUPLEADERHOST --force
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

# Use this target for argument help
function benchmark-help() {
    build-ifneeded
    ./voltkv --help
}

# Asynchronous benchmark sample
# latencyreport: default is OFF
# ratelimit: must be a reasonable value if lantencyreport is ON
# Disable the comments (and add a preceding slash) to get latency report
function async-benchmark() {
    build-ifneeded
    ./voltkv \
        --runtype=async
        --displayinterval=5 \
        --duration=120 \
        --servers=$SERVERS \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --usecompression=false
}

# Multi-goroutines synchronous benchmark sample
function sync-benchmark() {
    build-ifneeded
    ./voltkv \
        --runtype=sync
        --displayinterval=5 \
        --duration=120 \
        --servers=$SERVERS \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --goroutines=40
}

# Sql API benchmark sample
# Use this target for argument help
function sql-benchmark() {
    build-ifneeded
    ./voltkv   \
        --runtype=sql
        --duration=120 \
        --servers=$SERVERS \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
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


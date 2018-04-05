#! /usr/bin/env bash

rm dbperf; go build

echo "####### initial build ##################################"
ARGS="-klen 22 -vlen 128"
LOAD="-load 10000000 -lsm"
echo "./dbperf -db mvcc $ARGS $LOAD"
./dbperf -db mvcc $ARGS $LOAD
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo

echo "####### incremental build ##############################"
ARGS="-klen 22 -vlen 128"
LOAD="-load 1000000 -lsm"
UPSERTS="-inserts 1000000 -upserts 1000000 -setas set"
DELETES="-deletes 1000000 -delas del"
READS="-gets 10000000 -getas get -ranges 10000000 -rngas tgn"
echo "./dbperf -db mvcc $ARGS $LOAD $UPSERTS $DELETES $READS"
./dbperf -db mvcc $ARGS $LOAD $UPSERTS $DELETES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo

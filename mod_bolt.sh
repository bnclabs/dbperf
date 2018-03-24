#! /usr/bin/env bash

rm dbperf; go build

echo "########################################################"
ARGS="-klen 22 -vlen 128"
LOAD="-load 10000000"
echo "./dbperf -db bolt $ARGS $LOAD"
./dbperf -db bolt $ARGS $LOAD
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo

echo "########################################################"
ARGS="-klen 22 -vlen 128"
LOAD="-load 1000000"
WRITES="-inserts 1000000 -upserts 1000000 -deletes 1000000"
READS="-gets 10000000 -ranges 10000000"
echo "./dbperf -db bolt $ARGS $LOAD $WRITES $READS"
./dbperf -db bolt $ARGS $LOAD $WRITES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo

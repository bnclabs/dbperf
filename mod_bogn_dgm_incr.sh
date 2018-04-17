#! /usr/bin/env bash

rm -rf *.svg dbperf; go build

echo "###### initial build, small value, without value log ###########"
ARGS="-db bogn -bogn dgm -klen 22 -vlen 128 -memcap 4096 -log bogn"
LOAD="-load 1000000"
WRITES="-inserts 10000000 -upserts 10000000 -deletes 10000000"
READS="-gets 10000000 -ranges 10000000"
echo "./dbperf $ARGS $LOAD $WRITES $READS"
./dbperf $ARGS $LOAD $WRITES
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

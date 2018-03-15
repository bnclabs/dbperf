#! /usr/bin/env bash

ARGS="-db bogn -bogn dgm -klen 22 -vlen 128 -memcap 4096 -log bogn"
LOAD="-load 10000000"

rm -rf *.svg dbperf; go build

echo "./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS"
./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

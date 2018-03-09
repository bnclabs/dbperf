#! /usr/bin/env bash

ARGS="-db bogn -bogn dgm -klen 32 -vlen 200 -memcap 4096 -log bogn"
UPSERTS="-inserts 30000000 -setas set"

rm -rf *.svg dbperf; go build

echo "./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS"
./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

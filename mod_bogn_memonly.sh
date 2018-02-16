#! /usr/bin/env bash

ARGS="-db bogn -bogn memonly -klen 32 -vlen 32"
LOAD="-load 1000000 -lsm"
UPSERTS="-inserts 1000000 -upserts 1000000 -setas set"
DELETES="-deletes 1000000 -delas del"
READS="-gets 10000000 -getas get -ranges 10000000 -rngas vgn"

rm -rf *.svg dbperf; go build

echo "./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS"
./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

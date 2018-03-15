#! /usr/bin/env bash

ARGS="-db bogn -bogn memonly -klen 32 -vlen 512 -log bogn"
LOAD="-load 1000000 -lsm"
UPSERTS="-inserts 2000000 -upserts 2000000 -setas set"
DELETES="-deletes 2000000 -delas del"
READS="-gets 10000000 -getas get -ranges 10000000 -rngas vgn"

rm -rf *.svg dbperf; go build

echo "./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS"
./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

#! /usr/bin/env bash

LOAD="-load 1000000 -lsm"
UPSERTS="-inserts 10000000 -upserts 10000000 -setas set"
DELETES="-deletes 10000000 -delas del"
READS="-gets 100000000 -getas get -ranges 100000000 -rngas vgn"
rm -rf *.svg dbperf; go build

echo "./dbperf -db bogn -klen 32 -vlen 32 $LOAD $UPSERTS $DELETES $READS"
./dbperf -db bogn -klen 32 -vlen 32 $LOAD $UPSERTS $DELETES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

#! /usr/bin/env bash

LOAD="-load 1000000 -lsm"
UPSERTS="-inserts 1000000 -upserts 1000000 -setas set"
DELETES="-deletes 1000000 -delas del"
READS="-gets 10000000 -getas get -ranges 100000000 -rngas tgn"

rm dbperf; go build

echo "./dbperf -db llrb -klen 32 -vlen 32 $LOAD $UPSERTS $DELETES $READS"
./dbperf -db llrb -klen 32 -vlen 32 $LOAD $UPSERTS $DELETES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

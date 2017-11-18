#! /usr/bin/env bash

LOAD="-load 1000000"
WRITES="-inserts 1000000 -upserts 1000000 -deletes 1000000"
READS="-gets 10000000 -ranges 100000000"

go build

echo "./dbperf -db mvcc -klen 32 -vlen 32 $LOAD $WRITES $READS"
./dbperf -db mvcc -klen 32 -vlen 32 $LOAD # $WRITES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

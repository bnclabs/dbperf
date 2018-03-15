#! /usr/bin/env bash

ARGS="-klen 22 -vlen 128"
LOAD="-load 10000000"
WRITES="-inserts 1000000 -upserts 1000000 -deletes 1000000"
READS="-gets 10000000 -ranges 10000000"

go build

echo "./dbperf -db badger $ARGS $LOAD $WRITES $READS"
./dbperf -db badger $ARGS $LOAD # $WRITES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg



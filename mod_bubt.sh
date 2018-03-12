#! /usr/bin/env bash

ARGS="-klen 32 -vlen 200"
LOAD="-load 30000000"
READS="-gets 10000000 -getas all -ranges 10000000 -rngas all"

rm dbperf; go build

echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

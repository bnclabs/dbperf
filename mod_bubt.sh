#! /usr/bin/env bash

LOAD="-load 1000000"
READS="-gets 100000000 -getas all -ranges 10000000 -rngas all"

go build

./dbperf -db bubt -klen 32 -vlen 32 $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

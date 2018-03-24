#! /usr/bin/env bash

rm dbperf; go build

echo "########################################################"
ARGS="-klen 22 -vlen 128 -npaths 3"
LOAD="-load 30000000"
READS="-gets 10000000 -getas all -ranges 10000000 -rngas vgn"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo

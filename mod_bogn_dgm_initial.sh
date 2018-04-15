#! /usr/bin/env bash

rm -rf *.svg dbperf; go build

echo "###### initial build, small value, without value log ###########"
ARGS="-db bogn -bogn dgm -klen 22 -vlen 128 -memcap 4096 -log bogn"
LOAD="-load 30000000"
echo "./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS"
./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

echo "###### initial build, small value, with value log ###########"
ARGS="-db bogn -bogn dgm -klen 22 -vlen 128 -vsize 12288 -memcap 4096 -log bogn"
LOAD="-load 30000000"
echo "./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS"
./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

echo "###### initial build, typical value, with value log ###########"
ARGS="-db bogn -bogn dgm -klen 22 -vlen 1024 -vsize 12288 -memcap 4096 -log bogn"
LOAD="-load 30000000"
echo "./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS"
./dbperf $ARGS $LOAD $UPSERTS $DELETES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg

#! /usr/bin/env bash

rm dbperf; go build

echo "###### initial load ####################################"
ARGS="-klen 22 -vlen 128"
LOAD="-load 10000000"
echo "./dbperf -db lmdb $ARGS $LOAD"
./dbperf -db lmdb $ARGS $LOAD
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo

echo "###### incremental load ################################"
ARGS="-klen 32 -vlen 128"
LOAD="-load 1000000"
WRITES="-inserts 1000000 -upserts 1000000 -deletes 1000000"
READS="-gets 10000000 -ranges 10000000"
echo "./dbperf -db lmdb $ARGS $LOAD $WRITES $READS"
./dbperf -db lmdb $ARGS $LOAD $WRITES $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo

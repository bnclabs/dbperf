#! /usr/bin/env bash

rm dbperf; go build

echo -e "###### random gets, small zsize, no MMAP ######\n"
ARGS="-klen 22 -vlen 128"
LOAD="-load 30000000"
READS="-gets 10000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"

echo -e "###### random gets, small zsize, with MMAP ######\n"
ARGS="-klen 22 -vlen 128 -mmap"
LOAD="-load 30000000"
READS="-gets 10000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"


echo -e "###### random gets, large zsize ######\n"
ARGS="-klen 22 -vlen 128 -zsize 16384"
LOAD="-load 30000000"
READS="-gets 10000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"

echo -e "###### random gets, small zsize ######\n"
ARGS="-klen 22 -vlen 128"
LOAD="-load 30000000"
READS="-gets 10000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"

echo -e "###### random gets, DGM, large zsize ######\n"
ARGS="-klen 22 -vlen 1024 -zsize 16384"
LOAD="-load 30000000"
READS="-gets 3000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"

echo -e "###### random gets, DGM, small zsize ######\n"
ARGS="-klen 22 -vlen 1024"
LOAD="-load 30000000"
READS="-gets 10000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"

echo -e "###### random gets, DGM, small zsize and deep queue ######\n"
ARGS="-klen 22 -vlen 1024 -cpu 32"
LOAD="-load 30000000"
READS="-gets 3000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"

echo -e "###### random ranges ######\n"
ARGS="-klen 22 -vlen 1024"
LOAD="-load 30000000"
READS="-ranges 10000000 -rngas vgn"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"

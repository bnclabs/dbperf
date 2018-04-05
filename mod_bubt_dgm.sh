#! /usr/bin/env bash

rm dbperf; go build

echo -e "###### random gets, no MMAP, no value log ######\n"
ARGS="-klen 22 -vlen 1024 -msize 4096 -zsize 4096"
LOAD="-load 30000000"
READS="-gets 10000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"

echo -e "###### random gets, with MMAP, no value log ######\n"
ARGS="-klen 22 -vlen 1024 -msize 4096 -zsize 4096 -mmap"
LOAD="-load 30000000"
READS="-gets 10000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"

echo -e "###### random gets, no MMAP, with value log ######\n"
ARGS="-klen 22 -vlen 1024 -msize 4096 -zsize 4096 -vsize 16384"
LOAD="-load 30000000"
READS="-gets 10000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"

echo -e "###### random gets, with MMAP, with value log ######\n"
ARGS="-klen 22 -vlen 1024 -msize 4096 -zsize 4096 -vsize 16384 -mmap"
LOAD="-load 30000000"
READS="-gets 10000000 -getas get"
echo "./dbperf -db bubt $ARGS $LOAD $READS"
./dbperf -db bubt $ARGS $LOAD $READS
go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
echo -e "\n"


#echo -e "###### random ranges ######\n"
#ARGS="-klen 22 -vlen 128 -msize 4096 -zsize 4096 -vsize 16384"
#LOAD="-load 30000000"
#READS="-ranges 10000000 -rngas vgn"
#echo "./dbperf -db bubt $ARGS $LOAD $READS"
#./dbperf -db bubt $ARGS $LOAD $READS
#go tool pprof -svg dbperf dbperf.pprof  > pprof.svg
#go tool pprof -alloc_space -svg dbperf dbperf.mprof  > alloc_space.svg
#echo -e "\n"

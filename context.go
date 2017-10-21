package main

import "runtime"

var ninserts int64
var nupserts int64
var ndeletes int64
var xdeletes int64
var numentries int64
var totalwrites int64
var totalreads int64
var delmod = int64(0)
var updmod = int64(1)
var conflicts = int64(0)
var rollbacks = int64(0)
var numcpus = runtime.GOMAXPROCS(-1)

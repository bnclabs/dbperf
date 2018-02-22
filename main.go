package main

import "os"
import "flag"
import "time"
import "strings"
import "runtime"
import "net/http"
import "runtime/pprof"
import _ "net/http/pprof"

import "github.com/bnclabs/golog"
import "github.com/bnclabs/gostore/bogn"
import "github.com/bnclabs/gostore/bubt"
import "github.com/bnclabs/gostore/llrb"

var options struct {
	db       string
	cpu      int
	bogn     string
	memstore string
	period   int
	load     int
	inserts  int
	upserts  int
	deletes  int
	gets     int
	ranges   int
	limit    int
	keylen   int
	vallen   int
	lsm      bool
	seed     int
	setas    string
	delas    string
	getas    string
	rngas    string
	log      string
}

func optparse(args []string) {
	f := flag.NewFlagSet("dbperf", flag.ExitOnError)
	cpu := runtime.GOMAXPROCS(-1) / 2

	f.StringVar(&options.db, "db", "llrb", "pick db storage to benchmark.")
	f.IntVar(&options.cpu, "cpu", cpu, "limit number of cores.")
	f.StringVar(&options.bogn, "bogn", "memonly", "memonly|durable|dgm|workset")
	f.StringVar(&options.memstore, "memstore", "mvcc", "llrb|mvcc for bogn")
	f.IntVar(&options.period, "period", 10, "bogn flush period, in seconds")
	f.IntVar(&options.load, "load", 1000000, "items to initially load")
	f.IntVar(&options.inserts, "inserts", 0, "new items to create")
	f.IntVar(&options.upserts, "upserts", 0, "items to update")
	f.IntVar(&options.deletes, "deletes", 0, "items to delete")
	f.IntVar(&options.gets, "gets", 0, "items to get")
	f.IntVar(&options.ranges, "ranges", 0, "items to iterate")
	f.IntVar(&options.limit, "limit", 100, "limit items per iteration")
	f.IntVar(&options.keylen, "klen", 32, "size of each key")
	f.IntVar(&options.vallen, "vlen", 32, "size of each value")
	f.BoolVar(&options.lsm, "lsm", false, "delete in lsm mode.")
	f.IntVar(&options.seed, "seed", 0, "seed value to generate randomness")
	f.StringVar(&options.setas, "setas", "all", "set|cas|txn|cur|all")
	f.StringVar(&options.delas, "delas", "all", "del|txn|cur|delcur|all")
	f.StringVar(&options.getas, "getas", "all", "get|txn|view|all")
	f.StringVar(&options.rngas, "rngas", "all", "tgn|tyn|vgn|vyn|all")
	f.StringVar(&options.log, "log", "", "llrb,mvcc,bubt,bogn")
	f.Parse(args)

	if options.seed == 0 {
		options.seed = int(time.Now().UnixNano())
	}

	for _, comp := range strings.Split(options.log, ",") {
		switch comp {
		case "bubt":
			bubt.LogComponents("self")
		case "bogn":
			bogn.LogComponents("self")
		case "llrb", "mvcc":
			llrb.LogComponents("self")
		case "all":
			bubt.LogComponents("all")
			bogn.LogComponents("all")
			llrb.LogComponents("all")
		}
	}
}

func main() {
	optparse(os.Args[1:])

	go func() {
		log.Infof("%v", http.ListenAndServe("localhost:6060", nil))
	}()

	// cpu profile
	f1, err := os.Create("dbperf.pprof")
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer f1.Close()
	pprof.StartCPUProfile(f1)
	defer pprof.StopCPUProfile()
	// mem profile
	f2, err := os.Create("dbperf.mprof")
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer f2.Close()
	defer pprof.WriteHeapProfile(f2)

	switch options.db {
	case "lmdb":
		perflmdb()
	case "bolt":
		perfbolt()
	case "llrb":
		perfllrb()
	case "mvcc":
		perfmvcc()
	case "bubt":
		perfbubt()
	case "bogn":
		perfbogn()
	case "badger":
		perfbadger()
	}
}

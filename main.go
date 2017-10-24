package main

import "os"
import "flag"
import "net/http"
import _ "net/http/pprof"
import "runtime/pprof"

import "github.com/prataprc/golog"

var options struct {
	db       string
	load     int
	inserts  int
	upserts  int
	deletes  int
	gets     int
	iterates int
	limit    int
	keylen   int
	vallen   int
	lsm      bool
	seed     int
}

func optparse(args []string) {
	f := flag.NewFlagSet("dbperf", flag.ExitOnError)

	f.StringVar(&options.db, "db", "llrb", "pick db storage to torture test.")
	f.IntVar(&options.load, "load", 1000000, "items to initially load")
	f.IntVar(&options.inserts, "inserts", 0, "new items to create")
	f.IntVar(&options.upserts, "upserts", 0, "items to update")
	f.IntVar(&options.deletes, "deletes", 0, "items to delete")
	f.IntVar(&options.gets, "gets", 0, "items to get")
	f.IntVar(&options.iterates, "iterates", 0, "items to iterate")
	f.IntVar(&options.limit, "limit", 100, "limit items per iteration")
	f.IntVar(&options.keylen, "klen", 32, "size of each key")
	f.IntVar(&options.vallen, "vlen", 32, "size of each value")
	f.IntVar(&options.seed, "seed", 10, "seed value to generate randomness")
	f.Parse(args)
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
	case "llrb":
		perfllrb()
	case "mvcc":
		perfmvcc()
	case "bubt":
		perfbubt()
	}
}

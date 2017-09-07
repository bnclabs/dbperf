package main

import "os"
import "flag"

var options struct {
	db      string
	path    string
	entries int
	keylen  int
	vallen  int
}

func optparse(args []string) {
	f := flag.NewFlagSet("dbperf", flag.ExitOnError)

	f.StringVar(&options.db, "db", "lmdb",
		"pick db storage to performance torture.")
	f.StringVar(&options.path, "path", "perflmdb/",
		"db path to open")
	f.IntVar(&options.entries, "n", 1000000,
		"db path to open")
	f.IntVar(&options.keylen, "key", 32,
		"db path to open")
	f.IntVar(&options.vallen, "val", 128,
		"db path to open")
	f.Parse(args)
}

func main() {
	optparse(os.Args[1:])
	switch options.db {
	case "lmdb":
		lmdbperf()
	}
}

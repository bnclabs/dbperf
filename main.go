package main

import "os"
import "flag"

var options struct {
	db      string
	seed    int
	path    string
	entries int
	keylen  int
	vallen  int
}

var value []byte

func optparse(args []string) {
	f := flag.NewFlagSet("dbperf", flag.ExitOnError)

	f.StringVar(&options.db, "db", "llrb",
		"pick db storage to performance torture.")
	f.StringVar(&options.path, "path", "",
		"db path to open.")
	f.IntVar(&options.entries, "n", 1000000,
		"maximum number entries to load/create.")
	f.IntVar(&options.keylen, "key", 32,
		"key length for each entry, must be > 0.")
	f.IntVar(&options.vallen, "val", 0,
		"value length for each entry >= 0.")
	f.IntVar(&options.seed, "seed", 0,
		"seed to use for random generation.")
	f.Parse(args)

	// initialize value
	value = make([]byte, options.vallen)
	for i := range value {
		value[i] = 'a'
	}
}

func main() {
	optparse(os.Args[1:])
	switch options.db {
	case "lmdb":
		dolmdb()
	case "llrb":
		dollrb()
	}
}

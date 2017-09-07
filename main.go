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

var value []byte

func optparse(args []string) {
	f := flag.NewFlagSet("dbperf", flag.ExitOnError)

	f.StringVar(&options.db, "db", "llrb",
		"pick db storage to performance torture.")
	f.StringVar(&options.path, "path", "",
		"db path to open")
	f.IntVar(&options.entries, "n", 1000000,
		"db path to open")
	f.IntVar(&options.keylen, "key", 32,
		"db path to open")
	f.IntVar(&options.vallen, "val", 0,
		"db path to open")
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

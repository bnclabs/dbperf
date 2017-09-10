package main

import "os"
import "io"
import "flag"

import "github.com/prataprc/golog"

var options struct {
	keylen int
	vallen int
	seed   int
	db     string

	// lmdb specific
	path string

	// load/read/write options
	entries  int
	clone    bool
	scanall  bool
	getters  int
	rangers  int
	writers  int
	grow     int
	loglevel string
	finch    chan struct{}
}

var value []byte

func optparse(args []string) {
	f := flag.NewFlagSet("dbperf", flag.ExitOnError)

	f.IntVar(&options.keylen, "key", 32,
		"key length for each entry, must be > 0.")
	f.IntVar(&options.vallen, "val", 0,
		"value length for each entry >= 0.")
	f.IntVar(&options.seed, "seed", 0,
		"seed to use for random generation.")
	f.StringVar(&options.db, "db", "llrb",
		"pick db storage to performance torture.")
	f.StringVar(&options.path, "path", "",
		"db path to open.")

	f.IntVar(&options.entries, "n", 1000000,
		"number of random entries to initially load.")
	f.BoolVar(&options.clone, "clone", false,
		"clone after initial data load.")
	f.BoolVar(&options.scanall, "scanall", false,
		"full table scan after initial data load.")

	f.IntVar(&options.getters, "getters", 0,
		"after initial load, spawn routines to do Get call")
	f.IntVar(&options.rangers, "rangers", 0,
		"after initial load, spawn routines to do Range call")
	f.IntVar(&options.writers, "writers", 0,
		"after initial load, spawn routines for Update/Delete/Create")
	f.IntVar(&options.grow, "grow", 0,
		"increasing data set by grow factor")
	f.StringVar(&options.loglevel, "log", "warn",
		"log level")

	f.Parse(args)

	// initialize value
	value = make([]byte, options.vallen)
	for i := range value {
		value[i] = 'a'
	}
	setts := map[string]interface{}{
		"log.flags":      "lshortfile",
		"log.level":      options.loglevel,
		"log.timeformat": "",
		"log.prefix":     "",
		"log.colorfatal": "red",
		"log.colorerror": "hired",
		"log.colorwarn":  "yellow",
	}
	log.SetLogger(nil, setts)
}

func main() {
	optparse(os.Args[1:])
	if options.path != "" {
		if err := os.MkdirAll(options.path, 0755); err != nil {
			log.Errorf("%v", err)
			os.Exit(1)
		}
		defer os.RemoveAll(options.path)
	}

	switch options.db {
	case "lmdb":
		dolmdb()
	case "llrb":
		dollrb()
	}
	exitoneof()
}

func exitoneof() {
	buf := make([]byte, 4)
	for {
		if _, err := os.Stdin.Read(buf); err != nil && err == io.EOF {
			os.Exit(0)
		}
	}
}

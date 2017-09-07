package main

import "github.com/prataprc/golog"
import "github.com/bmatsuo/lmdb-go/lmdb"

func lmdbperf() error {
	env, err := lmdb.NewEnv()
	if err != nil {
		log.Errorf("%v", err)
		return err
	}
	defer env.Close()

	env.SetMaxDBs(1)
	size := (options.keylen + options.vallen) * options.entries
	env.SetMapSize(int64(size))

	// FixedMap   Danger zone. Map memory at a fixed address.
	// Readonly   Used in several functions to denote an object as readonly.
	// WriteMap   Use a writable memory map.
	// NoMetaSync Don't fsync metapage after commit.
	// NoSync     Don't fsync after commit.
	// MapAsync   Flush asynchronously when using the WriteMap flag.
	err = env.Open(options.path, 0, 0664)
	if err != nil {
		log.Errorf("%v", err)
		return err
	}

	// Clear stale readers
	stalerds, err := env.ReaderCheck()
	if err != nil {
		log.Errorf("%v", err)
		return err
	} else if stalerds > 0 {
		log.Infof("cleared %d reader slots from dead processes", stalerds)
	}

	var dbi lmdb.DBI
	if err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("dbperf")
		return err
	}); err != nil {
		log.Errorf("%v")
		return err
	}

	return nil
}

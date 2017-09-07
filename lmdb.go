package main

import "github.com/prataprc/golog"
import "github.com/bmatsuo/lmdb-go/lmdb"

func dolmdb() error {
	env, err := initlmdb()
	if err != nil {
		return err
	}
	defer env.Close()

	var dbi lmdb.DBI

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("perf")
		return err
	})
	if err != nil {
		log.Errorf("lmdb.OpenDBI():%v", err)
		return err
	} else if err = lmdbLoad(env, dbi); err != nil {
		return err
	}

	return nil
}

func initlmdb() (*lmdb.Env, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		log.Errorf("%v", err)
		return nil, err
	}

	env.SetMaxDBs(1)
	size := 1000000000
	env.SetMapSize(int64(size))

	// FixedMap   Danger zone. Map memory at a fixed address.
	// Readonly   Used in several functions to denote an object as readonly.
	// WriteMap   Use a writable memory map.
	// NoMetaSync Don't fsync metapage after commit.
	// NoSync     Don't fsync after commit.
	// MapAsync   Flush asynchronously when using the WriteMap flag.
	err = env.Open(options.path, 0, 0664)
	if err != nil {
		env.Close()
		log.Errorf("%v", err)
		return env, err
	}

	// Clear stale readers
	stalerds, err := env.ReaderCheck()
	if err != nil {
		env.Close()
		log.Errorf("%v", err)
		return env, err
	} else if stalerds > 0 {
		log.Infof("cleared %d reader slots from dead processes", stalerds)
	}
	return env, nil
}

func lmdbLoad(env *lmdb.Env, dbi lmdb.DBI) error {
	genkey := Generateloads(int64(options.keylen), int64(options.entries))
	key := make([]byte, options.keylen*2)
	for key = genkey(key); key != nil; key = genkey(key) {
		err := env.Update(func(txn *lmdb.Txn) (err error) {
			if err := txn.Put(dbi, key, value, 0); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Errorf("lmdb.Update(%q):%v", key, err)
			return err
		}
	}
	return nil
}

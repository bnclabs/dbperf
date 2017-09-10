package main

import "os"
import "fmt"
import "time"
import "math/rand"

import "github.com/prataprc/golog"
import humanize "github.com/dustin/go-humanize"
import "github.com/bmatsuo/lmdb-go/lmdb"

func dolmdb() error {
	env, dbi, err := initlmdb(lmdb.NoSync | lmdb.NoMetaSync)
	if err != nil {
		return err
	}
	defer env.Close()

	var seedl, seedc int64

	seedl = int64(options.seed)
	if options.writers > 0 {
		seedc = seedl + 100
	}
	if err = lmdbLoad(env, dbi, seedl); err != nil {
		return err
	}
	if options.clone {
		lmdbClone(env, dbi)
	} else if options.scanall {
		lmdbScanall(env, dbi)
	}
	if options.getters > 0 {
		lmdbGetters(seedl, seedc)
	}
	//if options.rangers > 0 {
	//	lmdbRangers(env, dbi)
	//}
	//if options.writers > 0 {
	//	lmdbWriters(env, dbi)
	//}
	return nil
}

func lmdbLoad(env *lmdb.Env, dbi lmdb.DBI, seedl int64) error {
	var genkey func([]byte) []byte

	klen, n := int64(options.keylen), int64(options.entries)
	if seedl > 0 {
		genkey = Generateloadr(klen, n, int64(seedl))
	} else {
		genkey = Generateloads(klen, n)
	}

	now := time.Now()
	key := make([]byte, options.keylen*2)
	err := env.Update(func(txn *lmdb.Txn) (err error) {
		for key = genkey(key); key != nil; key = genkey(key) {
			fmt.Printf("%s\n", key)
			if err := txn.Put(dbi, key, value, 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("lmdb.Update(%q):%v", key, err)
		return err
	}
	sz, _ := DirSize(options.path)
	size := humanize.Bytes(uint64(sz))
	entries := lmdbStat(env, dbi).Entries
	fmsg := "loaded %v entries in %v, disk size %v\n"
	fmt.Printf(fmsg, entries, time.Since(now), size)
	return nil
}

func lmdbClone(env *lmdb.Env, dbi lmdb.DBI) error {
	clonepath := "clone-" + options.path
	os.Mkdir(clonepath, 0755)

	now := time.Now()

	err := env.Copy(clonepath)
	if err != nil {
		log.Errorf("lmdb.Copy(%q): %v", clonepath, err)
	}

	sz, _ := DirSize(clonepath)
	size := humanize.Bytes(uint64(sz))
	entries := lmdbStat(env, dbi).Entries
	fmsg := "cloned %v entries in %v, disk size %v\n"
	fmt.Printf(fmsg, entries, time.Since(now), size)
	defer os.RemoveAll(clonepath)
	return err
}

func lmdbScanall(env *lmdb.Env, dbi lmdb.DBI) error {
	now := time.Now()
	err := env.View(func(txn *lmdb.Txn) error {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			log.Errorf("lmdb.OpenCursor(): %v", err)
			return err
		}
		defer cur.Close()

		for {
			if _, _, err := cur.Get(nil, nil, lmdb.Next); lmdb.IsNotFound(err) {
				return nil
			} else if err != nil {
				log.Errorf("lmdb.Get(): %v", err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("lmdb.View(): %v", err)
	} else {
		entries := lmdbStat(env, dbi).Entries
		fmsg := "full table scan of %v entries took %v\n"
		fmt.Printf(fmsg, entries, time.Since(now))
	}
	return err
}

func lmdbGetters(seedl, seedc int64) {
	getter := func(getid int) {
		time.Sleep(time.Duration(rand.Intn(10000)) * time.Millisecond)
		env, dbi, err := readlmdb(0)
		if err != nil {
			return
		}
		defer env.Close()

		klen, loadn := int64(options.keylen), int64(options.entries)
		genkey := Generateread(klen, loadn, seedl, seedc)
		key := make([]byte, options.keylen)

		now, count := time.Now(), 0
		for key = genkey(key); key != nil; key = genkey(key) {
			if err := env.View(func(txn *lmdb.Txn) error {
				_, err := txn.Get(dbi, key)
				return err
			}); err != nil {
				log.Errorf("%v lmdb.View(%s): %v", getid, key, err)
				return
			} else if count = count + 1; (count % 1000000) == 0 {
				fmsg := "%v: Get 1000000 random docs in %v\n"
				fmt.Printf(fmsg, getid, time.Since(now))
				now = time.Now()
			}
		}
		rem := count % 1000000
		fmsg := "%v: Get %v random docs in %v\n"
		fmt.Printf(fmsg, getid, rem, time.Since(now))
	}

	for i := 0; i < options.getters; i++ {
		go getter(i)
	}
}

func initlmdb(envflags uint) (*lmdb.Env, lmdb.DBI, error) {
	var dbi lmdb.DBI

	env, err := lmdb.NewEnv()
	if err != nil {
		log.Errorf("%v", err)
		return nil, dbi, err
	}

	env.SetMaxDBs(100)
	size := 14 * 1024 * 1024 * 1024
	env.SetMapSize(int64(size))

	// FixedMap   Danger zone. Map memory at a fixed address.
	// Readonly   Used in several functions to denote an object as readonly.
	// WriteMap   Use a writable memory map.
	// NoMetaSync Don't fsync metapage after commit.
	// NoSync     Don't fsync after commit.
	// MapAsync   Flush asynchronously when using the WriteMap flag.
	err = env.Open(options.path, envflags, 0755)
	if err != nil {
		env.Close()
		log.Errorf("%v", err)
		return env, dbi, err
	}

	// Clear stale readers
	stalerds, err := env.ReaderCheck()
	if err != nil {
		env.Close()
		log.Errorf("%v", err)
		return env, dbi, err
	} else if stalerds > 0 {
		log.Infof("cleared %d reader slots from dead processes", stalerds)
	}

	// load lmdb
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("perf")
		return err
	})
	if err != nil {
		log.Errorf("lmdb.CreateDBI():%v", err)
		return env, dbi, err
	}
	return env, dbi, nil
}

func readlmdb(envflags uint) (*lmdb.Env, lmdb.DBI, error) {
	var dbi lmdb.DBI

	env, err := lmdb.NewEnv()
	if err != nil {
		log.Errorf("%v", err)
		return nil, dbi, err
	}

	env.SetMaxDBs(100)

	// FixedMap   Danger zone. Map memory at a fixed address.
	// Readonly   Used in several functions to denote an object as readonly.
	// WriteMap   Use a writable memory map.
	// NoMetaSync Don't fsync metapage after commit.
	// NoSync     Don't fsync after commit.
	// MapAsync   Flush asynchronously when using the WriteMap flag.
	err = env.Open(options.path, envflags, 0755)
	if err != nil {
		env.Close()
		log.Errorf("%v", err)
		return env, dbi, err
	}

	// open dbi
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("perf", 0)
		return err
	})
	if err != nil {
		log.Errorf("lmdb.OpenDBI():%v", err)
		return env, dbi, err
	}
	return env, dbi, nil
}

func lmdbStat(env *lmdb.Env, dbi lmdb.DBI) (stat *lmdb.Stat) {
	if err := env.View(func(txn *lmdb.Txn) (err error) {
		stat, err = txn.Stat(dbi)
		return nil
	}); err != nil {
		log.Errorf("lmdb.View(): %v", err)
	}
	return stat
}

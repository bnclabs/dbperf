package main

import "os"
import "fmt"
import "sync"
import "time"
import "sync/atomic"
import "math/rand"
import "path/filepath"

import "github.com/bnclabs/golog"
import "github.com/bmatsuo/lmdb-go/lmdb"
import humanize "github.com/dustin/go-humanize"

func perflmdb() error {
	path := lmdbpath()
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
	}()
	fmt.Printf("LMDB path %q\n", path)

	env, dbi, err := initlmdb(path, lmdb.NoSync|lmdb.NoMetaSync)
	if err != nil {
		panic(err)
	}
	defer env.Close()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err = lmdbLoad(env, dbi, seedl); err != nil {
		return err
	}

	var wg sync.WaitGroup
	n := atomic.LoadInt64(&numentries)
	fin := make(chan struct{})

	if options.inserts+options.upserts+options.deletes > 0 {
		// writer routine
		go lmdbWriter(env, dbi, n, seedc, fin, &wg)
		wg.Add(1)
	}
	if options.gets > 0 {
		for i := 0; i < options.cpu; i++ {
			go lmdbGetter(path, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	if options.ranges > 0 {
		for i := 0; i < options.cpu; i++ {
			go lmdbRanger(path, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	wg.Wait()
	close(fin)

	dirsize, err := DirSize(path)
	if err != nil {
		panic(err)
	}
	fmsg := "LMDB total indexed %v items, footprint %v\n"
	fmt.Printf(fmsg, getlmdbCount(env, dbi), humanize.Bytes(uint64(dirsize)))

	return nil
}

func initlmdb(
	path string, envflags uint) (env *lmdb.Env, dbi lmdb.DBI, err error) {

	env, err = lmdb.NewEnv()
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	defer func() {
		if err != nil {
			env.Close()
		}
	}()

	env.SetMaxDBs(100)
	env.SetMapSize(14 * 1024 * 1024 * 1024) // 14GB

	// FixedMap   Danger zone. Map memory at a fixed address.
	// Readonly   Used in several functions to denote an object as readonly.
	// WriteMap   Use a writable memory map.
	// NoMetaSync Don't fsync metapage after commit.
	// NoSync     Don't fsync after commit.
	// MapAsync   Flush asynchronously when using the WriteMap flag.
	err = env.Open(path, envflags, 0755)
	if err != nil {
		log.Errorf("%v", err)
		return
	}

	// Clear stale readers
	stalereads, err := env.ReaderCheck()
	if err != nil {
		log.Errorf("%v", err)
		return
	} else if stalereads > 0 {
		log.Infof("cleared %d reader slots from dead processes", stalereads)
	}

	// load lmdb
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("perf")
		return err
	})
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	return
}

func readlmdb(path string, envflags uint) (*lmdb.Env, lmdb.DBI, error) {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

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
	err = env.Open(path, envflags, 0755)
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

func lmdbLoad(env *lmdb.Env, dbi lmdb.DBI, seedl int64) error {
	var key, value []byte

	markercount, count := int64(1000000), int64(0)
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	populate := func(txn *lmdb.Txn) (err error) {
		for i := 0; i < 1000000 && key != nil; i++ {
			if err := txn.Put(dbi, key, value, 0); err != nil {
				return err
			}
			count++
			key, value = g(key, value)
		}
		return nil
	}
	now, epoch := time.Now(), time.Now()
	key, value = g(key, value)
	for key != nil {
		if err := env.Update(populate); err != nil {
			log.Errorf("key %q err : %v", key, err)
			return err
		}
		if count%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "lmdbLoad {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, count, y)
			now = time.Now()
		}
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	count = int64(getlmdbCount(env, dbi))
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("Loaded %v items in %v\n", count, took)
	return nil
}

func lmdbWriter(
	env *lmdb.Env, dbi lmdb.DBI, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var key, value []byte
	var x, y, z int64
	loadn := int64(options.load)
	klen, vlen := int64(options.keylen), int64(options.vallen)
	gcreate := Generatecreate(klen, vlen, loadn, seedc)
	gupdate := Generateupdate(klen, vlen, loadn, seedl, seedc, -1)
	gdelete := Generatedelete(klen, vlen, loadn, seedl, seedc, delmod)

	put := func(txn *lmdb.Txn) (err error) {
		if err := txn.Put(dbi, key, value, 0); err != nil {
			return err
		}
		return nil
	}
	update := func(txn *lmdb.Txn) (err error) {
		if err := txn.Put(dbi, key, value, 0); err != nil {
			return err
		}
		return nil
	}
	delete := func(txn *lmdb.Txn) (err error) {
		if err := txn.Del(dbi, key, nil); err != nil {
			return err
		}
		return nil
	}

	rnd := rand.New(rand.NewSource(seedl))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	insn, upsn, deln := options.inserts, options.upserts, options.deletes
	count := int64(0)
	for totalops := insn + upsn + deln; totalops > 0; {
		idx := rnd.Intn(totalops)
		switch {
		case idx < insn:
			key, value = gcreate(key, value)
			if err := env.Update(put); err != nil {
				log.Errorf("key %q err : %v", key, err)
				return
			}
			atomic.AddInt64(&numentries, 1)
			x = atomic.AddInt64(&ninserts, 1)
			insn--

		case idx < upsn:
			key, value = gupdate(key, value)
			if err := env.Update(update); err != nil {
				log.Errorf("key %q err : %v", key, err)
				return
			}
			y = atomic.AddInt64(&nupserts, 1)
			upsn--

		case idx < deln:
			key, value = gdelete(key, value)
			if err := env.Update(delete); err != nil {
				atomic.AddInt64(&xdeletes, 1)
			} else {
				atomic.AddInt64(&ndeletes, 1)
			}
			z = atomic.LoadInt64(&ndeletes) + atomic.LoadInt64(&xdeletes)
			atomic.AddInt64(&numentries, -1)
			deln--
		}
		totalops = insn + upsn + deln
		if count > 0 && count%markercount == 0 {
			a := time.Since(now).Round(time.Second)
			b := time.Since(epoch).Round(time.Second)
			fmsg := "lmdbWriter {ins:%v,ups:%v,dels:%v in %v}, {%v ops %v}\n"
			fmt.Printf(fmsg, x, y, z, b, markercount, a)
			now = time.Now()
		}
		count++
	}

	n := atomic.AddInt64(&totalwrites, int64(x+y+z))

	took := time.Since(epoch).Round(time.Second)
	wg.Done()
	<-fin
	fmsg := "at exit lmdbWriter {%v,%v,%v (%v) in %v}\n"
	fmt.Printf(fmsg, x, y, z, n, took)
}

func lmdbGetter(
	path string, loadn, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var ngets, nmisses int64

	epoch := time.Now()
	defer func() {
		took := time.Since(epoch).Round(time.Second)
		wg.Done()
		<-fin
		fmsg := "at exit, lmdbGetter %v:%v items in %v\n"
		fmt.Printf(fmsg, ngets, nmisses, took)
	}()

	env, dbi, err := readlmdb(path, 0)
	if err != nil {
		return
	}
	defer env.Close()

	time.Sleep(time.Duration(rand.Intn(100)+300) * time.Millisecond)

	var key []byte
	g := Generateread(int64(options.keylen), loadn, seedl, seedc)

	get := func(txn *lmdb.Txn) (err error) {
		ngets++
		_, err = txn.Get(dbi, key)
		if err != nil {
			nmisses++
		}
		return nil
	}

	now, markercount := time.Now(), int64(10000000)
	for ngets+nmisses < int64(options.gets) {
		key = g(key, atomic.LoadInt64(&ninserts))
		env.View(get)
		if (ngets+nmisses)%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "lmdbGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}
	}
}

func lmdbRanger(
	path string, loadn, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var nranges, nmisses int64

	epoch := time.Now()
	defer func() {
		took := time.Since(epoch).Round(time.Second)
		wg.Done()
		<-fin
		fmsg := "at exit, lmdbRanger %v:%v items in %v\n"
		fmt.Printf(fmsg, nranges, nmisses, took)
	}()

	env, dbi, err := readlmdb(path, 0)
	if err != nil {
		return
	}
	defer env.Close()

	time.Sleep(time.Duration(rand.Intn(100)+300) * time.Millisecond)

	var key []byte
	g := Generateread(int64(options.keylen), loadn, seedl, seedc)

	ranger := func(txn *lmdb.Txn) error {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			log.Errorf("lmdb.OpenCursor(): %v", err)
			return err
		}
		defer cur.Close()

		_, _, err = cur.Get(key, nil, 0)
		for i := 0; i < options.limit; i++ {
			nranges++
			if err != nil {
				nmisses++
			}
			_, _, err = cur.Get(nil, nil, lmdb.Next)
		}
		return nil
	}

	now, markercount := time.Now(), int64(100000000)
	for nranges+nmisses < int64(options.ranges) {
		key = g(key, atomic.LoadInt64(&ninserts))
		env.View(ranger)
		if (nranges+nmisses)%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "lmdbRanger {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nranges, nmisses, y)
			now = time.Now()
		}
	}
}

func lmdbpath() string {
	path := filepath.Join(os.TempDir(), "lmdb.data")
	if err := os.RemoveAll(path); err != nil {
		panic(err)
	} else if err := os.MkdirAll(path, 0775); err != nil {
		panic(err)
	}
	return path
}

func getlmdbCount(env *lmdb.Env, dbi lmdb.DBI) (count uint64) {
	err := env.Update(func(txn *lmdb.Txn) (err error) {
		stat, err := txn.Stat(dbi)
		if err != nil {
			return err
		}
		count = stat.Entries
		return nil
	})
	if err != nil {
		panic(err)
	}
	return
}

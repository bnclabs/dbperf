package main

import "os"
import "fmt"
import "sync"
import "time"
import "sync/atomic"
import "math/rand"
import "path/filepath"

import "github.com/bnclabs/golog"
import "github.com/dgraph-io/badger"
import humanize "github.com/dustin/go-humanize"

var bucketname = "dbperf"

func perfbadger() error {
	pathdir := badgerpath()
	defer func() {
		//if err := os.RemoveAll(pathdir); err != nil {
		//	panic(err)
		//}
	}()
	fmt.Printf("BADGER path %q\n", pathdir)

	db, err := initbadger(pathdir)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	klen, vlen := int64(options.keylen), int64(options.vallen)+100
	loadn := int64(options.load)
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err = badgerload(db, klen, vlen, loadn, seedl); err != nil {
		return err
	}

	var wg sync.WaitGroup
	n := atomic.LoadInt64(&numentries)
	fin := make(chan struct{})

	if options.inserts+options.upserts+options.deletes > 0 {
		// writer routine
		go badgerWriter(db, klen, vlen, n, seedl, seedc, fin, &wg)
		wg.Add(1)
	}
	if options.gets > 0 {
		for i := 0; i < options.cpu; i++ {
			go badgerGetter(db, klen, vlen, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	if options.ranges > 0 {
		for i := 0; i < options.cpu; i++ {
			go badgerRanger(db, klen, vlen, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	wg.Wait()
	close(fin)

	dirsize, err := DirSize(pathdir)
	if err != nil {
		panic(err)
	}
	fmt.Printf("BADGER footprint: %v\n", humanize.Bytes(uint64(dirsize)))

	return nil
}

func initbadger(pathdir string) (db *badger.DB, err error) {
	opts := badger.DefaultOptions
	opts.Dir, opts.ValueDir = pathdir, pathdir
	db, err = badger.Open(opts)
	if err != nil {
		fmt.Printf("badger.Open(): %v\n", err)
		return nil, err
	}
	return db, nil
}

type badgerop struct {
	op    byte
	key   []byte
	value []byte
}

func badgerload(db *badger.DB, klen, vlen, loadn, seedl int64) error {
	markercount, count := int64(1000000), int64(0)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	cmds := make([]*badgerop, 10000)
	for off := range cmds {
		cmds[off] = &badgerop{}
	}
	populate := func(txn *badger.Txn) (err error) {
		for _, cmd := range cmds {
			if err = txn.Set(cmd.key, cmd.value); err != nil {
				log.Errorf("key %q err : %v", cmd.key, err)
				return err
			}
		}
		return nil
	}

	now, epoch, off := time.Now(), time.Now(), 0
	cmd := cmds[off]
	cmd.key, cmd.value = g(cmd.key, cmd.value)
	for off = 1; cmd.key != nil; off++ {
		if off == len(cmds) {
			if err := db.Update(populate); err != nil {
				panic(err)
			}
			off = 0
		}
		cmd = cmds[off]
		cmd.key, cmd.value = g(cmd.key, cmd.value)
		if count%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "badgerload {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, count, y)
			now = time.Now()
		}
		count++
	}

	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("Loaded %v items in %v\n", options.load, took)
	return nil
}

func badgerWriter(
	db *badger.DB, klen, vlen, loadn, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var x, y, z int64
	gcreate := Generatecreate(klen, vlen, loadn, seedc)
	gupdate := Generateupdate(klen, vlen, loadn, seedl, seedc, -1)
	gdelete := Generatedelete(klen, vlen, loadn, seedl, seedc, delmod)

	cmds := make([]*badgerop, 500)
	for off := range cmds {
		cmds[off] = &badgerop{}
	}
	rnd := rand.New(rand.NewSource(seedl))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	writeupdate := func(txn *badger.Txn) (err error) {
		for _, cmd := range cmds {
			switch cmd.op {
			case 1:
				//fmt.Printf("create %v %q %q\n", cmd.op, cmd.key, cmd.value)
				if err := txn.Set(cmd.key, cmd.value); err != nil {
					log.Errorf("create key %q err : %v", cmd.key, err)
					return err
				}
				atomic.AddInt64(&numentries, 1)
				x = atomic.AddInt64(&ninserts, 1)

			case 2:
				//fmt.Printf("update %v %q %q\n", cmd.op, cmd.key, cmd.value)
				if err := txn.Set(cmd.key, cmd.value); err != nil {
					log.Errorf("update key %q err : %v", cmd.key, err)
					return err
				}
				y = atomic.AddInt64(&nupserts, 1)

			case 3:
				//fmt.Printf("delete %v %q %q\n", cmd.op, cmd.key, cmd.value)
				if err := txn.Delete(cmd.key); err != nil {
					atomic.AddInt64(&xdeletes, 1)
				} else {
					atomic.AddInt64(&ndeletes, 1)
				}
				z = atomic.LoadInt64(&ndeletes) + atomic.LoadInt64(&xdeletes)
				atomic.AddInt64(&numentries, -1)
			}
		}
		return nil
	}

	count, off := int64(0), 0
	insn, upsn, deln := options.inserts, options.upserts, options.deletes
	for totalops := insn + upsn + deln; totalops > 0; off++ {
		if off == len(cmds) { // batch commit
			if err := db.Update(writeupdate); err != nil {
				panic(err)
			}
			off = 0
		}

		idx, cmd := rnd.Intn(totalops), cmds[off]
		switch {
		case idx < insn:
			cmd.op = 1
			cmd.key, cmd.value = gcreate(cmd.key, cmd.value)
			insn--

		case idx < (insn + upsn):
			cmd.op = 2
			cmd.key, cmd.value = gupdate(cmd.key, cmd.value)
			upsn--

		case idx < (insn + upsn + deln):
			cmd.op = 3
			cmd.key, cmd.value = gdelete(cmd.key, cmd.value)
			deln--

		default:
			fmsg := "insn: %v, upsn: %v, deln: %v idx: %v"
			panic(fmt.Errorf(fmsg, insn, upsn, deln, idx))
		}
		totalops = insn + upsn + deln

		if count > 0 && count%markercount == 0 {
			a := time.Since(now).Round(time.Second)
			b := time.Since(epoch).Round(time.Second)
			fmsg := "badgerWriter {ins:%v,ups:%v,dels:%v in %v}, {%v ops %v}\n"
			fmt.Printf(fmsg, x, y, z, b, markercount, a)
			now = time.Now()
		}
		count++
	}

	n := atomic.AddInt64(&totalwrites, int64(x+y+z))

	took := time.Since(epoch).Round(time.Second)
	wg.Done()
	<-fin
	fmsg := "at exit badgerWriter {%v,%v,%v (%v) in %v}\n"
	fmt.Printf(fmsg, x, y, z, n, took)
}

func badgerGetter(
	db *badger.DB, klen, vlen, loadn, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var ngets, nmisses int64

	epoch := time.Now()
	defer func() {
		took := time.Since(epoch).Round(time.Second)
		wg.Done()
		<-fin
		fmsg := "at exit, badgerGetter %v:%v items in %v\n"
		fmt.Printf(fmsg, ngets, nmisses, took)
	}()

	time.Sleep(time.Duration(rand.Intn(100)+300) * time.Millisecond)

	var key []byte
	g := Generateread(int64(options.keylen), loadn, seedl, seedc)

	get := func(txn *badger.Txn) (err error) {
		if _, err = txn.Get(key); err != nil {
			nmisses++
		} else {
			ngets++
		}
		return nil
	}

	now, markercount := time.Now(), int64(10000000)
	for ngets+nmisses < int64(options.gets) {
		key = g(key, atomic.LoadInt64(&ninserts))
		if err := db.View(get); err != nil {
			panic(err)
		}
		if (ngets+nmisses)%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "badgerGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}
	}
}

func badgerRanger(
	db *badger.DB, klen, vlen, loadn, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var nranges, nmisses int64

	epoch := time.Now()
	defer func() {
		took := time.Since(epoch).Round(time.Second)
		wg.Done()
		<-fin
		fmsg := "at exit, badgerRanger %v:%v items in %v\n"
		fmt.Printf(fmsg, nranges, nmisses, took)
	}()

	time.Sleep(time.Duration(rand.Intn(100)+300) * time.Millisecond)

	var key []byte
	g := Generateread(int64(options.keylen), loadn, seedl, seedc)

	ranger := func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = options.limit
		it := txn.NewIterator(opts)
		it.Seek(key)
		for i := 0; it.Valid() && i < options.limit; i++ {
			it.Next()
			nranges++
		}
		return nil
	}

	now, markercount := time.Now(), int64(100000000)
	for nranges+nmisses < int64(options.ranges) {
		key = g(key, atomic.LoadInt64(&ninserts))
		if err := db.View(ranger); err != nil {
			panic(err)
		}
		if (nranges+nmisses)%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "badgerRanger {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nranges, nmisses, y)
			now = time.Now()
		}
	}
}

func badgerpath() string {
	path := filepath.Join(os.TempDir(), "badger.data")
	if err := os.RemoveAll(path); err != nil {
		panic(err)
	}
	return path
}

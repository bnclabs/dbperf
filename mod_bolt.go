package main

import "os"
import "fmt"
import "sync"
import "time"
import "sync/atomic"
import "math/rand"
import "path/filepath"

import "github.com/bnclabs/golog"
import "github.com/coreos/bbolt"
import humanize "github.com/dustin/go-humanize"

func perfbolt() error {
	path := boltpath()
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
	}()
	fmt.Printf("BOLT path %q\n", path)

	db, err := initbolt(path)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	klen, vlen := int64(options.keylen), int64(options.vallen)+100
	loadn := int64(options.load)
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err = boltload(db, klen, vlen, loadn, seedl); err != nil {
		return err
	}

	var wg sync.WaitGroup
	n := atomic.LoadInt64(&numentries)
	fin := make(chan struct{})

	if options.inserts+options.upserts+options.deletes > 0 {
		// writer routine
		go boltWriter(db, klen, vlen, n, seedl, seedc, fin, &wg)
		wg.Add(1)
	}
	if options.gets > 0 {
		for i := 0; i < options.cpu; i++ {
			go boltGetter(db, klen, vlen, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	if options.ranges > 0 {
		for i := 0; i < options.cpu; i++ {
			go boltRanger(db, klen, vlen, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	wg.Wait()
	close(fin)

	dirsize, err := DirSize(path)
	if err != nil {
		panic(err)
	}
	fmsg := "BOLT total indexed %v items, footprint %v\n"
	fmt.Printf(fmsg, getboltCount(db), humanize.Bytes(uint64(dirsize)))

	return nil
}

func initbolt(path string) (db *bolt.DB, err error) {
	// Open the database.
	db, err = bolt.Open(path, 0666, nil)
	if err != nil {
		fmt.Printf("bolt.Open(): %v\n", err)
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		_, err := tx.CreateBucket([]byte(bucketname))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		fmt.Printf("bolt.CreateBucket(%q): %v\n", bucketname, err)
		return nil, err
	}
	return db, nil
}

func boltload(db *bolt.DB, klen, vlen, loadn, seedl int64) error {
	var key, value []byte

	markercount, count := int64(1000000), int64(0)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	populate := func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(bucketname))
		for i := 0; i < 1000000 && key != nil; i++ {
			if err = b.Put(key, value); err != nil {
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
		if err := db.Update(populate); err != nil {
			log.Errorf("key %q err : %v", key, err)
			return err
		}
		if count%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "boltload {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, count, y)
			now = time.Now()
		}
	}

	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	count = int64(getboltCount(db))
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("Loaded %v items in %v\n", count, took)
	return nil
}

func boltWriter(
	db *bolt.DB, klen, vlen, loadn, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var key, value []byte
	var x, y, z int64
	gcreate := Generatecreate(klen, vlen, loadn, int64(options.inserts), seedc)
	gupdate := Generateupdate(
		klen, vlen, loadn, int64(options.inserts), seedl, seedc, -1,
	)
	gdelete := Generatedelete(
		klen, vlen, loadn, int64(options.inserts), seedl, seedc, delmod,
	)

	bname := []byte(bucketname)
	put := func(tx *bolt.Tx) (err error) {
		b := tx.Bucket(bname)
		if err := b.Put(key, value); err != nil {
			return err
		}
		return nil
	}
	update := func(tx *bolt.Tx) (err error) {
		b := tx.Bucket(bname)
		if err := b.Put(key, value); err != nil {
			return err
		}
		return nil
	}
	delete := func(tx *bolt.Tx) (err error) {
		b := tx.Bucket(bname)
		if err := b.Delete(key); err != nil {
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
			if err := db.Update(put); err != nil {
				log.Errorf("key %q err : %v", key, err)
				return
			}
			atomic.AddInt64(&numentries, 1)
			x = atomic.AddInt64(&ninserts, 1)
			insn--

		case idx < (insn + upsn):
			key, value = gupdate(key, value)
			if err := db.Update(update); err != nil {
				log.Errorf("key %q err : %v", key, err)
				return
			}
			y = atomic.AddInt64(&nupserts, 1)
			upsn--

		case idx < (insn + upsn + deln):
			key, value = gdelete(key, value)
			if err := db.Update(delete); err != nil {
				atomic.AddInt64(&xdeletes, 1)
			} else {
				atomic.AddInt64(&ndeletes, 1)
			}
			z = atomic.LoadInt64(&ndeletes) + atomic.LoadInt64(&xdeletes)
			atomic.AddInt64(&numentries, -1)
			deln--

		default:
			fmsg := "insn: %v, upsn: %v, deln: %v idx: %v"
			panic(fmt.Errorf(fmsg, insn, upsn, deln, idx))
		}
		totalops = insn + upsn + deln
		if count > 0 && count%markercount == 0 {
			a := time.Since(now).Round(time.Second)
			b := time.Since(epoch).Round(time.Second)
			fmsg := "boltWriter {ins:%v,ups:%v,dels:%v in %v}, {%v ops %v}\n"
			fmt.Printf(fmsg, x, y, z, b, markercount, a)
			now = time.Now()
		}
		count++
	}

	n := atomic.AddInt64(&totalwrites, int64(x+y+z))

	took := time.Since(epoch).Round(time.Second)
	wg.Done()
	<-fin
	fmsg := "at exit boltWriter {%v,%v,%v (%v) in %v}\n"
	fmt.Printf(fmsg, x, y, z, n, took)
}

func boltGetter(
	db *bolt.DB, klen, vlen, loadn, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var ngets, nmisses int64

	epoch := time.Now()
	defer func() {
		took := time.Since(epoch).Round(time.Second)
		wg.Done()
		<-fin
		fmsg := "at exit, boltGetter %v:%v items in %v\n"
		fmt.Printf(fmsg, ngets, nmisses, took)
	}()

	time.Sleep(time.Duration(rand.Intn(100)+300) * time.Millisecond)

	var key []byte
	g := Generateread(
		int64(options.keylen), loadn, int64(options.inserts), seedl, seedc,
	)

	bname := []byte(bucketname)
	get := func(tx *bolt.Tx) (err error) {
		b := tx.Bucket(bname)
		v := b.Get(key)
		if v == nil {
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
			fmsg := "boltGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}
	}
}

func boltRanger(
	db *bolt.DB, klen, vlen, loadn, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var nranges, nmisses int64

	epoch := time.Now()
	defer func() {
		took := time.Since(epoch).Round(time.Second)
		wg.Done()
		<-fin
		fmsg := "at exit, boltRanger %v:%v items in %v\n"
		fmt.Printf(fmsg, nranges, nmisses, took)
	}()

	time.Sleep(time.Duration(rand.Intn(100)+300) * time.Millisecond)

	var key []byte
	g := Generateread(
		int64(options.keylen), loadn, int64(options.inserts), seedl, seedc,
	)

	bname := []byte(bucketname)
	ranger := func(tx *bolt.Tx) error {
		b := tx.Bucket(bname)
		cur := b.Cursor()
		_, v := cur.Seek(key)
		for i := 0; i < options.limit; i++ {
			nranges++
			if v == nil {
				nmisses++
			}
			_, v = cur.Next()
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
			fmsg := "boltRanger {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nranges, nmisses, y)
			now = time.Now()
		}
	}
}

func boltpath() string {
	path := filepath.Join(os.TempDir(), "bolt.data")
	if err := os.RemoveAll(path); err != nil {
		panic(err)
	}
	return path
}

func getboltCount(db *bolt.DB) (count uint64) {
	err := db.Update(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(bucketname))
		count = uint64(b.Stats().KeyN)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return
}

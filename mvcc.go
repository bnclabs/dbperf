package main

import "io"
import "fmt"
import "sync"
import "time"
import "bytes"
import "strconv"
import "sync/atomic"
import "math/rand"

import "github.com/prataprc/gostore/llrb"
import humanize "github.com/dustin/go-humanize"

func perfmvcc() error {
	setts := llrb.Defaultsettings()
	index := llrb.NewMVCC("dbperf", setts)
	defer index.Destroy()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err := mvccLoad(index, seedl); err != nil {
		return err
	}

	var wg sync.WaitGroup
	n := atomic.LoadInt64(&numentries)
	fin := make(chan struct{})

	if options.inserts+options.upserts+options.deletes > 0 {
		// writer routines
		go mvccWriter(index, n, seedl, seedc, fin, &wg)
		wg.Add(1)
	}
	if options.gets > 0 {
		for i := 0; i < options.cpu; i++ {
			go mvccGetter(index, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	if options.ranges > 0 {
		for i := 0; i < options.cpu; i++ {
			go mvccRanger(index, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	wg.Wait()
	close(fin)
	time.Sleep(1 * time.Second)

	index.Log()
	index.Validate()

	fmsg := "MVCC total indexed %v items, footprint %v\n"
	fmt.Printf(fmsg, index.Count(), humanize.Bytes(uint64(index.Footprint())))

	return nil
}

func mvccLoad(index *llrb.MVCC, seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	value, oldvalue := make([]byte, vlen), make([]byte, vlen)
	if options.vallen <= 0 {
		value, oldvalue = nil, nil
	}
	key, now := make([]byte, klen), time.Now()
	for key, value = g(key, value); key != nil; key, value = g(key, value) {
		index.Set(key, value, oldvalue)
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	fmt.Printf("Loaded %v items in %v\n", index.Count(), time.Since(now))
	return nil
}

var mvccsets = []func(index *llrb.MVCC, key, val, oldval []byte) uint64{
	mvccSet1, mvccSet2, mvccSet3, mvccSet4,
}

func mvccWriter(
	index *llrb.MVCC, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {
	var x, y, z int64

	klen, vlen := int64(options.keylen), int64(options.vallen)
	gcreate := Generatecreate(klen, vlen, n, seedc)
	gupdate := Generateupdate(klen, vlen, n, seedl, seedc, -1)
	gdelete := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	value, oldvalue := make([]byte, vlen), make([]byte, vlen)
	if options.vallen <= 0 {
		value, oldvalue = nil, nil
	}
	key, rnd := make([]byte, klen), rand.New(rand.NewSource(seedl))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	insn, upsn, deln := options.inserts, options.upserts, options.deletes

	for totalops := insn + upsn + deln; totalops > 0; {
		idx := rnd.Intn(totalops)
		switch {
		case idx < insn:
			key, value = gcreate(key, value)
			mvccsets[0](index, key, value, oldvalue)
			atomic.AddInt64(&numentries, 1)
			x = atomic.AddInt64(&ninserts, 1)
			insn--
		case idx < (insn + upsn):
			key, value = gupdate(key, value)
			mvccsets[0](index, key, value, oldvalue)
			y = atomic.AddInt64(&nupserts, 1)
			upsn--
		case idx < (insn + upsn + deln):
			key, value = gdelete(key, value)
			mvccdels[0](index, key, value, options.lsm /*lsm*/)
			atomic.AddInt64(&numentries, -1)
			z = atomic.AddInt64(&ndeletes, 1)
			deln--
		}
		totalops = insn + upsn + deln
		if n := x + y + z; n%markercount == 0 {
			a := time.Since(now).Round(time.Second)
			b := time.Since(epoch).Round(time.Second)
			fmsg := "mvccWriter {%v,%v,%v in %v}, {%v ops %v}\n"
			fmt.Printf(fmsg, x, y, z, b, markercount, a)
			now = time.Now()
		}
	}
	duration := time.Since(epoch)
	wg.Done()
	<-fin
	n = x + y + z
	fmsg := "at exit lmdbWriter {%v,%v,%v (%v) in %v}\n"
	fmt.Printf(fmsg, x, y, z, n, duration)
}

func mvccSet1(index *llrb.MVCC, key, value, oldvalue []byte) uint64 {
	oldvalue, cas := index.Set(key, value, oldvalue)
	//fmt.Printf("update1 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	return cas
}

func mvccSet2(index *llrb.MVCC, key, value, oldvalue []byte) uint64 {
	for i := 2; i >= 0; i-- {
		oldvalue, oldcas, deleted, ok := index.Get(key, oldvalue)
		if deleted || ok == false {
			oldcas = 0
		} else if oldcas == 0 {
			panic(fmt.Errorf("unexpected %v", oldcas))
		} else if bytes.Compare(key, oldvalue) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
		}
		oldvalue, _, _ = index.SetCAS(key, value, oldvalue, oldcas)
	}
	panic("unreachable code")
}

func mvccSet3(index *llrb.MVCC, key, value, oldvalue []byte) uint64 {
	txn := index.BeginTxn(0xC0FFEE)
	oldvalue = txn.Set(key, value, oldvalue)
	//fmt.Printf("update3 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0
}

func mvccSet4(index *llrb.MVCC, key, value, oldvalue []byte) uint64 {
	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	oldvalue = cur.Set(key, value, oldvalue)
	//fmt.Printf("update4 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0
}

var mvccdels = []func(*llrb.MVCC, []byte, []byte, bool) (uint64, bool){
	mvccDel1, mvccDel2, mvccDel3, mvccDel4,
}

func mvccDel1(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	oldvalue, cas := index.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %s", key, oldvalue))
	} else if len(oldvalue) > 0 {
		ok = true
	}
	return cas, ok
}

func mvccDel2(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	oldvalue = txn.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	} else if len(oldvalue) > 0 {
		ok = true
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, ok
}

func mvccDel3(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	oldvalue = cur.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	} else if len(oldvalue) > 0 {
		ok = true
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, ok
}

func mvccDel4(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	curkey, _ := cur.Key()
	if bytes.Compare(key, curkey) == 0 {
		cur.Delcursor(lsm)
		ok = true
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, ok
}

var mvccgets = []func(x *llrb.MVCC, k, v []byte) ([]byte, uint64, bool, bool){
	mvccGet1, mvccGet2, mvccGet3,
}

func mvccGetter(
	index *llrb.MVCC, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var ngets, nmisses int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	value := make([]byte, options.vallen)
	if options.vallen <= 0 {
		value = nil
	}
	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
	for ngets+nmisses < int64(options.gets) {
		ngets++
		key = g(key, atomic.LoadInt64(&ninserts))
		if _, _, _, ok := mvccgets[0](index, key, value); ok == false {
			nmisses++
		}
		if ngm := ngets + nmisses; ngm%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "mvccGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}
	}
	duration := time.Since(epoch)
	wg.Done()
	<-fin
	fmsg := "at exit, mvccGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, duration)
}

func mvccGet1(
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	return index.Get(key, value)
}

func mvccGet2(
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	txn := index.BeginTxn(0xC0FFEE)
	value, _, del, ok := txn.Get(key, value)
	if ok == true {
		cur, err := txn.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		if ckey, cdel := cur.Key(); cdel != del {
			panic(fmt.Errorf("expected %v, got %v", del, cdel))
		} else if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", value, cvalue))
		}
	}
	txn.Abort()
	return value, 0, del, ok
}

func mvccGet3(
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	view := index.View(0x1235)
	value, _, del, ok := view.Get(key, value)
	if ok == true {
		cur, err := view.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		if ckey, cdel := cur.Key(); cdel != del {
			panic(fmt.Errorf("expected %v, got %v", del, cdel))
		} else if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", value, cvalue))
		}
	}
	view.Abort()
	return value, 0, del, ok
}

var mvccrngs = []func(index *llrb.MVCC, key, val []byte) int64{
	mvccRange1, mvccRange2, mvccRange3, mvccRange4,
}

func mvccRanger(
	index *llrb.MVCC, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var nranges int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	value := make([]byte, options.vallen)
	if options.vallen <= 0 {
		value = nil
	}
	epoch := time.Now()
	for nranges < int64(options.ranges) {
		key = g(key, atomic.LoadInt64(&ninserts))
		n := mvccrngs[0](index, key, value)
		nranges += n
	}
	duration := time.Since(epoch)
	wg.Done()
	<-fin
	fmt.Printf("at exit, mvccRanger %v items in %v\n", nranges, duration)
}

func mvccRange1(index *llrb.MVCC, key, value []byte) (n int64) {
	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 100; i++ {
		key, value, del, err := cur.GetNext()
		if err == io.EOF {
		} else if err != nil {
			panic(err)
		} else if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x)%2) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false && bytes.Compare(key, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, value))
		}
		n++
	}
	txn.Abort()
	return
}

func mvccRange2(index *llrb.MVCC, key, value []byte) (n int64) {
	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 100; i++ {
		key, value, _, del, err := cur.YNext(false /*fin*/)
		if err == io.EOF {
			continue
		} else if err != nil {
			panic(err)
		}
		if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x)%2) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false && bytes.Compare(key, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, value))
		}
		n++
	}
	txn.Abort()
	return
}

func mvccRange3(index *llrb.MVCC, key, value []byte) (n int64) {
	view := index.View(0x1236)
	cur, err := view.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 100; i++ {
		key, value, del, err := cur.GetNext()
		if err == io.EOF {
			continue
		} else if err != nil {
			panic(err)
		}
		if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x)%2) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false && bytes.Compare(key, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, value))
		}
		n++
	}
	view.Abort()
	return
}

func mvccRange4(index *llrb.MVCC, key, value []byte) (n int64) {
	view := index.View(0x1237)
	cur, err := view.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 100; i++ {
		key, value, _, del, err := cur.YNext(false /*fin*/)
		if err == io.EOF {
			continue
		} else if err != nil {
			panic(err)
		}
		if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x)%2) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false && bytes.Compare(key, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, value))
		}
		n++
	}
	view.Abort()
	return
}

package main

import "io"
import "fmt"
import "sync"
import "time"
import "bytes"
import "runtime"
import "strconv"
import "sync/atomic"
import "math/rand"

import "github.com/prataprc/gostore/llrb"

func perfllrb() error {
	setts := llrb.Defaultsettings()
	index := llrb.NewLLRB("dbtest", setts)
	defer index.Destroy()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err := llrbLoad(index, seedl); err != nil {
		return err
	}

	var wg sync.WaitGroup
	n := atomic.LoadInt64(&numentries)
	fin := make(chan struct{})

	if options.inserts+options.upserts+options.deletes > 0 {
		// writer routines
		go llrbWriter(index, n, seedl, seedc, fin, &wg)
		wg.Add(1)
	}
	if options.gets > 0 {
		for i := 0; i < runtime.GOMAXPROCS(-1); i++ {
			go llrbGetter(index, n, seedl, seedc, fin, &wg)
			wg.Add(1)
			break
		}
	}
	if options.iterates > 0 {
		for i := 0; i < runtime.GOMAXPROCS(-1); i++ {
			go llrbRanger(index, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	wg.Wait()
	close(fin)
	time.Sleep(1 * time.Second)

	fmt.Printf("LLRB total indexed %v items\n", index.Count())

	return nil
}

func llrbLoad(index *llrb.LLRB, seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	key, value := make([]byte, 16), make([]byte, 16)
	now, oldvalue := time.Now(), make([]byte, 16)
	for key, value = g(key, value); key != nil; key, value = g(key, value) {
		index.Set(key, value, oldvalue)
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	fmt.Printf("Loaded %v items in %v\n", index.Count(), time.Since(now))
	return nil
}

var llrbsets = []func(index *llrb.LLRB, key, val, oldval []byte) uint64{
	llrbSet1, llrbSet2, llrbSet3, llrbSet4,
}

func llrbWriter(
	index *llrb.LLRB, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {
	var x, y, z int64

	klen, vlen := int64(options.keylen), int64(options.vallen)
	gcreate := Generatecreate(klen, vlen, n, seedc)
	gupdate := Generateupdate(klen, vlen, n, seedl, seedc, -1)
	gdelete := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	key, value, oldvalue := make([]byte, 16), make([]byte, 16), make([]byte, 16)
	rnd := rand.New(rand.NewSource(seedl))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	insn, upsn, deln := options.inserts, options.upserts, options.deletes

	for totalops := insn + upsn + deln; totalops > 0; {
		idx := rnd.Intn(totalops)
		switch {
		case idx < insn:
			key, value = gcreate(key, value)
			llrbsets[0](index, key, value, oldvalue)
			atomic.AddInt64(&numentries, 1)
			x = atomic.AddInt64(&ninserts, 1)
			insn--
		case idx < upsn:
			key, value = gupdate(key, value)
			llrbsets[0](index, key, value, oldvalue)
			y = atomic.AddInt64(&nupserts, 1)
			upsn--
		case idx < deln:
			key, value = gdelete(key, value)
			llrbdels[0](index, key, value, false /*lsm*/)
			atomic.AddInt64(&numentries, -1)
			z = atomic.AddInt64(&ndeletes, 1)
			deln--
		}
		totalops = insn + upsn + deln
		if n := x + y + z; n%markercount == 0 {
			a := time.Since(now).Round(time.Second)
			b := time.Since(epoch).Round(time.Second)
			fmsg := "llrbWriter {%v,%v,%v in %v}, {%v ops %v}\n"
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

func llrbSet1(index *llrb.LLRB, key, value, oldvalue []byte) uint64 {
	oldvalue, cas := index.Set(key, value, oldvalue)
	//fmt.Printf("update1 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	return cas
}

func llrbSet2(index *llrb.LLRB, key, value, oldvalue []byte) uint64 {
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

func llrbSet3(index *llrb.LLRB, key, value, oldvalue []byte) uint64 {
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

func llrbSet4(index *llrb.LLRB, key, value, oldvalue []byte) uint64 {
	txn := index.BeginTxn(0xC0FFEE)
	cur := txn.OpenCursor(key)
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

var llrbdels = []func(*llrb.LLRB, []byte, []byte, bool) (uint64, bool){
	llrbDel1, llrbDel2, llrbDel3, llrbDel4,
}

func llrbDel1(index *llrb.LLRB, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	oldvalue, cas := index.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %s", key, oldvalue))
	} else if len(oldvalue) > 0 {
		ok = true
	}
	return cas, ok
}

func llrbDel2(index *llrb.LLRB, key, oldvalue []byte, lsm bool) (uint64, bool) {
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

func llrbDel3(index *llrb.LLRB, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	cur := txn.OpenCursor(key)
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

func llrbDel4(index *llrb.LLRB, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	cur := txn.OpenCursor(key)
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

var llrbgets = []func(x *llrb.LLRB, k, v []byte) ([]byte, uint64, bool, bool){
	llrbGet1, llrbGet2, llrbGet3,
}

func llrbGetter(
	index *llrb.LLRB, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var ngets, nmisses int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
	value := make([]byte, 16)
	for ngets+nmisses < int64(options.gets) {
		ngets++
		key = g(key, atomic.LoadInt64(&ninserts))
		if _, _, _, ok := llrbgets[0](index, key, value); ok == false {
			nmisses++
		}
		if ngm := ngets + nmisses; ngm%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "llrbGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
		}
	}
	duration := time.Since(epoch)
	wg.Done()
	<-fin
	fmsg := "at exit, llrbGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, duration)
}

func llrbGet1(
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	//fmt.Printf("llrbGet1 %q\n", key)
	//defer fmt.Printf("llrbGet1-abort %q\n", key)
	return index.Get(key, value)
}

func llrbGet2(
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	//fmt.Printf("llrbGet2\n")
	txn := index.BeginTxn(0xC0FFEE)
	value, del, ok := txn.Get(key, value)
	if ok == true {
		cur := txn.OpenCursor(key)
		if ckey, cdel := cur.Key(); cdel != del {
			panic(fmt.Errorf("expected %v, got %v", del, cdel))
		} else if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", value, cvalue))
		}
	}
	//fmt.Printf("llrbGet2-abort\n")
	txn.Abort()
	return value, 0, del, ok
}

func llrbGet3(
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	view := index.View(0x1235)
	value, del, ok := view.Get(key, value)
	if ok == true {
		cur := view.OpenCursor(key)
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

var llrbrngs = []func(index *llrb.LLRB, key, val []byte) int64{
	llrbRange1, llrbRange2, llrbRange3, llrbRange4,
}

func llrbRanger(
	index *llrb.LLRB, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var nranges int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	epoch, value := time.Now(), make([]byte, 16)
	for nranges < int64(options.iterates) {
		key = g(key, atomic.LoadInt64(&ninserts))
		n := llrbrngs[0](index, key, value)
		nranges += n
	}
	duration := time.Since(epoch)
	wg.Done()
	<-fin
	fmt.Printf("at exit, llrbRanger %v items in %v\n", nranges, duration)
}

func llrbRange1(index *llrb.LLRB, key, value []byte) (n int64) {
	//fmt.Printf("llrbRange1 %q\n", key)
	txn := index.BeginTxn(0xC0FFEE)
	cur := txn.OpenCursor(key)
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

func llrbRange2(index *llrb.LLRB, key, value []byte) (n int64) {
	txn := index.BeginTxn(0xC0FFEE)
	cur := txn.OpenCursor(key)
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

func llrbRange3(index *llrb.LLRB, key, value []byte) (n int64) {
	view := index.View(0x1236)
	cur := view.OpenCursor(key)
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

func llrbRange4(index *llrb.LLRB, key, value []byte) (n int64) {
	view := index.View(0x1237)
	cur := view.OpenCursor(key)
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

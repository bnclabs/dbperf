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

func perfllrb() error {
	setts := llrb.Defaultsettings()
	index := llrb.NewLLRB("dbperf", setts)
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
		// writer routine
		go llrbWriter(index, n, seedl, seedc, fin, &wg)
		wg.Add(1)
	}
	if options.gets > 0 {
		for i := 0; i < options.cpu; i++ {
			go llrbGetter(index, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	if options.ranges > 0 {
		for i := 0; i < options.cpu; i++ {
			go llrbRanger(index, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	wg.Wait()
	close(fin)
	time.Sleep(1 * time.Second)

	index.Log()
	index.Validate()

	fmsg := "LLRB total indexed %v items, footprint %v\n"
	fmt.Printf(fmsg, index.Count(), humanize.Bytes(uint64(index.Footprint())))

	return nil
}

func llrbLoad(index *llrb.LLRB, seedl int64) error {
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

	took := time.Since(now).Round(time.Second)
	fmt.Printf("Loaded %v items in %v\n", index.Count(), took)
	return nil
}

type llrbsetfn = func(*llrb.LLRB, []byte, []byte, []byte) uint64

var llrbsets = map[string][]llrbsetfn{
	"set": []llrbsetfn{llrbSet1},
	"cas": []llrbsetfn{llrbSet2},
	"txn": []llrbsetfn{llrbSet3},
	"cur": []llrbsetfn{llrbSet4},
	"all": []llrbsetfn{llrbSet1, llrbSet2, llrbSet3, llrbSet4},
}

func llrbWriter(
	index *llrb.LLRB, n, seedl, seedc int64,
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

	as, bs := llrbsets[options.setas], llrbdels[options.delas]
	for totalops := insn + upsn + deln; totalops > 0; {
		llrbset := as[rnd.Intn(len(as))]
		llrbdel := bs[rnd.Intn(len(bs))]

		idx := rnd.Intn(totalops)
		switch {
		case idx < insn:
			key, value = gcreate(key, value)
			llrbset(index, key, value, oldvalue)
			atomic.AddInt64(&numentries, 1)
			x = atomic.AddInt64(&ninserts, 1)
			insn--
		case idx < (insn + upsn):
			key, value = gupdate(key, value)
			llrbset(index, key, value, oldvalue)
			y = atomic.AddInt64(&nupserts, 1)
			upsn--
		case idx < (insn + upsn + deln):
			key, value = gdelete(key, value)
			llrbdel(index, key, value, options.lsm)
			atomic.AddInt64(&numentries, -1)
			z = atomic.AddInt64(&ndeletes, 1)
			deln--
		}
		totalops = insn + upsn + deln
		if n := x + y + z; n > 0 && n%markercount == 0 {
			a := time.Since(now).Round(time.Second)
			b := time.Since(epoch).Round(time.Second)
			fmsg := "llrbWriter {%v,%v,%v in %v}, {%v ops %v}\n"
			fmt.Printf(fmsg, x, y, z, b, markercount, a)
			now = time.Now()
		}
	}
	took := time.Since(epoch).Round(time.Second)
	wg.Done()
	<-fin
	n = x + y + z
	fmsg := "at exit llrbWriter {%v,%v,%v (%v) in %v}\n"
	fmt.Printf(fmsg, x, y, z, n, took)
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
	var cas uint64

	oldvalue, oldcas, deleted, ok := index.Get(key, oldvalue)
	if deleted || ok == false {
		oldcas = 0
	} else if oldcas == 0 {
		panic(fmt.Errorf("unexpected %v", oldcas))
	} else if bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	oldvalue, cas, _ = index.SetCAS(key, value, oldvalue, oldcas)
	return cas
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

type llrbdelfn = func(*llrb.LLRB, []byte, []byte, bool) (uint64, bool)

var llrbdels = map[string][]llrbdelfn{
	"del":    []llrbdelfn{llrbDel1},
	"txn":    []llrbdelfn{llrbDel2},
	"cur":    []llrbdelfn{llrbDel3},
	"delcur": []llrbdelfn{llrbDel4},
	"all":    []llrbdelfn{llrbDel1, llrbDel2, llrbDel3, llrbDel4},
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

func llrbDel4(index *llrb.LLRB, key, oldvalue []byte, lsm bool) (uint64, bool) {
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

type llrbgetfn = func(*llrb.LLRB, []byte, []byte) ([]byte, uint64, bool, bool)

var llrbgets = map[string][]llrbgetfn{
	"get":  []llrbgetfn{llrbGet1},
	"txn":  []llrbgetfn{llrbGet2},
	"view": []llrbgetfn{llrbGet3},
	"all":  []llrbgetfn{llrbGet1, llrbGet2, llrbGet3},
}

func llrbGetter(
	index *llrb.LLRB, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var ngets, nmisses int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(seedl))
	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
	value := make([]byte, options.vallen)
	if options.vallen <= 0 {
		value = nil
	}

	cs := llrbgets[options.getas]
	for ngets+nmisses < int64(options.gets) {
		llrbget := cs[rnd.Intn(len(cs))]

		ngets++
		key = g(key, atomic.LoadInt64(&ninserts))
		if _, _, _, ok := llrbget(index, key, value); ok == false {
			nmisses++
		}
		if ngm := ngets + nmisses; ngm%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "llrbGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}
	}
	took := time.Since(epoch).Round(time.Second)
	wg.Done()
	<-fin
	fmsg := "at exit, llrbGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, took)
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
	//fmt.Printf("llrbGet2-abort\n")
	txn.Abort()
	return value, 0, del, ok
}

func llrbGet3(
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

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

type llrbrngfn = func(*llrb.LLRB, []byte, []byte) int64

var llrbrngs = map[string][]llrbrngfn{
	"tgn": []llrbrngfn{llrbRange1},
	"tyn": []llrbrngfn{llrbRange2},
	"vgn": []llrbrngfn{llrbRange3},
	"vyn": []llrbrngfn{llrbRange4},
	"all": []llrbrngfn{llrbRange1, llrbRange2, llrbRange3, llrbRange4},
}

func llrbRanger(
	index *llrb.LLRB, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var nranges int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(seedl))
	epoch, value := time.Now(), make([]byte, options.vallen)
	if options.vallen <= 0 {
		value = nil
	}

	ds := llrbrngs[options.rngas]
	for nranges < int64(options.ranges) {
		llrbrng := ds[rnd.Intn(len(ds))]

		key = g(key, atomic.LoadInt64(&ninserts))
		n := llrbrng(index, key, value)
		nranges += n
	}
	took := time.Since(epoch).Round(time.Second)
	wg.Done()
	<-fin
	fmt.Printf("at exit, llrbRanger %v items in %v\n", nranges, took)
}

func llrbRange1(index *llrb.LLRB, key, value []byte) (n int64) {
	//fmt.Printf("llrbRange1 %q\n", key)
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

func llrbRange2(index *llrb.LLRB, key, value []byte) (n int64) {
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

func llrbRange3(index *llrb.LLRB, key, value []byte) (n int64) {
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

func llrbRange4(index *llrb.LLRB, key, value []byte) (n int64) {
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

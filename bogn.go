package main

import "io"
import "fmt"
import "sync"
import "time"
import "bytes"
import "strconv"
import "sync/atomic"
import "math/rand"

import "github.com/prataprc/gostore/bogn"
import "github.com/prataprc/gostore/api"
import s "github.com/prataprc/gosettings"

//import humanize "github.com/dustin/go-humanize" TODO

func perfbogn() error {
	setts := bognsettings(options.seed)
	bogn.PurgeIndex("dbperf", setts)

	index, err := bogn.New("dbperf", setts)
	if err != nil {
		panic(err)
	}
	defer index.Destroy()
	defer index.Close()
	index.Start()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err := bognLoad(index, seedl); err != nil {
		return err
	}

	var wg sync.WaitGroup
	fin := make(chan struct{})

	n := atomic.LoadInt64(&numentries)

	if options.inserts+options.upserts+options.deletes > 0 {
		// writer routines
		go bognWriter(index, n, seedl, seedc, fin, &wg)
		wg.Add(1)
	}
	if options.gets > 0 {
		for i := 0; i < options.cpu; i++ {
			go bognGetter(index, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	if options.ranges > 0 {
		for i := 0; i < options.cpu; i++ {
			go bognRanger(index, n, seedl, seedc, fin, &wg)
			wg.Add(1)
		}
	}
	wg.Wait()
	close(fin)
	time.Sleep(1 * time.Second)

	index.Log()
	index.Validate()

	fmt.Printf("Number of ROLLBACKS: %v\n", rollbacks)
	//TODO:
	//fmsg := "BOGN total indexed %v items, footprint %v\n"
	//fmt.Printf(fmsg, index.Count(), humanize.Bytes(uint64(index.Footprint())))

	return nil
}

func bognLoad(index *bogn.Bogn, seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	now := time.Now()

	value, oldvalue := make([]byte, vlen), make([]byte, vlen)
	if options.vallen <= 0 {
		value, oldvalue = nil, nil
	}
	key := make([]byte, klen)
	for key, value = g(key, value); key != nil; key, value = g(key, value) {
		//fmt.Printf("load %s %s\n", key, value)
		index.Set(key, value, oldvalue)
	}
	n := atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	fmt.Printf("Loaded %v items in %v\n", n, time.Since(now))
	return nil
}

type bognsetfn = func(*bogn.Bogn, []byte, []byte, []byte) uint64

var bognsets = map[string][]bognsetfn{
	"set": []bognsetfn{bognSet1},
	"cas": []bognsetfn{bognSet2},
	"txn": []bognsetfn{bognSet3},
	"cur": []bognsetfn{bognSet4},
	"all": []bognsetfn{bognSet1, bognSet2, bognSet3, bognSet4},
}

func bognWriter(
	index *bogn.Bogn, n, seedl, seedc int64,
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

	as, bs := bognsets[options.setas], bogndels[options.delas]
	for totalops := insn + upsn + deln; totalops > 0; {
		bognset := as[rnd.Intn(len(as))]
		bogndel := bs[rnd.Intn(len(bs))]

		idx := rnd.Intn(totalops)
		switch {
		case idx < insn:
			key, value = gcreate(key, value)
			//fmt.Printf("create %s %s\n", key, value)
			bognset(index, key, value, oldvalue)
			atomic.AddInt64(&numentries, 1)
			x = atomic.AddInt64(&ninserts, 1)
			insn--
		case idx < (insn + upsn):
			key, value = gupdate(key, value)
			//fmt.Printf("update %s %s\n", key, value)
			bognset(index, key, value, oldvalue)
			y = atomic.AddInt64(&nupserts, 1)
			upsn--
		case idx < (insn + upsn + deln):
			key, value = gdelete(key, value)
			//fmt.Printf("delete %s %s\n", key, value)
			bogndel(index, key, value, options.lsm /*lsm*/)
			atomic.AddInt64(&numentries, -1)
			z = atomic.AddInt64(&ndeletes, 1)
			deln--
		}
		totalops = insn + upsn + deln
		if n := x + y + z; n%markercount == 0 {
			a := time.Since(now).Round(time.Second)
			b := time.Since(epoch).Round(time.Second)
			fmsg := "bognWriter {%v,%v,%v in %v}, {%v ops %v}\n"
			fmt.Printf(fmsg, x, y, z, b, markercount, a)
			now = time.Now()
		}
	}
	duration := time.Since(epoch)
	wg.Done()
	<-fin
	n = x + y + z
	fmsg := "at exit bognWriter {%v,%v,%v (%v) in %v}\n"
	fmt.Printf(fmsg, x, y, z, n, duration)
}

func bognSet1(index *bogn.Bogn, key, value, oldvalue []byte) uint64 {
	oldvalue, cas := index.Set(key, value, oldvalue)
	//fmt.Printf("update1 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	return cas
}

func bognSet2(index *bogn.Bogn, key, value, oldvalue []byte) uint64 {
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

func bognSet3(index *bogn.Bogn, key, value, oldvalue []byte) uint64 {
	txn := index.BeginTxn(0xC0FFEE)
	oldvalue = txn.Set(key, value, oldvalue)
	//fmt.Printf("update3 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	err := txn.Commit()
	if err != nil && err.Error() == api.ErrorRollback.Error() {
		atomic.AddInt64(&rollbacks, 1)
	}
	return 0
}

func bognSet4(index *bogn.Bogn, key, value, oldvalue []byte) uint64 {
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
	err = txn.Commit()
	if err != nil && err.Error() == api.ErrorRollback.Error() {
		atomic.AddInt64(&rollbacks, 1)
	}
	return 0
}

type bogndelfn = func(*bogn.Bogn, []byte, []byte, bool) (uint64, bool)

var bogndels = map[string][]bogndelfn{
	"del":    []bogndelfn{bognDel1},
	"txn":    []bogndelfn{bognDel2},
	"cur":    []bogndelfn{bognDel3},
	"delcur": []bogndelfn{bognDel4},
	"all":    []bogndelfn{bognDel1, bognDel2, bognDel3, bognDel4},
}

func bognDel1(index *bogn.Bogn, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	oldvalue, cas := index.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %s", key, oldvalue))
	} else if len(oldvalue) > 0 {
		ok = true
	}
	return cas, ok
}

func bognDel2(index *bogn.Bogn, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	oldvalue = txn.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	} else if len(oldvalue) > 0 {
		ok = true
	}
	err := txn.Commit()
	if err != nil && err.Error() == api.ErrorRollback.Error() {
		atomic.AddInt64(&rollbacks, 1)
	}
	return 0, ok
}

func bognDel3(index *bogn.Bogn, key, oldvalue []byte, lsm bool) (uint64, bool) {
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
	err = txn.Commit()
	if err != nil && err.Error() == api.ErrorRollback.Error() {
		atomic.AddInt64(&rollbacks, 1)
	}
	return 0, ok
}

func bognDel4(index *bogn.Bogn, key, oldvalue []byte, lsm bool) (uint64, bool) {
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
	err = txn.Commit()
	if err != nil && err.Error() == api.ErrorRollback.Error() {
		atomic.AddInt64(&rollbacks, 1)
	}
	return 0, ok
}

type bogngetfn = func(*bogn.Bogn, []byte, []byte) ([]byte, uint64, bool, bool)

var bogngets = map[string][]bogngetfn{
	"get":  []bogngetfn{bognGet1},
	"txn":  []bogngetfn{bognGet2},
	"view": []bogngetfn{bognGet3},
	"all":  []bogngetfn{bognGet1, bognGet2, bognGet3},
}

func bognGetter(
	index *bogn.Bogn, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var ngets, nmisses int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(int64(seedl)))
	value := make([]byte, options.vallen)
	if options.vallen <= 0 {
		value = nil
	}

	cs := bogngets[options.getas]
	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
	for ngets+nmisses < int64(options.gets) {
		bognget := cs[rnd.Intn(len(cs))]

		ngets++
		key = g(key, atomic.LoadInt64(&ninserts))
		if _, _, _, ok := bognget(index, key, value); ok == false {
			nmisses++
		}
		if ngm := ngets + nmisses; ngm%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "bognGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}
	}
	duration := time.Since(epoch)
	wg.Done()
	<-fin
	fmsg := "at exit, bognGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, duration)
}

func bognGet1(
	index *bogn.Bogn, key, value []byte) ([]byte, uint64, bool, bool) {

	return index.Get(key, value)
}

func bognGet2(
	index *bogn.Bogn, key, value []byte) ([]byte, uint64, bool, bool) {

	txn := index.BeginTxn(0xC0FFEE)
	value, _, del, ok := txn.Get(key, value)
	if ok == true {
		cur, err := txn.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		ckey, cdel := cur.Key()
		if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, value) != 0 {
			panic(fmt.Errorf("key %q expected %q, got %q", key, value, cvalue))
		} else if cdel != del {
			panic(fmt.Errorf("key %q expected %v, got %v", key, del, cdel))
		}
	}
	txn.Abort()
	return value, 0, del, ok
}

func bognGet3(
	index *bogn.Bogn, key, value []byte) ([]byte, uint64, bool, bool) {

	view := index.View(0x1235)
	value, _, del, ok := view.Get(key, value)
	if ok == true {
		cur, err := view.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		ckey, cdel := cur.Key()
		if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, value) != 0 {
			panic(fmt.Errorf("key %s expected %q, got %q", key, value, cvalue))
		} else if cdel != del {
			panic(fmt.Errorf("key %s expected %v, got %v", key, del, cdel))
		}
	}
	view.Abort()
	return value, 0, del, ok
}

type bognrngfn = func(*bogn.Bogn, []byte, []byte) int64

var bognrngs = map[string][]bognrngfn{
	"tgn": []bognrngfn{bognRange1},
	"tyn": []bognrngfn{bognRange2},
	"vgn": []bognrngfn{bognRange3},
	"vyn": []bognrngfn{bognRange4},
	"all": []bognrngfn{bognRange1, bognRange2, bognRange3, bognRange4},
}

func bognRanger(
	index *bogn.Bogn, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	var nranges int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(int64(seedl)))
	value := make([]byte, options.vallen)
	if options.vallen <= 0 {
		value = nil
	}

	ds, epoch := bognrngs[options.rngas], time.Now()
	for nranges < int64(options.ranges) {
		bognrng := ds[rnd.Intn(len(ds))]

		key = g(key, atomic.LoadInt64(&ninserts))
		n := bognrng(index, key, value)
		nranges += n
	}
	duration := time.Since(epoch)
	wg.Done()
	<-fin
	fmt.Printf("at exit, bognRanger %v items in %v\n", nranges, duration)
}

func bognRange1(index *bogn.Bogn, key, value []byte) (n int64) {
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

func bognRange2(index *bogn.Bogn, key, value []byte) (n int64) {
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

func bognRange3(index *bogn.Bogn, key, value []byte) (n int64) {
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

func bognRange4(index *bogn.Bogn, key, value []byte) (n int64) {
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

func bognsettings(seed int) s.Settings {
	rnd := rand.New(rand.NewSource(int64(seed)))
	setts := bogn.Defaultsettings()
	setts["memstore"] = options.memstore
	setts["period"] = int64(options.period)
	ratio := []float64{.5, .33, .25, .20, .16, .125, .1}[rnd.Intn(10000)%7]
	setts["ratio"] = ratio
	setts["bubt.mmap"] = []bool{true, false}[rnd.Intn(10000)%2]
	setts["bubt.msize"] = []int64{4096, 8192, 12288}[rnd.Intn(10000)%3]
	setts["bubt.zsize"] = []int64{4096, 8192, 12288}[rnd.Intn(10000)%3]
	//setts["llrb.memcapacity"] = 10 * 1024 * 1024 * 1024
	setts["llrb.allocator"] = "flist"
	setts["llrb.snapshottick"] = []int64{4, 8, 16, 32}[rnd.Intn(10000)%4]
	switch options.bogn {
	case "inmem":
		setts["durable"] = false
		setts["dgm"] = false
		setts["workingset"] = false
	case "durable":
		setts["durable"] = true
		setts["dgm"] = false
		setts["workingset"] = false
	case "dgm":
		setts["durable"] = true
		setts["dgm"] = true
		setts["workingset"] = false
	case "workset":
		setts["durable"] = true
		setts["dgm"] = true
		setts["workingset"] = true
	}

	a, b, c := setts["durable"], setts["dgm"], setts["workingset"]
	fmt.Printf("durable:%v dgm:%v workingset:%v\n", a, b, c)
	a, b = setts["ratio"], setts["period"]
	fmt.Printf("ratio:%v period:%v lsm:%v\n", a, b, options.lsm)
	a = setts["llrb.snapshottick"]
	fmt.Printf("llrb snapshottick:%v\n", a)
	a, b = setts["bubt.diskpaths"], setts["bubt.msize"]
	c, d := setts["bubt.zsize"], setts["bubt.mmap"]
	fmt.Printf("bubt diskpaths:%v msize:%v zsize:%v mmap:%v\n", a, b, c, d)

	return setts
}

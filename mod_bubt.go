package main

import "io"
import "os"
import "fmt"
import "sync"
import "time"
import "bytes"
import "strconv"
import "path/filepath"
import "math/rand"

import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/bubt"

func perfbubt() error {
	path, paths := os.TempDir(), []string{}
	for i, base := range []string{"1", "2", "3"} {
		paths = append(paths, filepath.Join(path, base))
		fmt.Printf("Path %v %q\n", i, filepath.Join(path, base))
	}

	name := "dbperf"
	rnd := rand.New(rand.NewSource(int64(options.seed)))
	msize := int64(4096 * (rnd.Intn(5) + 1))
	zsize := int64(4096 * (rnd.Intn(5) + 1))
	mmap := []bool{true, false}[rnd.Intn(10000)%2]
	bt, err := bubt.NewBubt(name, paths, msize, zsize)
	if err != nil {
		panic(err)
	}

	klen, vlen := int64(options.keylen), int64(options.keylen)
	seed, n := int64(options.seed), int64(options.load)
	iter := makeiterator(klen, vlen, n, delmod)
	md := generatemeta(seed)

	fmsg := "msize: %v zsize:%v mmap:%v mdsize:%v\n"
	fmt.Printf(fmsg, msize, zsize, mmap, len(md))

	now := time.Now()
	bt.Build(iter, md)
	took := time.Since(now).Round(time.Second)
	fmt.Printf("Took %v to build %v entries\n", took, n)
	bt.Close()
	iter(true /*fin*/)

	index, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		panic(err)
	}
	defer index.Destroy()
	defer index.Close()

	if index.Count() != n {
		panic(fmt.Errorf("expected %v, got %v", n, index.Count()))
	} else if index.ID() != name {
		panic(fmt.Errorf("expected %v, got %v", name, index.ID()))
	}

	var rwg sync.WaitGroup
	finch := make(chan struct{})
	if options.gets > 0 {
		for i := 0; i < options.cpu; i++ {
			go bubtGetter(index, n, seed, finch, &rwg)
			rwg.Add(1)
		}
	}
	if options.ranges > 0 {
		for i := 0; i < options.cpu; i++ {
			go bubtRanger(index, n, seed, finch, &rwg)
			rwg.Add(1)
		}
	}
	rwg.Wait()
	close(finch)
	time.Sleep(1 * time.Second)

	index.Log()
	index.Validate()

	fmsg = "BUBT total indexed %v items, footprint %v\n"
	fmt.Printf(fmsg, index.Count(), index.Footprint())

	return nil
}

type bubtgetfn = func(
	*bubt.Snapshot, []byte, []byte) ([]byte, uint64, bool, bool)

var bubtgets = map[string][]bubtgetfn{
	"get":  []bubtgetfn{bubtGet1},
	"view": []bubtgetfn{bubtGet2},
	"all":  []bubtgetfn{bubtGet1, bubtGet2},
}

func bubtGetter(
	index *bubt.Snapshot, n, seed int64, finch chan struct{},
	wg *sync.WaitGroup) {

	var ngets, nmisses int64
	var key []byte
	g := Generatereadseq(int64(options.keylen), n, seed)

	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
	value := make([]byte, options.vallen)
	rnd := rand.New(rand.NewSource(seed))

	cs := bubtgets[options.getas]
	bubtget := cs[rnd.Intn(len(cs))]

loop:
	for {
		ngets++
		key = g(key, 0)
		_, _, _, ok := bubtget(index, key, value)
		if !ok {
			nmisses++
		}

		ngm := (ngets + nmisses)
		if ngm%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "bubtGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}

		if ngm > int64(options.gets) {
			break loop
		}
	}
	took := time.Since(epoch).Round(time.Second)
	wg.Done()
	<-finch
	fmsg := "at exit, bubtGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, took)
}

func bubtGet1(
	index *bubt.Snapshot, key, value []byte) ([]byte, uint64, bool, bool) {

	return index.Get(key, value)
}

func bubtGet2(
	index *bubt.Snapshot, key, value []byte) ([]byte, uint64, bool, bool) {

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

type bubtrngfn = func(*bubt.Snapshot, []byte, []byte) int64

var bubtrngs = map[string][]bubtrngfn{
	"vgn": []bubtrngfn{bubtRange1},
	"vyn": []bubtrngfn{bubtRange2},
	"all": []bubtrngfn{bubtRange1, bubtRange2},
}

func bubtRanger(
	index *bubt.Snapshot, n, seed int64, finch chan struct{},
	wg *sync.WaitGroup) {

	var nranges int64
	var key []byte
	g := Generatereadseq(int64(options.keylen), n, seed)

	rnd := rand.New(rand.NewSource(seed))
	epoch, value := time.Now(), make([]byte, options.vallen)

	ds := bubtrngs[options.rngas]
	bubtrng := ds[rnd.Intn(len(ds))]

loop:
	for {
		key = g(key, 0)
		n := bubtrng(index, key, value)
		nranges += n

		if nranges > int64(options.ranges) {
			break loop
		}
	}
	took := time.Since(epoch).Round(time.Second)
	wg.Done()
	<-finch
	fmt.Printf("at exit, bubtRanger %v items in %v\n", nranges, took)
}

func bubtRange1(index *bubt.Snapshot, key, value []byte) (n int64) {
	//fmt.Printf("bubtRange1 %q\n", key)
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

func bubtRange2(index *bubt.Snapshot, key, value []byte) (n int64) {
	//fmt.Printf("bubtRange2 %q\n", key)
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

func makeiterator(klen, vlen, entries, mod int64) api.Iterator {
	g := Generateloads(klen, vlen, entries)
	key, seqno := make([]byte, options.keylen), uint64(0)
	value := make([]byte, options.vallen)

	return func(fin bool) ([]byte, []byte, uint64, bool, error) {
		key, value = g(key, value)
		if key != nil {
			seqno++
			x, _ := strconv.Atoi(Bytes2str(key))
			deleted := false
			if (int64(x) % 2) == mod {
				deleted = true
			}
			//fmt.Printf("iterate %q %q %v %v\n", key, value, seqno, deleted)
			return key, value, seqno, deleted, nil
		}
		return nil, nil, 0, false, io.EOF
	}
}

func generatemeta(seed int64) []byte {
	rnd := rand.New(rand.NewSource(seed))
	md := make([]byte, rnd.Intn(20000))
	for i := range md {
		md[i] = byte(97 + rnd.Intn(26))
	}
	return md
}

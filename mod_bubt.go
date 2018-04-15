package main

import "io"
import "os"
import "fmt"
import "sync"
import "time"
import "strconv"
import "path/filepath"
import "math/rand"

import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/bubt"
import humanize "github.com/dustin/go-humanize"

func perfbubt() error {
	paths := bubtpaths(options.npaths)

	name := "dbperf"
	//rnd := rand.New(rand.NewSource(int64(options.seed)))
	msize, zsize := int64(options.msize), int64(options.zsize)
	vsize, mmap := int64(options.vsize), options.mmap
	bt, err := bubt.NewBubt(name, paths, msize, zsize, vsize)
	if err != nil {
		panic(err)
	}

	klen, vlen := int64(options.keylen), int64(options.vallen)
	seed, n := int64(options.seed), int64(options.load)
	iter := makeiterator(klen, vlen, n, delmod)
	md := generatemeta(seed)

	fmsg := "msize: %v zsize:%v vsize: %v mmap:%v mdsize:%v\n"
	fmt.Printf(fmsg, msize, zsize, vsize, mmap, len(md))

	now := time.Now()
	bt.Build(iter, md)
	took := time.Since(now).Round(time.Second)
	bt.Close()
	iter(true /*fin*/)

	index, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		panic(err)
	}
	defer index.Destroy()
	defer index.Close()

	fmsg = "Took %v to build %v entries with footprint %v\n"
	fmt.Printf(fmsg, took, n, humanize.Bytes(uint64(index.Footprint())))

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
	//index.Validate()

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
		_, _, _, err := cur.GetNext()
		if err == io.EOF {
			continue
		} else if err != nil {
			panic(err)
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
		_, _, _, _, err := cur.YNext(false /*fin*/)
		if err == io.EOF {
			continue
		} else if err != nil {
			panic(err)
		}
		n++
	}
	view.Abort()
	return
}

func makeiterator(klen, vlen, entries, mod int64) api.EntryIterator {
	g := Generateloads(klen, vlen, entries)
	entry := &indexentry{
		key: make([]byte, 0, 16), value: make([]byte, 0, 16),
		seqno: 0, deleted: false, err: nil,
	}

	return func(fin bool) api.IndexEntry {
		entry.key, entry.value = g(entry.key, entry.value)
		if entry.key != nil {
			entry.seqno += 1
			x, _ := strconv.Atoi(Bytes2str(entry.key))
			entry.deleted = false
			if (int64(x) % 2) == mod {
				entry.deleted = true
			}
			entry.err = nil
			//fmt.Printf("iterate %q %q %v %v\n", key, value, seqno, deleted)
			return entry
		}
		entry.key, entry.value = nil, nil
		entry.seqno, entry.deleted, entry.err = 0, false, io.EOF
		return entry
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

func bubtpaths(npaths int) []string {
	path, paths := os.TempDir(), []string{}
	for i := 0; i < npaths; i++ {
		base := fmt.Sprintf("%v", i+1)
		path := filepath.Join(path, base)
		paths = append(paths, path)
		fmt.Printf("Path %v %q\n", i+1, path)
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
		if err := os.MkdirAll(path, 0755); err != nil {
			panic(err)
		}
	}
	return paths
}

type indexentry struct {
	key     []byte
	value   []byte
	seqno   uint64
	deleted bool
	err     error
}

func (entry *indexentry) ID() string {
	return ""
}

func (entry *indexentry) Key() ([]byte, uint64, bool, error) {
	return entry.key, entry.seqno, entry.deleted, entry.err
}

func (entry *indexentry) Value() []byte {
	return entry.value
}

func (entry *indexentry) Valueref() (valuelen uint64, vpos int64) {
	return 0, -1
}

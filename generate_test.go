package main

import "fmt"
import "time"
import "reflect"
import "testing"
import "strconv"
import "math/rand"

var _ = fmt.Sprintf("dummy")

func TestRandSource(t *testing.T) {
	rnd := rand.New(rand.NewSource(100))
	first := []int{}
	for i := 0; i < 10; i++ {
		first = append(first, rnd.Intn(10000000))
	}
	rnd = rand.New(rand.NewSource(100))
	second := []int{}
	for i := 0; i < 10; i++ {
		second = append(second, rnd.Intn(10000000))
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("%v != %v", first, second)
	}
}

func TestGenerateloads(t *testing.T) {
	klen, vlen, n := int64(32), int64(32), int64(1*1000*1000)
	g := Generateloads(klen, vlen, n)
	m, now, lastkey := map[int64]bool{}, time.Now(), int64(-1)
	for key, value := g(nil, nil); key != nil; key, value = g(key, value) {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if keynum <= lastkey {
			t.Fatalf("generated key %s is <= %v", key, lastkey)
		} else if _, ok := m[keynum]; ok {
			t.Fatalf("duplicated key %s", key)
		}
		m[keynum], lastkey = true, keynum
	}
	if int64(len(m)) != n {
		t.Fatalf("expected %v, got %v", n, len(m))
	}
	took := time.Since(now).Round(time.Second)
	t.Logf("Took %v to load %v items", took, len(m))
}

func TestGenerateloadr(t *testing.T) {
	klen, vlen, n, seed := int64(32), int64(32), int64(1*1000*1000), int64(100)
	g := Generateloadr(klen, vlen, n, seed)
	m, now := map[int64]bool{}, time.Now()
	for key, value := g(nil, nil); key != nil; key, value = g(key, value) {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if _, ok := m[keynum]; ok {
			t.Fatalf("duplicate key %s", key)
		}
		m[keynum] = true
	}
	if int64(len(m)) != n {
		t.Fatalf("expected %v, got %v", n, len(m))
	}
	took := time.Since(now).Round(time.Second)
	t.Logf("Took %v to load %v items", took, len(m))
}

func TestGenerateCRUD(t *testing.T) {
	loadm := map[int64]bool{}
	loadmaxkey := int64(0)

	// Initial load
	klen, vlen, loadn, seedl := int64(32), int64(32), int64(1000000), int64(100)
	g := Generateloadr(klen, vlen, loadn, seedl)
	for key, value := g(nil, nil); key != nil; key, value = g(key, value) {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		loadm[keynum] = true
		if keynum > loadmaxkey {
			loadmaxkey = keynum
		}
	}
	if int64(len(loadm)) != loadn {
		t.Fatalf("expected %v, got %v", loadn, len(loadm))
	}

	createm := map[int64]bool{}
	// Create load
	klen, vlen = int64(32), int64(32)
	createn, seedc := int64(2000000), int64(200)
	g = Generatecreate(klen, vlen, loadn, seedc)
	key, value := g(nil, nil)
	for i := int64(0); i < createn; i++ {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if _, ok := createm[keynum]; ok {
			t.Fatalf("duplicate key %s in create path", key)
		} else if _, ok := loadm[keynum]; ok {
			t.Fatalf("duplicate key %s in load path", key)
		} else if keynum <= loadmaxkey {
			t.Fatalf("%v <= %v", keynum, loadmaxkey)
		}
		createm[keynum] = true
		key, value = g(key, value)
	}
	if int64(len(createm)) != createn {
		t.Fatalf("expected %v, got %v", createn, len(createm))
	}

	readl, readc := map[int64]bool{}, map[int64]bool{}
	// read load
	klen, readn := int64(32), int64(2000000)
	gr := Generateread(klen, loadn, seedl, seedc)
	key = gr(nil, int64(len(createm)))
	for i := int64(0); i < readn; i++ {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if _, ok := loadm[keynum]; ok {
			readl[keynum] = true
		} else if _, ok := createm[keynum]; ok {
			readc[keynum] = true
		} else {
			t.Fatalf("generated key %s not found", key)
		}
		key = gr(key, int64(len(createm)))
	}
	if len(readl) != 850300 {
		t.Fatalf("%v != %v", len(readl), 850300)
	} else if len(readc) != 500000 {
		t.Fatalf("%v != %v", len(readc), 500000)
	}

	updatel, updatec := map[int64]bool{}, map[int64]bool{}
	// update load
	klen, vlen, updaten := int64(32), int64(32), int64(2000000)
	g = Generateupdate(klen, vlen, loadn, seedl, seedc, -1)
	key, value = g(nil, nil)
	for i := int64(0); i < updaten; i++ {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if _, ok := loadm[keynum]; ok {
			updatel[keynum] = true
		} else if _, ok := createm[keynum]; ok {
			updatec[keynum] = true
		} else {
			t.Fatalf("generated key %s not found", key)
		}
		key, value = g(key, value)
	}
	if len(updatel) != 850300 {
		t.Fatalf("%v != %v", len(updatel), 850300)
	} else if len(updatec) != 333333 {
		t.Fatalf("%v != %v", len(updatec), 333333)
	}

	deletel, deletec := map[int64]bool{}, map[int64]bool{}
	// delete load
	klen, deleten := int64(32), int64(1000000)
	gd := Generatedelete(klen, vlen, loadn, seedl, seedc, delmod)
	key, value = gd(nil, nil)
	for i := int64(0); i < deleten; i++ {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if _, ok := loadm[keynum]; ok {
			deletel[keynum] = true
		} else if _, ok := createm[keynum]; ok {
			deletec[keynum] = true
		} else {
			t.Fatalf("generated key %s not found", key)
		}
		key, value = gd(key, value)
	}
	if len(deletel) != 424961 {
		t.Fatalf("%v != %v", len(deletel), 424961)
	} else if len(deletec) != 166235 {
		t.Fatalf("%v != %v", len(deletec), 166235)
	}
}

func BenchmarkGenerateloads(b *testing.B) {
	klen, vlen, n := int64(32), int64(32), int64(1*1000*1000)
	g := Generateloads(klen, vlen, n)
	key, value := g(nil, nil)
	for i := 0; i < b.N; i++ {
		key, value = g(key, value)
	}
}

func BenchmarkGenerateloadr(b *testing.B) {
	klen, vlen, n, seed := int64(32), int64(32), int64(1*1000*1000), int64(100)
	g := Generateloadr(klen, vlen, n, seed)
	key, value := g(nil, nil)
	for i := 0; i < b.N; i++ {
		key, value = g(key, value)
	}
}

func BenchmarkGeneratecreate(b *testing.B) {
	klen, vlen, n, seedc := int64(32), int64(32), int64(1*1000*1000), int64(100)
	g := Generatecreate(klen, vlen, n, seedc)
	key, value := g(nil, nil)
	for i := 0; i < b.N; i++ {
		key, value = g(key, value)
	}
}

func BenchmarkGenerateupdate(b *testing.B) {
	klen, vlen, n := int64(32), int64(32), int64(1*1000*1000)
	seedl, seedc := int64(100), int64(200)
	g := Generateupdate(klen, vlen, n, seedl, seedc, -1)
	key, value := g(nil, nil)
	for i := 0; i < b.N; i++ {
		key, value = g(key, value)
	}
}

func BenchmarkGenerateread(b *testing.B) {
	klen, n := int64(32), int64(1*1000*1000)
	seedl, seedc := int64(100), int64(200)
	g := Generateread(klen, n, seedl, seedc)
	key := g(nil, 0)
	for i := 0; i < b.N; i++ {
		g(key, 0)
	}
}

func BenchmarkGeneratedelete(b *testing.B) {
	klen, n := int64(32), int64(1*1000*1000)
	seedl, seedc := int64(100), int64(200)
	g := Generatedelete(klen, klen, n, seedl, seedc, delmod)
	key, value := g(nil, nil)
	for i := 0; i < b.N; i++ {
		g(key, value)
	}
}

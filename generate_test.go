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
	keylen, n := int64(32), int64(1*1000*1000)
	g := Generateloads(keylen, n)
	m, now, lastkey := map[int64]bool{}, time.Now(), int64(-1)
	for key := g(nil); key != nil; key = g(key) {
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
	t.Logf("Took %v to load %v items", time.Since(now), len(m))
}

func TestGenerateloadr(t *testing.T) {
	keylen, n, seed := int64(32), int64(1*1000*1000), int64(100)
	g := Generateloadr(keylen, n, seed)
	m, now := map[int64]bool{}, time.Now()
	for key := g(nil); key != nil; key = g(key) {
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
	t.Logf("Took %v to load %v items", time.Since(now), len(m))
}

func TestGenerateCRUD(t *testing.T) {
	loadm := map[int64]bool{}
	loadmaxkey := int64(0)

	// Initial load
	keylen, loadn, seedl := int64(32), int64(1000000), int64(100)
	g := Generateloadr(keylen, loadn, seedl)
	for key := g(nil); key != nil; key = g(key) {
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
	keylen, createn, seedc := int64(32), int64(1000000), int64(200)
	g = Generatecreate(keylen, loadn, seedc)
	for i, key := int64(0), g(nil); i < createn; i, key = i+1, g(key) {
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
	}
	if int64(len(createm)) != createn {
		t.Fatalf("expected %v, got %v", createn, len(createm))
	}

	readl, readc := map[int64]bool{}, map[int64]bool{}
	// read load
	keylen, readn := int64(32), int64(1000000)
	g = Generateread(keylen, loadn, seedl, seedc)
	for i, key := int64(0), g(nil); i < readn; i, key = i+1, g(key) {
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
	}
	if len(readl) != 424961 {
		t.Fatalf("%v != %v", len(readl), 424961)
	} else if len(readc) != 166235 {
		t.Fatalf("%v != %v", len(readc), 166235)
	}

	updatel, updatec := map[int64]bool{}, map[int64]bool{}
	// update load
	keylen, updaten := int64(32), int64(1000000)
	g = Generateupdate(keylen, loadn, seedl, seedc)
	for i, key := int64(0), g(nil); i < updaten; i, key = i+1, g(key) {
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
	}
	if len(updatel) != 424961 {
		t.Fatalf("%v != %v", len(updatel), 424961)
	} else if len(updatec) != 166235 {
		t.Fatalf("%v != %v", len(updatec), 166235)
	}

	deletel, deletec := map[int64]bool{}, map[int64]bool{}
	// delete load
	keylen, deleten := int64(32), int64(1000000)
	g = Generatedelete(keylen, loadn, seedl, seedc)
	for i, key := int64(0), g(nil); i < deleten; i, key = i+1, g(key) {
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
	}
	if len(deletel) != 425339 {
		t.Fatalf("%v != %v", len(deletel), 424961)
	} else if len(deletec) != 167111 {
		t.Fatalf("%v != %v", len(deletec), 166235)
	}
}

func BenchmarkGenerateloads(b *testing.B) {
	keylen, n := int64(32), int64(1*1000*1000)
	g := Generateloads(keylen, n)
	key := g(nil)
	for i := 0; i < b.N; i++ {
		g(key)
	}
}

func BenchmarkGenerateloadr(b *testing.B) {
	keylen, n, seed := int64(32), int64(1*1000*1000), int64(100)
	g := Generateloadr(keylen, n, seed)
	key := g(nil)
	for i := 0; i < b.N; i++ {
		g(key)
	}
}

func BenchmarkGeneratecreate(b *testing.B) {
	keylen, n, seedc := int64(32), int64(1*1000*1000), int64(100)
	g := Generatecreate(keylen, n, seedc)
	key := g(nil)
	for i := 0; i < b.N; i++ {
		g(key)
	}
}

func BenchmarkGenerateread(b *testing.B) {
	keylen, n := int64(32), int64(1*1000*1000)
	seedl, seedc := int64(100), int64(200)
	g := Generateread(keylen, n, seedl, seedc)
	key := g(nil)
	for i := 0; i < b.N; i++ {
		g(key)
	}
}

func BenchmarkGenerateupdate(b *testing.B) {
	keylen, n := int64(32), int64(1*1000*1000)
	seedl, seedc := int64(100), int64(200)
	g := Generateupdate(keylen, n, seedl, seedc)
	key := g(nil)
	for i := 0; i < b.N; i++ {
		g(key)
	}
}

func BenchmarkGeneratedelete(b *testing.B) {
	keylen, n := int64(32), int64(1*1000*1000)
	seedl, seedc := int64(100), int64(200)
	g := Generatedelete(keylen, n, seedl, seedc)
	key := g(nil)
	for i := 0; i < b.N; i++ {
		g(key)
	}
}

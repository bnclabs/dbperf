package main

import "time"
import "testing"
import "math/rand"

func TestRandIntnUnique(t *testing.T) {
	s := rand.NewSource(100)
	rnd, n := rand.New(s), 10000000
	m := map[int]bool{}
	now := time.Now()
	for i := 0; i < n; i++ {
		m[rnd.Intn(n)] = true
	}
	fmsg := "Took %v to populate %v(%v) items in map"
	t.Logf(fmsg, time.Since(now), len(m), n)
}

func BenchmarkSourceInt63(b *testing.B) {
	s := rand.NewSource(100)
	for i := 0; i < b.N; i++ {
		s.Int63()
	}
}

func BenchmarkRandIntn(b *testing.B) {
	s := rand.NewSource(100)
	rnd := rand.New(s)
	for i := 0; i < b.N; i++ {
		rnd.Intn(1 * 1000 * 1000)
	}
}

package main

import "fmt"
import "strconv"
import "math/rand"

var _ = fmt.Sprintf("dummy")

var rndscale = int64(3)
var bitmask = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
var zeros = make([]byte, 4096)

func init() {
	for i := range zeros {
		zeros[i] = '0'
	}
}

// Generate presorted load, always return unique key,
// return nil after `n` keys.
func Generateloads(keylen, n int64) func([]byte) []byte {
	var textint [16]byte

	keynum := int64(0)
	return func(key []byte) []byte {
		if keynum >= n {
			return nil
		}
		key = Fixbuffer(key, int64(keylen))
		copy(key, zeros)
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		copy(key[keylen-int64(len(ascii)):keylen], ascii)
		keynum++
		return key
	}
}

// Generate unsorted load, always return unique key,
// return nill after `n` keys.
func Generateloadr(keylen, n, seed int64) func([]byte) []byte {
	var textint [16]byte

	intn := n * rndscale
	rnd := rand.New(rand.NewSource(seed))
	bitmap := make([]byte, ((intn / 8) + 1))

	count := int64(0)
	return func(key []byte) []byte {
		if count >= n {
			return nil
		}
		key = Fixbuffer(key, int64(keylen))
		copy(key, zeros)
		keynum := makeuniquekey(rnd, bitmap, intn)
		ascii := strconv.AppendInt(textint[:0], keynum, 10)
		copy(key[keylen-int64(len(ascii)):keylen], ascii)
		count++
		//fmt.Println(string(key))
		return key
	}
}

// Generate keys greater than loadn, always return unique keys.
func Generatecreate(keylen, loadn, seed int64) func([]byte) []byte {
	var textint [16]byte

	loadn = int64(loadn * rndscale)
	intn := int64(9223372036854775807) - loadn
	rnd := rand.New(rand.NewSource(seed))

	return func(key []byte) []byte {
		key = Fixbuffer(key, int64(keylen))
		copy(key, zeros)
		keynum := int64(rnd.Intn(int(intn))) + loadn
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		copy(key[keylen-int64(len(ascii)):keylen], ascii)
		return key
	}
}

func Generateread(keylen, loadn, seedl, seedc int64) func([]byte) []byte {
	var textint [16]byte
	var rndl, rndc *rand.Rand

	rndl = rand.New(rand.NewSource(seedl))
	if seedc > 0 {
		rndc = rand.New(rand.NewSource(seedc))
	}
	keynum, lcount := int64(0), int64(0)
	return func(key []byte) []byte {
		key = Fixbuffer(key, int64(keylen))
		copy(key, zeros)
		keynum, lcount, rndl = getkey(rndl, rndc, seedl, lcount, loadn)
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		copy(key[keylen-int64(len(ascii)):keylen], ascii)
		return key
	}
}

func Generateupdate(keylen, loadn, seedl, seedc int64) func([]byte) []byte {
	var textint [16]byte
	var keynum int64

	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	keynum, lcount := int64(0), int64(0)
	return func(key []byte) []byte {
		key = Fixbuffer(key, int64(keylen))
		copy(key, zeros)
		keynum, lcount, rndl = getkey(rndl, rndc, seedl, lcount, loadn)
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		copy(key[keylen-int64(len(ascii)):keylen], ascii)
		return key
	}
}

func Generatedelete(keylen, loadn, seedl, seedc int64) func([]byte) []byte {
	var textint [16]byte

	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	keynum, lcount := int64(0), int64(0)
	return func(key []byte) []byte {
		key = Fixbuffer(key, int64(keylen))
		copy(key, zeros)
		keynum, lcount, rndl = getkey(rndl, rndc, seedl, lcount, loadn)
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		copy(key[keylen-int64(len(ascii)):keylen], ascii)
		return key
	}
}

func makeuniquekey(rnd *rand.Rand, bitmap []byte, intn int64) int64 {
	for true {
		keynum := int64(rnd.Intn(int(intn)))
		if (bitmap[keynum/8] & bitmask[keynum%8]) == 0 {
			bitmap[keynum/8] |= bitmask[keynum%8]
			return keynum
		}
	}
	panic("unreachable code")
}

func getkey(
	rndl, rndc *rand.Rand,
	seedl, lcount, loadn int64) (keynum, lcount1 int64, rndl1 *rand.Rand) {

	loadn1 := loadn * rndscale
	intn := int64(9223372036854775807) - loadn1
	if lcount < loadn { // from load pool, headstart
		keynum = int64(rndl.Intn(int(loadn1)))
	} else if rndc != nil && (lcount%3) == 0 { // from create pool
		keynum = loadn1 + int64(rndc.Intn(int(intn)))
	} else { // from load pool
		keynum = int64(rndl.Intn(int(loadn1)))
	}
	lcount++
	if lcount >= loadn && (lcount%loadn) == 0 {
		rndl = rand.New(rand.NewSource(seedl))
	}
	return keynum, lcount, rndl
}

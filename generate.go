package main

import "fmt"
import "strconv"
import "math/rand"

var _ = fmt.Sprintf("dummy")

// Generate presorted load, always return unique key,
// return nil after `n` keys.
func Generateloads(klen, vlen, n int64) func(k, v []byte) ([]byte, []byte) {
	var textint [16]byte

	keynum := int64(0)
	return func(key, value []byte) ([]byte, []byte) {
		if keynum >= n {
			return nil, nil
		}
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		// create key
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		// create value
		value = Fixbuffer(value, int64(vlen))
		copy(value, zeros)
		copy(value[vlen-int64(len(ascii)):vlen], ascii)

		keynum++
		return key, value
	}
}

// Generate unsorted load, always return unique key,
// return nill after `n` keys.
func Generateloadr(
	klen, vlen, n, seed int64) func(k, v []byte) ([]byte, []byte) {

	var textint [16]byte

	intn := n * rndscale
	rnd := rand.New(rand.NewSource(seed))
	bitmap := make([]byte, ((intn / 8) + 1))

	count := int64(0)
	return func(key, value []byte) ([]byte, []byte) {
		if count >= n {
			return nil, nil
		}
		keynum := makeuniquekey(rnd, bitmap, intn)
		ascii := strconv.AppendInt(textint[:0], keynum, 10)
		// create key
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		// create value
		value = Fixbuffer(value, int64(vlen))
		copy(value, zeros)
		copy(value[vlen-int64(len(ascii)):vlen], ascii)

		count++
		return key, value
	}
}

// Generate keys greater than loadn, always return unique keys.
func Generatecreate(
	klen, vlen, loadn, seed int64) func(k, v []byte) ([]byte, []byte) {

	var textint [16]byte

	loadn = int64(loadn * rndscale)
	intn := int64(9223372036854775807) - loadn
	rnd := rand.New(rand.NewSource(seed))

	return func(key, value []byte) ([]byte, []byte) {
		keynum := int64(rnd.Intn(int(intn))) + loadn
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		// create key
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		// create value
		value = Fixbuffer(value, int64(vlen))
		copy(value, zeros)
		copy(value[vlen-int64(len(ascii)):vlen], ascii)
		return key, value
	}
}

func Generateupdate(
	klen, vlen, loadn,
	seedl, seedc, mod int64) func(k, v []byte) ([]byte, []byte) {

	var textint [16]byte
	var getkey func()

	loadn1 := loadn * rndscale
	intn := int64(9223372036854775807) - loadn1
	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	keynum, lcount := int64(0), int64(0)

	getkey = func() {
		if lcount < loadn { // from load pool, headstart
			keynum = int64(rndl.Intn(int(loadn1)))
		} else if (lcount % 3) == 0 { // from create pool
			keynum = loadn1 + int64(rndc.Intn(int(intn)))
		} else { // from load pool
			keynum = int64(rndl.Intn(int(loadn1)))
		}
		lcount++
		if lcount >= loadn && (lcount%loadn) == 0 {
			rndl = rand.New(rand.NewSource(seedl))
		}
		if mod >= 0 && (keynum%2) != mod {
			getkey()
		}
	}

	return func(key, value []byte) ([]byte, []byte) {
		getkey()
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		// create key
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		// create value
		value = Fixbuffer(value, int64(vlen))
		copy(value, zeros)
		copy(value[vlen-int64(len(ascii)):vlen], ascii)
		return key, value
	}
}

func Generateread(klen, loadn, seedl, seedc int64) func([]byte, int64) []byte {
	var textint [16]byte
	var getkey func(int64)

	loadn1 := loadn * rndscale
	intn := int64(9223372036854775807) - loadn1
	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	keynum, lcount := int64(0), int64(0)

	getkey = func(mod int64) {
		if lcount < loadn { // from load pool, headstart
			keynum = int64(rndl.Intn(int(loadn1)))
		} else if mod > 0 && (lcount%mod) != 0 { // from create pool
			keynum = loadn1 + int64(rndc.Intn(int(intn)))
		} else { // from load pool
			keynum = int64(rndl.Intn(int(loadn1)))
		}
		lcount++
		if lcount >= loadn && (lcount%loadn) == 0 {
			rndl = rand.New(rand.NewSource(seedl))
			rndc = rand.New(rand.NewSource(seedc))
		}
	}

	return func(key []byte, ncreates int64) []byte {
		getkey(ncreates / loadn)
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		return key
	}
}

func Generatereadseq(klen, loadn, seedl int64) func([]byte, int64) []byte {
	var textint [16]byte
	var getkey func(int64)

	rndl := rand.New(rand.NewSource(seedl))
	keynum, lcount := int64(0), int64(0)

	getkey = func(mod int64) {
		keynum = int64(rndl.Intn(int(loadn)))
		lcount++
	}

	return func(key []byte, ncreates int64) []byte {
		getkey(ncreates / loadn)
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		return key
	}
}

func Generatedelete(
	klen, vlen,
	loadn, seedl, seedc, mod int64) func(k, v []byte) ([]byte, []byte) {

	var textint [16]byte
	var getkey func()

	loadn1 := loadn * rndscale
	intn := int64(9223372036854775807) - loadn1
	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	keynum, lcount := int64(0), int64(0)

	getkey = func() {
		if lcount < loadn { // from load pool, headstart
			keynum = int64(rndl.Intn(int(loadn1)))
		} else if (lcount % 3) == 0 { // from create pool
			keynum = loadn1 + int64(rndc.Intn(int(intn)))
		} else { // from load pool
			keynum = int64(rndl.Intn(int(loadn1)))
		}
		lcount++
		if lcount >= loadn && (lcount%loadn) == 0 {
			rndl = rand.New(rand.NewSource(seedl))
		}
		if mod >= 0 && (keynum%2) != mod {
			getkey()
		}
	}

	return func(key, value []byte) ([]byte, []byte) {
		getkey()
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		// create key
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		// create value
		value = Fixbuffer(value, int64(vlen))
		copy(value, zeros)
		copy(value[vlen-int64(len(ascii)):vlen], ascii)
		return key, value
	}
}

var rndscale = int64(3)
var bitmask = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
var zeros = make([]byte, 4096)

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

func init() {
	for i := range zeros {
		zeros[i] = '0'
	}
}

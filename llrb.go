package main

import "github.com/prataprc/golog"
import gsllrb "github.com/prataprc/gostore/llrb"

func dollrb() error {
	err := llrbLoad()
	return err
}

func llrbLoad() error {
	var genkey func([]byte) []byte

	klen, n := int64(options.keylen), int64(options.entries)
	if options.seed > 0 {
		genkey = Generateloadr(klen, n, int64(options.seed))
	} else {
		genkey = Generateloads(klen, n)
	}

	llrb := gsllrb.NewLLRB1("perf", nil)
	key := make([]byte, options.keylen*2)
	val := make([]byte, options.vallen*2)
	for key = genkey(key); key != nil; key = genkey(key) {
		llrb.Set(key, value, val)
	}
	llrb.Log()
	stats := llrb.Stats()
	delete(stats, "node.blocks")
	delete(stats, "value.blocks")
	log.Infof("%v", stats)
	return nil
}

package main

import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"
import gsllrb "github.com/prataprc/gostore/llrb"

func dollrb() error {
	keycapacity := int64(float64(options.entries*options.keylen) * 1.2)
	valcapacity := int64(float64(options.entries*options.vallen) * 1.2)
	setts := s.Settings{"keycapacity": keycapacity, "valcapacity": valcapacity}
	llrb := gsllrb.NewLLRB1("perf", setts)
	genkey := Generateloadr(int64(options.keylen), int64(options.entries), 100)
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

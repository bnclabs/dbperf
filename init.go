package main

import "github.com/bnclabs/golog"

func init() {
	setts := map[string]interface{}{
		"log.flags":      "lshortfile",
		"log.timeformat": "",
		"log.prefix":     "",
	}
	log.SetLogger(nil, setts)
}

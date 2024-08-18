package main

import (
	"log"
	"mini-bitcask/bitcask"
)

var bc *bitcask.Bitcask
var dir string

func init() {
	dir = `D:\Code\goods\mini-bitcask\data`
	bc = bitcask.Open(dir)
	if bc == nil {
		log.Fatalf("get bc instance failed, nil bc")
		return
	}
}

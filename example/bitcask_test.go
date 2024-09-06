package main

import (
	"mini-bitcask/bitcask"
	"testing"
)

func TestLock(t *testing.T) {
	dir = `D:\Code\goods\mini-bitcask\data`
	bc2 := bitcask.Open(dir, bitcask.NewOption())
	if bc2 == nil {
		t.Logf("get bc instance failed, nil bc")
	}

	err := bc.ReleaseLock()
	if err != nil {
		t.Error(err)
		return
	}

	bc3 := bitcask.Open(dir, bitcask.NewOption())
	if bc3 == nil {
		t.Fatalf("get bc instance failed, nil bc")
	}
}

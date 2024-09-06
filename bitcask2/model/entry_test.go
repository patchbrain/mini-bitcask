package model_test

import (
	"github.com/stretchr/testify/assert"
	"mini-bitcask/bitcask2/codec"
	"mini-bitcask/bitcask2/model"
	"testing"
)

func TestEntryEnc(t *testing.T) {
	ent := model.NewEntry([]byte("test_key"), []byte("wwwlllttt"), false)
	b := codec.Encode(ent)
	t.Logf("bytes(len: %d): %#v", len(b), b)
	ent2 := codec.Decode(b)
	assert.NotNil(t, ent2)
	t.Logf("entry: %#v", ent2)

	entD := model.NewEntry([]byte("test_key"), []byte("wwwlllttt"), true)
	bD := codec.Encode(entD)
	t.Logf("bytes(len: %d): %#v", len(bD), bD)
	entD2 := codec.Decode(bD)
	assert.NotNil(t, entD2)
	t.Logf("entry: %#v", entD2)
}

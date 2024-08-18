package bitcask

import "testing"

func TestEncodeEntry(t *testing.T) {
	ent := Entry{
		KeySz:  5,
		ValSz:  4,
		TStamp: 1723988881,
		Key:    "test1",
		Value:  []byte("MTIz"),
	}

	EncodeEntry(ent)
}

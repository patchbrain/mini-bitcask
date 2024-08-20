package bitcask

import (
	"encoding/json"
	"time"
)

type Entry struct {
	KeySz  int32  `json:"keySz"`
	ValSz  int32  `json:"valSz"`
	TStamp int32  `json:"tStamp"`
	Key    string `json:"key"`
	Value  Value  `json:"value"`
}

type Value struct {
	Body []byte `json:"body"`
	Tomb byte   `json:"tomb"`
}

func NewEntry(t time.Time, key string, value Value) Entry {
	b, _ := json.Marshal(value)
	valLen := int32(len(b))

	return Entry{
		KeySz:  int32(len(key)),
		ValSz:  valLen,
		TStamp: int32(t.Unix()),
		Key:    key,
		Value:  value,
	}
}

func NewTombEntry(key string) Entry {
	return NewEntry(time.Now(), key, Value{Body: []byte{}, Tomb: 1})
}

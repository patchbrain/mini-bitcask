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
	Value  []byte `json:"value"`
}

type Tomb struct {
	TombStone int `json:"tombStone"`
}

func NewEntry(t time.Time, key string, value []byte) Entry {
	return Entry{
		KeySz:  int32(len(key)),
		ValSz:  int32(len(value)),
		TStamp: int32(t.Unix()),
		Key:    key,
		Value:  value,
	}
}

func NewTombEntry(key string) Entry {
	tomb := Tomb{TombStone: 1}
	b, _ := json.Marshal(tomb)

	return NewEntry(time.Now(), key, b)
}

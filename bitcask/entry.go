package bitcask

type Entry struct {
	KeySz  uint32 `json:"keySz"`
	ValSz  uint32 `json:"valSz"`
	TStamp uint32 `json:"tStamp"`
	Key    string `json:"key"`
	Value  Value  `json:"value"`
}

type Value struct {
	Body []byte `json:"body"`
	Tomb byte   `json:"tomb"`
}

func NewEntry(key string, value Value) Entry {
	valLen := len(value.Body) + 1

	return Entry{
		KeySz:  uint32(len(key)),
		ValSz:  uint32(valLen),
		TStamp: Tick(),
		Key:    key,
		Value:  value,
	}
}

func NewTombEntry(key string) Entry {
	return NewEntry(key, Value{Body: []byte{}, Tomb: 1})
}

package model

type Entry struct {
	Crc    uint32
	TStamp uint64
	KSize  uint32
	VSize  uint32 // value.Body.len + value.Tomb.len
	Key    []byte
	Value  Value
}

type Value struct {
	Body []byte
	Tomb byte // if entry is deleted, tomb equals 1
}

func NewEntry(key []byte, val []byte, del bool) Entry {
	var tomb byte
	if del {
		tomb = 1
	}

	ent := Entry{
		KSize: uint32(len(key)),
		VSize: uint32(len(val)) + 1, // body.len + tomb.len
		Key:   key,
		Value: Value{Body: val, Tomb: tomb},
	}
	return ent
}

const EntryHeaderSize int64 = 20

func (e *Entry) Len() int64 {
	return EntryHeaderSize + int64(e.KSize) + int64(e.VSize)
}

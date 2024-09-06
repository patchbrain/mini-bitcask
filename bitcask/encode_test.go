package bitcask

import "testing"

func TestEncodeEntry(t *testing.T) {
	ent := Entry{
		KeySz:  5,
		ValSz:  4,
		TStamp: 1723988881,
		Key:    "test1",
		Value: Value{
			Body: []byte("123"),
			Tomb: 0,
		},
	}

	ent2 := Entry{
		KeySz:  10,
		ValSz:  6,
		TStamp: 1723988881,
		Key:    "aaaaaaaaaa",
		Value: Value{
			Body: []byte("bbbbb"),
			Tomb: 0,
		},
	}

	b := EncodeEntry(ent)
	b = append(b, EncodeEntry(ent2)...)

	ent, offset, err := DecodeFrom(0, b)
	t.Logf("ent: %#v, offset: %d, err: %v", ent, offset, err)
	ent, offset, err = DecodeFrom(offset, b)
	t.Logf("ent: %#v, offset: %d, err: %v", ent, offset, err)

	return
}

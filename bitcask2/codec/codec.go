package codec

import (
	"encoding/binary"
	"github.com/sirupsen/logrus"
	_const "mini-bitcask/bitcask2/const"
	"mini-bitcask/bitcask2/model"
)

var headerSize = uint32(_const.EntryHeaderSize)

func Encode(e model.Entry) []byte {
	l := headerSize + e.KSize + e.VSize // 4(kSize) + 4(vSize) + 4(crc) + 8(timestamp) = 20
	b := make([]byte, 0, l)
	be := binary.BigEndian
	b = be.AppendUint32(b, e.Crc)
	b = be.AppendUint64(b, e.TStamp)
	b = be.AppendUint32(b, e.KSize)
	b = be.AppendUint32(b, e.VSize)
	b = append(b, e.Key...)
	b = append(b, e.Value.Body...)
	b = append(b, e.Value.Tomb)

	if int(l) != len(b) {
		return nil
	}
	return b
}

func Decode(data []byte) *model.Entry {
	var e model.Entry
	be := binary.BigEndian

	if len(data) < int(headerSize) {
		logrus.Errorf("decode bytes failed: data.len(%d) < headerSize", len(data))
		return nil
	}

	e.Crc = be.Uint32(data[:4])
	e.TStamp = be.Uint64(data[4:12])
	e.KSize = be.Uint32(data[12:16])
	e.VSize = be.Uint32(data[16:20])

	// check all length
	expectedLen := int(headerSize + e.KSize + e.VSize)
	if len(data) < expectedLen {
		logrus.Errorf("decode bytes failed: data.len(%d) < expectedLen(%d)", len(data), expectedLen)
		return nil
	}

	// key
	e.Key = make([]byte, e.KSize)
	copy(e.Key, data[headerSize:headerSize+e.KSize])

	// value.Body
	e.Value.Body = make([]byte, e.VSize-1)
	valAt := headerSize + e.KSize
	copy(e.Value.Body, data[valAt:valAt+e.VSize-1])

	// value.Tomb
	e.Value.Tomb = data[valAt+e.VSize-1]

	return &e
}

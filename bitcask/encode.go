package bitcask

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"mini-bitcask/util/log"
)

func EncodeEntry(ent Entry) []byte {
	b := make([]byte, 0)

	resBE := binary.BigEndian
	b = resBE.AppendUint32(b, uint32(ent.KeySz))
	b = resBE.AppendUint32(b, uint32(ent.ValSz))
	b = resBE.AppendUint32(b, uint32(ent.TStamp))
	buf := bytes.NewBuffer(b)
	buf.WriteString(ent.Key)
	buf.Write(append(ent.Value.Body, ent.Value.Tomb))
	log.FnLog("encode get string: %s", buf.String())
	return buf.Bytes()
}

var BytesShortErr = fmt.Errorf("bytes' length is too short")

func DecodeFrom(offset int, b []byte) (Entry, int /* new offset */, error) {
	var res Entry
	if offset+Header_size > len(b) {
		return Entry{}, 0, fmt.Errorf("%w, offset: %d, length: %d", BytesShortErr, offset, len(b))
	}

	readInt32 := func() int32 {
		value := int32(binary.BigEndian.Uint32(b[offset:]))
		offset += 4
		return value
	}

	res.TStamp = readInt32()
	res.KeySz = readInt32()
	res.ValSz = readInt32()

	keyEnd := offset + int(res.KeySz)
	if keyEnd+int(res.ValSz) > len(b) {
		return Entry{}, 0, fmt.Errorf("%w, offset: %d, length: %d", BytesShortErr, offset, len(b))
	}

	res.Key = string(b[offset:keyEnd])
	offset = keyEnd

	bodyEnd := offset + int(res.ValSz) - 1
	res.Value = Value{
		Body: b[offset:bodyEnd],
		Tomb: b[bodyEnd],
	}
	offset = bodyEnd + 1

	return res, offset, nil
}

package bitcask

import (
	"bytes"
	"encoding/binary"
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
	buf.Write(ent.Value)

	log.FnLog("encode get string: %s", buf.String())
	return buf.Bytes()
}

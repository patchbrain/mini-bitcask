package bitcask

import (
	"mini-bitcask/util/log"
)

// Index bitcask的索引，用于快速查找数据
type Index struct {
	M map[string]IndexEntry
}

func NewIndex(num ...int) *Index {
	log.FnLog("into")
	var capicity int
	if len(num) == 1 {
		capicity = num[0]
	}
	if capicity < 0 {
		capicity = 0
	}

	return &Index{M: make(map[string]IndexEntry, capicity)}
}

func (t *Index) Add(key string, newIe IndexEntry) {
	log.FnLog("into")

	t.M[key] = newIe
}

func (t *Index) Fetch(key string) IndexEntry {
	log.FnLog("into")

	return t.M[key]
}

type IndexEntry struct {
	FileIdx int32 `json:"fileIdx"`
	ValSz   int32 `json:"valSz"`
	Offset  int   `json:"offset"`
	TStamp  int32 `json:"tStamp"`
}

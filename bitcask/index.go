package bitcask

import (
	"sync"
)

// Index bitcask的索引，用于快速查找数据
type Index struct {
	sync.Mutex
	M map[string]IndexEntry
}

func NewIndex(num ...int) *Index {
	var capcity int
	if len(num) == 1 {
		capcity = num[0]
	}
	if capcity < 0 {
		capcity = 0
	}

	return &Index{M: make(map[string]IndexEntry, capcity)}
}

func (t *Index) Add(key string, newIe IndexEntry) {
	t.Lock()
	defer t.Unlock()

	t.M[key] = newIe
}

type IndexEntry struct {
	FileIdx int32 `json:"fileIdx"`
	ValSz   int32 `json:"valSz"`
	Offset  int   `json:"offset"`
	TStamp  int32 `json:"tStamp"`
}

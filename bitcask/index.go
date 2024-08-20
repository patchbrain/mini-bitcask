package bitcask

import (
	"mini-bitcask/util/log"
)

type IndexEntry struct {
	FileIdx int32 `json:"fileIdx"`
	ValSz   int32 `json:"valSz"`
	Offset  int   `json:"offset"`
	TStamp  int32 `json:"tStamp"`
}

// Index keyDir of bitcask, for fast index
type Index struct {
	M          map[string]IndexEntry
	MergeIdxCh chan IndexStuff
	EndMerge   chan struct{}
}

const (
	Update IndexOp = iota + 1
	Delete
)

type IndexOp int8

type IndexStuff struct {
	Op  IndexOp
	Key string
	Idx IndexEntry
}

func NewIndexStuff(op IndexOp, key string, idxEnt IndexEntry) IndexStuff {
	if op != Update && op != Delete {
		return IndexStuff{}
	}

	return IndexStuff{
		Op:  op,
		Key: key,
		Idx: idxEnt,
	}
}

func (t *Index) UpdateForMerge() {
	for {
		select {
		case item := <-t.MergeIdxCh:
			switch item.Op {
			case Update:
				t.M[item.Key] = item.Idx
			case Delete:
				delete(t.M, item.Key)
			}
		case <-t.EndMerge:
			log.FnLog("merge end")
		}
	}
}

func NewIndex(num ...int) *Index {
	log.FnDebug("into")
	var capicity int
	if len(num) == 1 {
		capicity = num[0]
	}
	if capicity < 0 {
		capicity = 0
	}

	return &Index{
		M:          make(map[string]IndexEntry, capicity),
		MergeIdxCh: make(chan IndexStuff),
		EndMerge:   make(chan struct{}),
	}
}

func (t *Index) Add(key string, newIe IndexEntry) {
	log.FnDebug("into")

	t.M[key] = newIe
}

func (t *Index) Fetch(key string) (IndexEntry, bool) {
	log.FnDebug("into")

	val, ok := t.M[key]
	return val, ok
}

func (t *Index) Remove(key string) {
	log.FnDebug("into")

	delete(t.M, key)
}

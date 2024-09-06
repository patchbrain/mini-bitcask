package bitcask

import (
	"mini-bitcask/util/log"
	"sync"
)

const (
	Update IndexOp = iota + 1
	Delete
)

var (
	MergeIdxChSize = 1000
)

type IndexEntry struct {
	FileIdx int32  `json:"fileIdx"`
	ValSz   int32  `json:"valSz"`
	Offset  int    `json:"offset"`
	TStamp  uint32 `json:"tStamp"`
}

// Index keyDir of bitcask, for fast index
type Index struct {
	M map[string]IndexEntry
	sync.RWMutex
}

type IndexOp int8

type IndexStuff struct {
	Op  IndexOp
	Key string
	Idx IndexEntry
}

func NewIndexStuff(op IndexOp, key string, idxEnt IndexEntry) IndexStuff {
	if op != Update && op != Delete {
		log.FnErrLog("invalid op type: %#v", op)
		return IndexStuff{}
	}

	return IndexStuff{
		Op:  op,
		Key: key,
		Idx: idxEnt,
	}
}

func (t *Index) UpdateForMerge(mergeIdxCh chan IndexStuff, wg *sync.WaitGroup) {
	log.FnDebug("into")
	defer wg.Done()
	for {
		select {
		case item, ok := <-mergeIdxCh:
			if !ok {
				log.FnLog("merge end or encountered an error")
				return
			}
			log.FnLog("get a index stuff: %#v", item)

			switch item.Op {
			case Update:
				t.Add(item.Key, item.Idx)
			case Delete:
				t.Remove(item.Key)
			}
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
		M: make(map[string]IndexEntry, capicity),
	}
}

func (t *Index) Add(key string, newIe IndexEntry) {
	log.FnDebug("into")

	t.Lock()
	v, ok := t.M[key]
	if !ok || (ok && newIe.TStamp > v.TStamp) {
		t.M[key] = newIe
	}
	t.Unlock()
}

func (t *Index) Fetch(key string) (IndexEntry, bool) {
	log.FnDebug("into")

	t.RLock()
	val, ok := t.M[key]
	t.RUnlock()
	return val, ok
}

func (t *Index) Remove(key string) {
	log.FnDebug("into")

	t.Lock()
	delete(t.M, key)
	t.Unlock()
}

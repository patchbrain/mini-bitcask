package index

import (
	_const "mini-bitcask/bitcask2/const"
	"sync"
)

// IndexEntry help to find entry in disk quickly
// encode: [ file_id(4) | offset(8) | value_sz(8) ]
type IndexEntry struct {
	FileId  int32 `json:"file_id"`
	Offset  int64 `json:"offset"`
	ValueSz int64 `json:"value_sz"`
}

func NewIndexEntry(fid int32, offset, valueSz int64) (IndexEntry, error) {
	ie := IndexEntry{
		FileId:  fid,
		Offset:  offset,
		ValueSz: valueSz,
	}

	if !ie.IsValid() {
		return IndexEntry{}, _const.InvalidIndexEntErr
	}

	return ie, nil
}

func (ie *IndexEntry) IsValid() bool {
	if ie.FileId <= 0 || ie.Offset < 0 || ie.ValueSz <= 0 {
		return false
	}

	return true
}

type Indexer interface {
	LoadIndexes() error // 从磁盘读取
	SaveIndexes() error // 持久化索引
	Add(Key, IndexEntry) error
	Get(Key) (IndexEntry, error)
	Del(Key) error
	Copy() Indexer
	Keys() []Key
	Foreach(fn Callback) error
	Exist(Key) bool
}

type Key string

type indexer struct {
	mu       sync.Mutex
	index    map[Key]IndexEntry
	needSave bool
}

func NewIndexer() Indexer {
	return &indexer{}
}

func (i *indexer) LoadIndexes() error {
	//TODO implement me
	panic("implement me")
}

func (i *indexer) SaveIndexes() error {
	//TODO implement me
	panic("implement me")
}

func (i *indexer) Add(key Key, entry IndexEntry) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if !entry.IsValid() {
		return _const.InvalidIndexEntErr
	}

	if i.index == nil {
		i.index = make(map[Key]IndexEntry)
	}

	i.index[key] = entry
	i.needSave = true

	return nil
}

func (i *indexer) Get(key Key) (IndexEntry, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.index == nil {
		return IndexEntry{}, _const.InvalidIndexErr
	}

	ie, ok := i.index[key]
	if !ok {
		return IndexEntry{}, _const.NoIndexErr
	}

	return ie, nil
}

func (i *indexer) Del(key Key) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.index == nil {
		return _const.InvalidIndexErr
	}

	i.index[key] = IndexEntry{}
	return nil
}

func (i *indexer) Copy() Indexer {
	i.mu.Lock()
	defer i.mu.Unlock()

	indexM := make(map[Key]IndexEntry, len(i.index))
	for k, v := range i.index {
		indexM[k] = v
	}

	return &indexer{index: indexM}
}

func (i *indexer) Keys() []Key {
	i.mu.Lock()
	defer i.mu.Unlock()

	keys := make([]Key, 0, len(i.index))
	for key := range i.index {
		keys = append(keys, key)
	}

	return keys
}

type Callback func(Key, IndexEntry) error

func (i *indexer) Foreach(fn Callback) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	var err error
	for k, v := range i.index {
		if err = fn(k, v); err != nil {
			return err
		}
	}

	return nil
}

func (i *indexer) Exist(key Key) bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	_, ok := i.index[key]
	return ok
}

package index

import (
	"encoding/binary"
	"github.com/sirupsen/logrus"
	"io"
	_const "mini-bitcask/bitcask2/const"
	"mini-bitcask/util/file"
	"os"
	"path/filepath"
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
	LoadIndexes(path string) error // load index from disk
	SaveIndexes(path string) error // save index to disk
	Add(Key, IndexEntry) error
	Get(Key) (IndexEntry, error)
	Del(Key) error
	Copy() Indexer
	Keys() []Key
	Foreach(fn Callback) error
	Exist(Key) bool
}

type Key string

func (k Key) Len() int {
	return len(k)
}

type indexer struct {
	mu       sync.Mutex
	index    map[Key]IndexEntry
	needSave bool
}

func NewIndexer() Indexer {
	indexes := make(map[Key]IndexEntry)
	return &indexer{index: indexes}
}

func (i *indexer) LoadIndexes(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			logrus.Infof("no hint file to load")
			return nil
		}

		return err
	}
	defer f.Close()

	err = loadIndex(i.index, f)
	if err != nil {
		return err
	}

	return nil
}

func loadIndex(indexes map[Key]IndexEntry, f *os.File) error {
	logrus.Infof("into")
	be := binary.BigEndian

	// Loop until the end of the file or an error occurs
	for {
		// Read the size of the key
		keySizeBuffer := make([]byte, _const.Int32Sz)
		if _, err := f.Read(keySizeBuffer); err != nil {
			if err == io.EOF {
				logrus.Infof("meet EOF, so finish load indexes")
				break // End of file reached, stop reading
			}
			return err
		}

		keySz := be.Uint32(keySizeBuffer)

		totalSize := int64(keySz) + _const.Int32Sz + _const.Int64Sz*2
		buffer := make([]byte, totalSize)
		if _, err := f.Read(buffer); err != nil {
			return err
		}

		var p int64
		var idx IndexEntry

		// Extract the key
		key := buffer[p : p+int64(keySz)]
		p += int64(keySz)

		idx.FileId = int32(be.Uint32(buffer[p : p+_const.Int32Sz]))
		p += _const.Int32Sz
		idx.Offset = int64(be.Uint64(buffer[p : p+_const.Int64Sz]))
		p += _const.Int64Sz
		idx.ValueSz = int64(be.Uint64(buffer[p : p+_const.Int64Sz]))
		p += _const.Int64Sz

		indexes[Key(key)] = idx
	}

	return nil
}

func (i *indexer) SaveIndexes(path string) error {
	if !i.needSave || i.index == nil {
		return nil
	}

	logrus.Infof("into")
	dir := filepath.Dir(path)
	if !file.IsFileExist(dir) {
		return _const.FileNotExist
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	iCopy := i.Copy()
	if err = iCopy.Foreach(func(k Key, v IndexEntry) error {
		if cErr := writeIndex(k, v, f); cErr != nil {
			return cErr
		}

		return nil
	}); err != nil {
		logrus.Errorf("@SaveIndexes save index to disk failed: %s", err.Error())
		return err
	}
	logrus.Infof("finish save indexes")

	return nil
}

func writeIndex(k Key, v IndexEntry, f io.Writer) error {
	be := binary.BigEndian
	b := make([]byte, 0, _const.Int32Sz*2+int64(k.Len())+_const.Int64Sz*2)
	b = be.AppendUint32(b, uint32(k.Len()))
	b = append(b, []byte(k)...)
	b = be.AppendUint32(b, uint32(v.FileId))
	b = be.AppendUint64(b, uint64(v.Offset))
	b = be.AppendUint64(b, uint64(v.ValueSz))

	n, err := f.Write(b)
	if n != len(b) {
		return _const.InvalidIndexEntErr
	}

	if err != nil {
		return err
	}

	if err = f.(*os.File).Sync(); err != nil {
		return err
	}

	return nil
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

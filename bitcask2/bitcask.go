package bitcask2

import (
	"errors"
	"github.com/gofrs/flock"
	_const "mini-bitcask/bitcask2/const"
	"mini-bitcask/bitcask2/files_mgr"
	"mini-bitcask/bitcask2/index"
	"path/filepath"
	"sync"
)

func Open(dir string, opt ...Option) (b *Bitcask, err error) {
	if len(opt) > 1 {
		err = errors.New("too many options, just need 1")
		return nil, err
	}
	if len(opt) == 0 {
		opt = append(opt, defOpt)
	}

	b = new(Bitcask)

	b.flock = flock.New(filepath.Join(dir, ".lock"))
	ok, err := b.flock.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, _const.FileLockErr
	}

	indexer := index.NewIndexer()
	b.index = indexer

	b.fm = files_mgr.NewFileMgr(dir, opt[0].maxFileSize)
	err = b.fm.LoadDfs()
	if err != nil {
		return nil, err
	}

	return b, nil
}

type Bitcask struct {
	// mutex
	mu sync.Mutex
	// index, to find kv efficiently, and recover from disk
	index index.Indexer
	// files, interact with File System
	fm *files_mgr.FileMgr
	// flock, file lock
	flock *flock.Flock
	// isMerging, true: merging, false conversely
	isMerging bool
}

func (b *Bitcask) Put(key, value []byte) error {
	// put value in disk
	fid, offset, valSz, err := b.fm.Put(key, value)
	if err != nil {
		return err
	}

	// create index
	indexEnt, err := index.NewIndexEntry(fid, offset, valSz)
	if err != nil {
		return err
	}

	err = b.index.Add(index.Key(key), indexEnt)
	if err != nil {
		return err
	}

	return nil
}

func (b *Bitcask) Del(key []byte) (err error) {
	err = b.fm.Del(key)
	if err != nil {
		return err
	}

	err = b.index.Del(index.Key(key))
	if err != nil {
		return err
	}

	return nil
}

func (b *Bitcask) Get(key []byte) (val []byte, err error) {
	var ie index.IndexEntry
	ie, err = b.index.Get(index.Key(key))
	if err != nil {
		return nil, err
	}

	if !ie.IsValid() {
		return nil, nil
	}

	val, err = b.fm.Get(ie.FileId, ie.Offset, ie.ValueSz)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (b *Bitcask) Merge(key []byte) (val []byte, err error) {
	return nil, err
}

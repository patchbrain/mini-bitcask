package bitcask2

import (
	"errors"
	"github.com/gofrs/flock"
	"mini-bitcask/bitcask2/files_mgr"
	"mini-bitcask/bitcask2/index"
	"sync"
)

func Open(dir string, opt ...Option) (b *Bitcask, err error) {
	if len(opt) >= 1 {
		err = errors.New("too many options, just need 1")
		return nil, err
	}

}

type Bitcask struct {
	// mutex
	mu sync.Mutex
	// index, to find kv efficiently, and recover from disk
	index *index.Index
	// files, interact with File System
	fm *files_mgr.FileMgr
	// flock, file lock
	flk *flock.Flock
	// isMerging, true: merging, false conversely
	isMerging bool
}

func (b *Bitcask) Put(key, value []byte) (err error) {

}

func (b *Bitcask) Del(key []byte) (err error) {

}

func (b *Bitcask) Get(key []byte) (val []byte, err error) {

}

func (b *Bitcask) Merge(key []byte) (val []byte, err error) {

}

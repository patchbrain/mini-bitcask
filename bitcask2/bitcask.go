package bitcask2

import (
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"github.com/sirupsen/logrus"
	_const "mini-bitcask/bitcask2/const"
	"mini-bitcask/bitcask2/files_mgr"
	"mini-bitcask/bitcask2/index"
	"os"
	"path/filepath"
	"strconv"
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
	// mergeTrigFid, file id of merge triggering moment
	mergeTrigFid int32
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

func (b *Bitcask) Merge() (err error) {
	b.mu.Lock()
	logrus.Info("Starting merge operation")

	if b.fm.MaxFileId() <= 0 {
		b.mu.Unlock()
		logrus.Warn("No files to merge")
		return _const.NoFileToMergeErr
	}

	b.mergeTrigFid = b.fm.MaxFileId() // fileId of merge triggering moment

	if err = b.fm.Rotate(); err != nil {
		b.mu.Unlock()
		logrus.Errorf("Failed to rotate file: %v", err)
		return err
	}

	// 保留当前索引的快照，用于创建新的数据文件
	curIndex := b.index.Copy()
	b.mu.Unlock()

	mFilePaths, mergeIdx, err := b.genFilesByIndexer(curIndex)
	if err != nil {
		logrus.Errorf("Failed to generate new data files from index: %v", err)
		return err
	}
	logrus.Infof("Generated new merged data files: %v", mFilePaths)

	err = b.replaceOldFiles(mFilePaths)
	if err != nil {
		logrus.Errorf("Failed to replace old data files: %v", err)
		return err
	}
	logrus.Info("Successfully replaced old data files with new merged files")

	err = b.mergeWithIndex(mergeIdx)
	if err != nil {
		logrus.Errorf("Failed to merge with new index: %v", err)
		return err
	}
	logrus.Info("Successfully merged with new indexer")

	return nil
}

func (b *Bitcask) genFilesByIndexer(indexer index.Indexer) ([]string, index.Indexer, error) {
	keys := indexer.Keys()

	mergeDir := _const.MergeDir
	mergeB, err := Open(mergeDir)
	if err != nil {
		return []string{}, nil, err
	}

	defer func() {
		// todo: mergeB.Close()
		// todo: del all files of bitcask for merge
	}()

	for _, key := range keys {
		v, err := b.Get([]byte(key))
		if err != nil {
			return []string{}, nil, err
		}

		err = mergeB.Put([]byte(key), v)
		if err != nil {
			return []string{}, nil, err
		}
	}

	dfs := mergeB.fm.DataFiles()
	paths := make([]string, 0, len(dfs)) // all paths need rename
	for _, df := range dfs {
		paths = append(paths, filepath.Join(mergeDir, df.Name()))
	}

	newIndex := mergeB.index.Copy()

	return paths, newIndex, nil
}

func (b *Bitcask) replaceOldFiles(newPs []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	curDir := b.fm.Dir()

	var oldFiles []string
	for i := 0; i < int(b.mergeTrigFid); i++ {
		fName := _const.Datafile_prefix + strconv.Itoa(i)
		filePath := filepath.Join(curDir, fName)
		_, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				// 文件不存在，可能文件 ID 不连续，继续
				continue
			}
			return fmt.Errorf("failed to stat old file %s: %v", filePath, err)
		}
		oldFiles = append(oldFiles, filePath)
	}

	for _, oldFile := range oldFiles {
		err := os.Remove(oldFile)
		if err != nil {
			return fmt.Errorf("failed to remove old file %s: %v", oldFile, err)
		}
	}

	for _, newP := range newPs {
		fName := filepath.Base(newP)
		finalName := filepath.Join(curDir, fName)
		err := os.Rename(newP, finalName)
		if err != nil {
			return fmt.Errorf("failed to rename new file %s: %s", newP, err.Error())
		}
	}

	return nil
}

// merge specific indexer with b.index
func (b *Bitcask) mergeWithIndex(mergeIdx index.Indexer) error {
	err := mergeIdx.Foreach(func(key index.Key, entry index.IndexEntry) error {
		if !b.index.Exist(key) {
			cErr := b.index.Add(key, entry)
			if cErr != nil {
				return cErr
			}

			return nil
		}

		// if b.index has the key, check the file id
		ent, cErr := b.index.Get(key)
		if cErr != nil {
			return cErr
		}

		if ent.FileId > b.mergeTrigFid {
			// indicate that the index is added after merge moment
			return nil
		} else {
			if cErr = b.index.Add(key, entry); cErr != nil {
				return cErr
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

package bitcask2

import (
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"github.com/sirupsen/logrus"
	_const "mini-bitcask/bitcask2/const"
	"mini-bitcask/bitcask2/files_mgr"
	"mini-bitcask/bitcask2/index"
	"mini-bitcask/util/file"
	"os"
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
		logrus.Errorf("Bitcask Get failed:%s, key: %s, index entry: %#v", err.Error(), string(key), ie)
		return nil, err
	}

	return val, nil
}

func (b *Bitcask) Merge() (err error) {
	b.mu.Lock()

	b.isMerging = true
	defer func() {
		// clean merge dir
		dir := filepath.Dir(b.fm.Dir())
		mergeDir := filepath.Join(dir, _const.MergeDir)
		_ = os.RemoveAll(mergeDir)

		b.isMerging = false
	}()

	if b.fm.MaxFileId() <= 0 {
		b.mu.Unlock()
		logrus.Warn("No files to merge")
		return _const.NoFileToMergeErr
	}

	b.mergeTrigFid = b.fm.MaxFileId() // fileId of merge triggering moment
	logrus.Infof("Starting merge operation, merge trigger fid: %d", b.mergeTrigFid)

	if err = b.fm.Rotate(); err != nil {
		b.mu.Unlock()
		logrus.Errorf("Failed to rotate file: %v", err)
		return err
	}

	// get a screenshot of indexer, to create new data files
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

	b.fm.Close()
	if err != nil {
		logrus.Errorf("close files manager failed: %v", err)
		return err
	}

	err = b.fm.LoadDfs()
	if err != nil {
		logrus.Errorf("load DFs after merge failed: %v", err)
		return err
	}

	logrus.Info("Successfully merged with new indexer")

	return nil
}

func (b *Bitcask) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var err error
	err = b.fm.CloseDfs(-1)
	if err != nil {
		return err
	}

	err = b.flock.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func (b *Bitcask) genFilesByIndexer(indexer index.Indexer) ([]string, index.Indexer, error) {
	keys := indexer.Keys()
	logrus.Infof("get indexer's keys: %#v", keys)

	dir := filepath.Dir(b.fm.Dir())
	mergeDir := filepath.Join(dir, _const.MergeDir)
	if err := file.EnsureDir(mergeDir); err != nil {
		return nil, nil, err
	}

	mergeB, err := Open(mergeDir)
	if err != nil {
		return []string{}, nil, err
	}

	defer mergeB.Close()

	for _, key := range keys {
		v, err := b.Get([]byte(key))
		if err != nil {
			logrus.Errorf("get kv failed: %s", err.Error())
			return []string{}, nil, err
		}

		if v == nil {
			continue
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
	var err error

	for _, df := range b.fm.DataFiles() {
		if df.FileId() > b.mergeTrigFid {
			continue
		}

		if err = df.Close(); err != nil {
			return err
		}
		filePath := filepath.Join(curDir, df.Name())
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
				logrus.Errorf("index.Add failed: %s", cErr.Error())
				return cErr
			}

			return nil
		}

		// if exists key in b.index, check the file id whether greater than b.mergeTrigFid
		ent, cErr := b.index.Get(key)
		if cErr != nil {
			logrus.Errorf("index.Get failed: %s", cErr.Error())
			return cErr
		}

		if ent.FileId > b.mergeTrigFid {
			// the index is after merge trigger moment
			return nil
		} else {
			logrus.Infof("merge index, key: %s, entry: %#v", string(key), entry)
			cErr = b.index.Add(key, entry)
			if cErr != nil {
				logrus.Errorf("index.Add failed: %s", cErr.Error())
				return cErr
			}
		}

		return nil
	})

	if err != nil {
		logrus.Errorf("mergeWithIndex failed: %s", err.Error())
		return err
	}

	logrus.Info("Successfully merged index with mergeIdx")
	return nil
}

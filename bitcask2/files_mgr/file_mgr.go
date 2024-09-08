package files_mgr

import (
	"github.com/sirupsen/logrus"
	_const "mini-bitcask/bitcask2/const"
	"mini-bitcask/bitcask2/model"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type FileMgr struct {
	// dfs models of files
	dfs []*Datafile

	// dir of data files
	dir string

	// cur, writable data file
	cur *Datafile

	// maxFileSize, if cur's file size exceed this, should rotate
	maxFileSize int
}

func NewFileMgr(dir string, maxFileSize int) *FileMgr {
	var mgr FileMgr
	mgr.dir = dir
	mgr.maxFileSize = maxFileSize

	return &mgr
}

// LoadDfs according to the dir, load all the datafiles
func (f *FileMgr) LoadDfs() error {
	files, err := filepath.Glob(filepath.Join(f.dir, _const.Datafile_prefix+"*"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		logrus.Infof("no file to load, dir: %s", f.dir)
		return nil
	}

	fids := make([]int, 0, len(files))
	for _, file := range files {
		file = filepath.Base(file)

		v, err := strconv.Atoi(strings.TrimPrefix(file, _const.Datafile_prefix))
		if err != nil {
			return err
		}
		fids = append(fids, v)
	}

	sort.Ints(fids) // asc order
	f.dfs = make([]*Datafile, 0, len(fids))

	for i, fid := range fids {
		writable := false
		if i == fids[len(fids)-1] {
			writable = true
		}

		df, err := NewDatafile(f.dir, fid, writable, f.maxFileSize)
		if err != nil {
			return err
		}

		f.dfs = append(f.dfs, df)
	}

	f.cur = f.dfs[len(f.dfs)-1]

	for _, df := range f.dfs {
		logrus.Infof("load datafile name: %s", df.Name())
	}

	return nil
}

func (f *FileMgr) Put(key, value []byte) (fileId int, offset int64, err error) {
	if f.cur == nil || f.cur.MaybeRotate() {
		if f.cur != nil {
			logrus.Infof("need rotate, now file id: %d, offset: %d", f.cur.FileId(), f.cur.Offset())
		} else {
			logrus.Infof("create the first file")
		}

		err = f.rotate()
		if err != nil {
			return 0, 0, err
		}
	}

	isDel := value == nil

	ent := model.NewEntry(key, value, isDel)
	err = f.cur.Put(ent)
	if err != nil {
		return 0, 0, err
	}

	fileId = f.cur.FileId()
	offset = f.cur.Offset() - ent.Len()
	logrus.Infof("put a entry: %#v, entry beginning offset: %d, file id: %d", ent, offset, fileId)
	return
}

func (f *FileMgr) Del(key []byte) (fileId int, offset int64, err error) {
	if f.cur == nil {
		return 0, 0, _const.NotWritableErr
	}

	return f.Put(key, nil)
}

func (f *FileMgr) MaxFileId() int {
	if f.cur == nil {
		return 0
	}

	return f.cur.FileId()
}

func (f *FileMgr) rotate() error {
	nextFid := f.MaxFileId() + 1

	curDf, err := NewDatafile(f.dir, nextFid, true, f.maxFileSize)
	if err != nil {
		return err
	}

	f.switchCurr(curDf)
	return nil
}

func (f *FileMgr) switchCurr(newCurDf *Datafile) {
	if f.cur != nil {
		f.cur.AbandonWrite()
	}

	f.cur = newCurDf
}

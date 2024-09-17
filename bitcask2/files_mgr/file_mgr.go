package files_mgr

import (
	"github.com/sirupsen/logrus"
	"math"
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

	// fid2arrIdx, value: dfs's index, key: fild id
	fid2arrIdx map[int32]int
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

	err = f.CloseDfs(-1)
	if err != nil {
		return err
	}

	f.dfs = make([]*Datafile, 0, len(fids))

	for i, fid := range fids {
		writable := false
		if i == fids[len(fids)-1] {
			writable = true
		}

		df, err := NewDatafile(f.dir, int32(fid), writable, f.maxFileSize)
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

func (f *FileMgr) Put(key, value []byte) (fileId int32, offset int64, valueSz int64, err error) {
	if f.cur == nil || f.cur.MaybeRotate() {
		if f.cur != nil {
			logrus.Infof("need rotate, now file id: %d, offset: %d", f.cur.FileId(), f.cur.Offset())
		} else {
			logrus.Infof("create the first file")
		}

		err = f.Rotate()
		if err != nil {
			return 0, 0, 0, err
		}
	}

	isDel := value == nil

	ent := model.NewEntry(key, value, isDel)
	err = f.cur.Put(ent)
	if err != nil {
		return 0, 0, 0, err
	}

	fileId = f.cur.FileId()
	offset = f.cur.Offset() - ent.Len()
	valueSz = ent.Len()
	logrus.Infof("put a entry: %#v, entry beginning offset: %d, file id: %d", ent, offset, fileId)
	return
}

func (f *FileMgr) Get(fid int32, offset, valSz int64) ([]byte, error) {
	ent, err := f.GetDatafileByFid(fid).ReadAt(offset, valSz)
	if err != nil {
		return nil, err
	}

	if ent.Len() != valSz {
		logrus.Errorf("actual entry's value len(%d) not equal to valSz(%d)", ent.Len(), valSz)
		return nil, _const.ReadEntryErr
	}

	return ent.Value.Body, nil
}

func (f *FileMgr) GetDatafileByFid(fid int32) *Datafile {
	if f.fid2arrIdx == nil {
		f.fid2arrIdx = make(map[int32]int, len(f.dfs))
		for i, df := range f.dfs {
			f.fid2arrIdx[df.FileId()] = i
		}
	}

	return f.dfs[f.fid2arrIdx[fid]]
}

func (f *FileMgr) DataFiles() []*Datafile {
	return f.dfs
}

func (f *FileMgr) Del(key []byte) (err error) {
	if f.cur == nil {
		return _const.NotWritableErr
	}

	_, _, _, err = f.Put(key, nil)
	return err
}

// CloseDfs if maxFid = -1, no limit
func (f *FileMgr) CloseDfs(maxFid int32) error {
	var err error
	if maxFid == -1 {
		maxFid = math.MaxInt32
	}

	for _, df := range f.dfs {
		if df.FileId() > maxFid {
			continue
		}

		if err = df.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (f *FileMgr) Close() error {
	err := f.CloseDfs(-1)
	f.fid2arrIdx = nil
	f.dfs = nil
	f.cur = nil

	return err
}

func (f *FileMgr) MaxFileId() int32 {
	if f.cur == nil {
		return 0
	}

	return f.cur.FileId()
}

func (f *FileMgr) Rotate() error {
	nextFid := f.MaxFileId() + 1

	curDf, err := NewDatafile(f.dir, nextFid, true, f.maxFileSize)
	if err != nil {
		return err
	}

	f.dfs = append(f.dfs, curDf)
	f.switchCurr(curDf)
	return nil
}

func (f *FileMgr) Dir() string {
	return f.dir
}

func (f *FileMgr) switchCurr(newCurDf *Datafile) {
	if f.cur != nil {
		f.cur.AbandonWrite()
	}

	f.cur = newCurDf
}

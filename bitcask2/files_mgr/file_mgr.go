package files_mgr

import (
	"github.com/sirupsen/logrus"
	_const "mini-bitcask/bitcask2/const"
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

	for _, df := range f.dfs {
		logrus.Infof("datafile name: %s", df.Name())
	}

	return nil
}

func (f *FileMgr) Put(key, value []byte) (fileId int, offset uint32, err error) {
	return 0, 0, err
}

func (f *FileMgr) rotate() error {
	return nil
}

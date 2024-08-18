package bitcask

import (
	"encoding/json"
	"fmt"
	"mini-bitcask/util/file"
	"mini-bitcask/util/log"
	"time"
)

type Bitcask struct {
	F   *FileMgr
	Idx *Index
	Opt *Option
	Dir string
}

func Open(dir string) *Bitcask {
	log.FnLog("into")
	bc := new(Bitcask)

	err := file.EnsureDir(dir, true)
	if err != nil {
		log.FnErrLog("ensure dir(%s) failed: %s", dir, err.Error())
		return nil
	}

	// todo: 检查是否有其他的Bitcask在使用该目录

	opt := NewOption()
	fMgr := NewFileMgr(dir, opt.MaxSingleFileSz)
	_, err = fMgr.CreateFile(true)
	if err != nil {
		log.FnErrLog("create file manager failed: %s", err.Error())
		return nil
	}

	bc.F = fMgr

	// todo: 加载index

	idx := NewIndex()
	bc.Idx = idx

	return bc
}

func (t *Bitcask) Get(key string) []byte {
	log.FnLog("into")
	// 找到索引结构体
	idxEntry := t.Idx.Fetch(key)
	valB := t.F.Read(idxEntry.FileIdx, idxEntry.ValSz, idxEntry.Offset, key)
	if valB == nil {
		log.FnErrLog("found nil val")
		return nil
	}

	log.FnLog("found val: %s", string(valB))
	return valB
}

func (t *Bitcask) Set(key string, val interface{}) error {
	log.FnLog("into")
	wErr := func(err error) error {
		return fmt.Errorf("bitcask set error: %w", err)
	}

	b, err := json.Marshal(val)
	if err != nil {
		return wErr(err)
	}

	var (
		valSz  = int32(len(b))
		tStamp = int32(time.Now().Unix())
		fid    int32
	)

	e := Entry{
		KeySz:  int32(len(key)),
		ValSz:  valSz,
		TStamp: tStamp,
		Key:    key,
		Value:  b,
	}

	fid, err = t.F.Append(e)
	if err != nil {
		return wErr(err)
	}

	lastOff := t.F.Offset() - int(valSz) - len(key) - Header_size
	// 写索引信息
	idxE := IndexEntry{
		FileIdx: fid,
		ValSz:   valSz,
		Offset:  lastOff,
		TStamp:  0,
	}
	log.FnLog("set a index entry: %#v", idxE)
	t.Idx.Add(key, idxE)

	return nil
}

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
	log.FnDebug("into")
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
	log.FnDebug("into")
	// 找到索引结构体
	idxEntry, ok := t.Idx.Fetch(key)
	if !ok {
		log.FnLog("found no index")
		return nil
	}

	valB := t.F.Read(idxEntry.FileIdx, idxEntry.ValSz, idxEntry.Offset, key)
	if valB == nil {
		log.FnLog("found nil val")
		return nil
	}

	if isTomb(valB) {
		log.FnLog("value is tomb, key: %s", key)
		return nil
	}

	log.FnLog("found val: %s", string(valB))
	return valB[:len(valB)-1]
}

func isTomb(b []byte) bool {
	if len(b) == 0 {
		log.FnErrLog("error data")
		return false
	}

	if b[len(b)-1] == 0 {
		return false
	}

	return true
}

func (t *Bitcask) Set(key string, val interface{}) error {
	log.FnDebug("into")
	wErr := func(err error) error {
		return fmt.Errorf("bitcask set error: %w", err)
	}

	b, err := json.Marshal(val)
	if err != nil {
		return wErr(err)
	}

	var (
		fid int32
	)

	e := NewEntry(time.Now(), key, Value{Body: b})

	fid, err = t.F.Append(e)
	if err != nil {
		return wErr(err)
	}

	valSz := int32(t.F.Offset() - t.F.LastOffset() - Header_size - len(key))
	// 写索引信息
	idxE := IndexEntry{
		FileIdx: fid,
		ValSz:   valSz,
		Offset:  t.F.LastOffset(),
		TStamp:  0,
	}
	log.FnLog("set a index entry: %#v", idxE)
	t.Idx.Add(key, idxE)

	return nil
}

func (t *Bitcask) Del(key string) error {
	log.FnDebug("into")
	// 在文件中追加一个墓碑值
	_, err := t.F.Append(NewTombEntry(key))
	if err != nil {
		return err
	}

	// 删除索引
	t.Idx.Remove(key)

	return nil
}

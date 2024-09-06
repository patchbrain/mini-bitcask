package bitcask

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"mini-bitcask/util/file"
	"mini-bitcask/util/log"
	"path"
	"sync"
)

type Bitcask struct {
	F     *FileMgr
	Index *Index
	Opt   Option
	Dir   string
	EndCh chan struct{}
	fLock *flock.Flock

	// fid of last time merge-end
	LastMerge int32
}

var (
	GMerging int32 = 0 // is merging now? 0:no 1:yes
)

func Open(dir string, opt Option) *Bitcask {
	log.FnDebug("into")
	bc := new(Bitcask)

	err := file.EnsureDir(dir)
	if err != nil {
		log.FnErrLog("ensure dir(%s) failed: %s", dir, err.Error())
		return nil
	}

	// check maybe exists other bitcask using the dir
	err = bc.tryLock()
	if err != nil {
		log.FnErrLog("lock failed: %s", err.Error())
		return nil
	}

	bc.Opt = opt

	fMgr := NewFileMgr(dir, opt.MaxSingleFileSz)
	_, err = fMgr.CreateFile(true)
	if err != nil {
		log.FnErrLog("create file manager failed: %s", err.Error())
		return nil
	}

	bc.F = fMgr

	// todo: 加载index

	idx := NewIndex()
	bc.Index = idx

	return bc
}

// try to lock the specified path
func (t *Bitcask) tryLock() error {
	p := path.Join(t.Dir, ".lock")
	t.fLock = flock.New(p)

	locked, err := t.fLock.TryLock()
	if err != nil {
		return err
	}

	if locked {
		return nil
	}

	return errors.New("file already locked")
}

func (t *Bitcask) ReleaseLock() error {
	return t.fLock.Unlock()
}

func (t *Bitcask) Get(key string) []byte {
	log.FnDebug("into")
	// 找到索引结构体
	idxEntry, ok := t.Index.Fetch(key)
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

	e := NewEntry(key, Value{Body: b})

	fid, err = t.F.Append(e)
	if err != nil {
		return wErr(err)
	}

	valSz := int32(t.F.Offset() - t.F.LastOffset() - Header_size - len(key))
	idxE := IndexEntry{
		FileIdx: fid,
		ValSz:   valSz,
		Offset:  t.F.LastOffset(),
		TStamp:  e.TStamp,
	}
	log.FnLog("set a index entry, key: %s,value: %#v", key, idxE)
	t.Index.Add(key, idxE)

	if GMerging == 0 && fid-t.LastMerge > t.Opt.MergeThreshold {
		log.FnLog("begin to merge, cur point to next file")
		GMerging = 1

		// because of merging, cur point next
		_, err = t.F.CreateFile(true)
		if err != nil {
			log.FnErrLog("because of merging, cur point next failed: %s", err.Error())
			return err
		}

		go func() {
			cErr := t.Merge()
			if cErr != nil {
				log.FnErrLog("exec merge failed: %s", cErr.Error())
				return
			}
		}()
	}

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
	t.Index.Remove(key)

	return nil
}

func (t *Bitcask) Merge() error {
	if t.F.next <= 2 {
		return fmt.Errorf("no need of merge")
	}

	var fid int32 // merge fid

	defer func() { GMerging = 0 }()

	toMergeM, err := t.F.Scan2Hash()
	if err != nil {
		log.FnErrLog("scan preview files 2 hashmap failed: %s", err.Error())
		return err
	}

	var wg sync.WaitGroup
	mergeIdxCh := make(chan IndexStuff, MergeIdxChSize)
	wg.Add(1)
	go t.Index.UpdateForMerge(mergeIdxCh, &wg)
	fid, err = t.F.Merge(toMergeM, mergeIdxCh)
	if err != nil {
		log.FnErrLog("FileMgr merge failed: %s", err.Error())
		return err
	}

	wg.Wait()

	err = t.F.RenameMergeFiles(fid)
	if err != nil {
		log.FnErrLog("rename merged files failed: %s", err.Error())
		return err
	}

	t.LastMerge += t.Opt.MergeThreshold
	return nil
}

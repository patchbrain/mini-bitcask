package bitcask

import (
	"encoding/json"
	"mini-bitcask/util/log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	File_Prefix = "data_"
	Header_size = 12 // entry header 的大小
)

type FileMgr struct {
	Cur    *os.File // 当前正在写的文件实例
	bc     *Bitcask
	dir    string
	next   int
	offset int
	sync.Mutex
}

func NewFileMgr(dir string, bc *Bitcask) *FileMgr {
	if len(dir) == 0 {
		log.FnErrLog("get empty dir")
		return nil
	}

	return &FileMgr{dir: dir, next: 1, bc: bc}
}

func (t *FileMgr) CreateFile(write bool) (*os.File, error) {
	t.Lock()
	defer t.Unlock()

	newPath := filepath.Join(t.dir, t.getNextName())
	t.next++

	f, err := os.OpenFile(newPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.FnErrLog("open new file failed: %s", err.Error())
		return nil, err
	}

	if write {
		t.Close()
		t.Cur = f
		t.offset = 0
	}

	return f, nil
}

func (t *FileMgr) getNextName() string {
	return File_Prefix + strconv.Itoa(t.next)
}

func (t *FileMgr) Close() {
	err := t.Cur.Close()
	if err != nil {
		log.FnErrLog("close latest file failed: %s", err.Error())
		os.Exit(1)
	}
}

func (t *FileMgr) Offset() int {
	t.Lock()
	defer t.Unlock()

	return t.offset
}

func (t *FileMgr) Append(entry Entry) error {
	b, err := json.Marshal(entry)
	if err != nil {
		log.FnErrLog("marshal entry failed: %s", err.Error())
		return err
	}

	if int64(t.offset+len(b)) > t.bc.Opt.MaxSingleFileSz {
		log.FnLog("offset exceed file's max size, so new one")
		_, err = t.CreateFile(true)
		if err != nil {
			log.FnErrLog("create new file failed: %s", err.Error())
			return err
		}
	}

	var n int
	n, err = t.Cur.Write(b)
	if err != nil {
		log.FnErrLog("write data failed: %s", err.Error())
		return err
	}

	t.offset += n

	return nil
}

func (t *FileMgr) Read(fid, offset, length int, key string) []byte {
	fPath := filepath.Join(t.dir, File_Prefix+strconv.Itoa(fid))
	f, err := os.Open(fPath)
	if err != nil {
		log.FnErrLog("open file failed: %s", err.Error())
		return nil
	}
	defer func() { _ = f.Close() }()

	dataB := make([]byte, length)
	at := int64(offset + Header_size + len(key) + 1)
	_, err = f.ReadAt(dataB, at)

	return dataB
}

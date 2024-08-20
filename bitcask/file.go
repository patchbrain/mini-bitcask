package bitcask

import (
	"mini-bitcask/util/log"
	"os"
	"path/filepath"
	"strconv"
)

const (
	File_Prefix = "data_"
	Header_size = 12 // entry header 的大小
)

type FileMgr struct {
	Cur        *os.File // 当前正在写的文件实例
	MaxFileSz  int64    // 最大单个文件大小
	dir        string
	next       int32 // 下一个可写文件的编号
	lastOffset int
	offset     int
}

func NewFileMgr(dir string, maxFileSz int64) *FileMgr {
	log.FnDebug("into")
	if len(dir) == 0 {
		log.FnErrLog("get empty dir")
		return nil
	}

	return &FileMgr{dir: dir, next: 1, MaxFileSz: maxFileSz}
}

func (t *FileMgr) CreateFile(write bool) (*os.File, error) {
	log.FnDebug("into")

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
	log.FnDebug("into")
	return File_Prefix + strconv.Itoa(int(t.next))
}

func (t *FileMgr) Close() {
	log.FnDebug("into")
	if t.Cur == nil {
		return
	}

	err := t.Cur.Close()
	if err != nil {
		log.FnErrLog("close latest file failed: %s", err.Error())
		os.Exit(1)
	}
}

func (t *FileMgr) Offset() int {
	log.FnDebug("into")

	return t.offset
}

func (t *FileMgr) LastOffset() int {
	log.FnDebug("into")

	return t.lastOffset
}

func (t *FileMgr) Append(entry Entry) (int32, error) {
	var err error

	log.FnDebug("into")
	b := EncodeEntry(entry)

	if int64(t.offset+len(b)) > t.MaxFileSz {
		log.FnLog("offset exceed file's max size, so new one")
		_, err = t.CreateFile(true)
		if err != nil {
			log.FnErrLog("create new file failed: %s", err.Error())
			return 0, err
		}
	}

	var n int
	n, err = t.Cur.Write(b)
	if err != nil {
		log.FnErrLog("write data failed: %s", err.Error())
		return 0, err
	}

	t.lastOffset = t.offset
	t.offset += n

	return t.next - 1, nil
}

func (t *FileMgr) Read(fid, length int32, offset int, key string) []byte {
	log.FnDebug("into")
	fPath := filepath.Join(t.dir, File_Prefix+strconv.Itoa(int(fid)))
	f, err := os.Open(fPath)
	if err != nil {
		log.FnErrLog("open file failed: %s", err.Error())
		return nil
	}
	defer func() { _ = f.Close() }()

	dataB := make([]byte, length)
	at := int64(offset + Header_size + len(key))
	_, err = f.ReadAt(dataB, at)

	return dataB
}

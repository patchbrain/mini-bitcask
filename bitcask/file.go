package bitcask

import (
	"errors"
	"fmt"
	"io"
	"mini-bitcask/util/log"
	"os"
	"path/filepath"
	"strconv"
)

const (
	File_Prefix  = "data_"
	Merge_Prefix = "merge_"
	Header_size  = 12 // size of entry-header
)

type FileMgr struct {
	// point of File being written
	Cur *os.File

	// when the size of file pointed by Cur has exceeded MaxFileSz, Cur will point the next file
	MaxFileSz int64

	dir string

	// next id for file which is writable
	next int32

	// mergeNext next file id for merge, that is the temporary storage place for kvs filtered by merge
	//mergeNext  int32
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
		t.lastOffset = 0
		t.offset = 0
	}

	return f, nil
}

func (t *FileMgr) getNextName() string {
	log.FnDebug("into")

	var res string
	res = File_Prefix + strconv.Itoa(int(t.next))

	return res
}

func (t *FileMgr) Close() {
	log.FnLog("into")
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

func (t *FileMgr) Append(entry Entry) (int32 /*file id*/, error) {
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

func (t *FileMgr) Read(fid, valLen int32, offset int, key string) []byte {
	log.FnDebug("into")
	fPath := t.GetFilepath(fid)
	f, err := os.Open(fPath)
	if err != nil {
		log.FnErrLog("open file failed: %s", err.Error())
		return nil
	}
	defer func() { _ = f.Close() }()

	dataB := make([]byte, valLen)
	at := int64(offset + Header_size + len(key))
	_, err = f.ReadAt(dataB, at)

	return dataB
}

func (t *FileMgr) Scan2Hash() (map[string]Entry, error) {
	var (
		offset  int       // offset of cur file
		fileIdx int32 = 1 // id of cur file
		res           = make(map[string]Entry)
		err     error
	)

	for ; fileIdx < t.next; fileIdx++ {
		log.FnLog("begin to scan file(%d)...", fileIdx)
		offset = 0
		fName := t.GetFilepath(fileIdx)
		var f *os.File
		f, err = os.Open(fName)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// maybe some file id correspond no file
				log.FnErrLog("file not exists: %s", fName)
				continue
			}
			log.FnErrLog("open scanned file failed: %s", err.Error())
			_ = f.Close()
			return nil, err
		}

		var fileB []byte
		fileB, err = io.ReadAll(f)
		if err != nil {
			log.FnErrLog("seek failed: %s", err.Error())
			_ = f.Close()
			return nil, err
		}

		for offset < len(fileB) {
			var ent Entry
			oldOff := offset
			ent, offset, err = DecodeFrom(offset, fileB)
			if err != nil {
				if errors.Is(err, BytesShortErr) {
					log.FnLog("file is scanned, fid: %d", fileIdx)
					break
				}
				log.FnErrLog("decode from offset(%d) failed: %s", oldOff, err.Error())
				_ = f.Close()
				return nil, err
			}

			old, ok := res[ent.Key]
			if (ok && old.TStamp < ent.TStamp) || !ok {
				// has old value and timestamp is newer, overwrite it
				// no old value, write to map
				res[ent.Key] = ent
			}
		}
	}

	return res, nil
}

func (t *FileMgr) Merge(toMergeM map[string]Entry, idxStuffCh chan IndexStuff) (int32, error) {
	defer close(idxStuffCh)

	var (
		offset, lastOff int
		mergeF          *os.File
		mergeNext       int32 = 1
		err             error
		fName           = t.GetMergeFilepath(mergeNext)
		mergedFiles     = []string{}
	)

	// the first merge file
	mergeF, err = os.OpenFile(fName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return 0, err
	}
	mergeNext++

	for key, val := range toMergeM {
		bEnt := EncodeEntry(val)
		if int64(offset+len(bEnt)) > t.MaxFileSz {
			log.FnLog("create merge file(%d)", mergeNext)
			// need write next merge-file
			_ = mergeF.Close()
			fName = t.GetMergeFilepath(mergeNext)
			mergeF, err = os.OpenFile(fName, os.O_CREATE|os.O_RDWR, 0666)
			if err != nil {
				return 0, err
			}

			offset = 0
			lastOff = 0
			mergeNext++
			mergedFiles = append(mergedFiles, fName)
		}

		var written int
		written, err = mergeF.WriteAt(bEnt, int64(offset))
		if written != len(bEnt) {
			t.removeMergeFiles(mergeNext - 1)
			return 0, fmt.Errorf("length of written byte[] is error, written: %d, need: %d", written, len(bEnt))
		}

		lastOff = offset
		offset += written

		var idxEnt IndexEntry
		idxEnt.FileIdx = mergeNext - 1
		idxEnt.Offset = lastOff
		idxEnt.TStamp = val.TStamp
		idxEnt.ValSz = int32(offset - lastOff - Header_size - len(key))

		op := Update
		if val.Value.Tomb == 1 {
			op = Delete
		}

		idxStuffCh <- NewIndexStuff(op, key, idxEnt)
	}

	_ = mergeF.Close()
	return mergeNext - 1, nil
}

func (t *FileMgr) GetFilepath(fid int32) string {
	return filepath.Join(t.dir, File_Prefix+strconv.Itoa(int(fid)))
}

func (t *FileMgr) GetMergeFilepath(fid int32) string {
	return filepath.Join(t.dir, Merge_Prefix+strconv.Itoa(int(fid)))
}

func (t *FileMgr) RenameMergeFiles(mergeFid int32) error {
	// rename files created by merging
	err := t.renameMergeFiles(mergeFid)
	if err != nil {
		t.removeMergeFiles(mergeFid)
		log.FnErrLog("rename merged files failed: %s, so remove deprecated files", err.Error())
		return err
	}

	return nil
}

func (t *FileMgr) renameMergeFiles(mergeFid int32) error {
	var err error
	for i := int32(1); i <= mergeFid; i++ {
		oldF := t.GetMergeFilepath(i)
		newF := t.GetFilepath(i)

		if err = os.Rename(oldF, newF); err != nil {
			return err
		}
	}

	return nil
}

func (t *FileMgr) removeMergeFiles(mergeFid int32) {
	var err error
	for i := int32(1); i <= mergeFid; i++ {
		fName := t.GetMergeFilepath(i)

		if err = os.Remove(fName); err != nil {
			log.FnErrLog("remove deprecated merged files failed: %s", err.Error())
			return
		}
	}

	return
}

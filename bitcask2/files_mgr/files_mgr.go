package files_mgr

import (
	"github.com/sirupsen/logrus"
	"mini-bitcask/bitcask2/codec"
	_const "mini-bitcask/bitcask2/const"
	"mini-bitcask/bitcask2/model"
	"os"
	"path/filepath"
	"strconv"
)

type Datafile struct {
	fId         int
	r           *os.File
	w           *os.File
	offset      int64
	maxFileSize int
}

func NewDatafile(dir string, fileId int, writable bool, maxFileSz int) (*Datafile, error) {
	df := new(Datafile)

	fName := _const.Datafile_prefix + strconv.Itoa(fileId)
	full := filepath.Join(dir, fName)
	var err error

	if writable {
		df.w, err = os.OpenFile(full, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			logrus.Errorf("create a new writable datafile(fileid: %d) failed: %s", fileId, err.Error())
			return nil, err
		}
	}

	df.r, err = os.OpenFile(full, os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		logrus.Errorf("create a new datafile(fileid: %d) failed: %s", fileId, err.Error())
		return nil, err
	}

	df.fId = fileId
	df.maxFileSize = maxFileSz

	return df, nil
}

func (df *Datafile) FileId() int {
	return df.fId
}

func (df *Datafile) Offset() int64 {
	return df.offset
}

func (df *Datafile) Name() string {
	return _const.Datafile_prefix + strconv.Itoa(df.fId)
}

func (df *Datafile) MaybeRotate() bool {
	if df.Offset() > int64(df.maxFileSize) {
		return true
	}

	return false
}

func (df *Datafile) Put(entry model.Entry) error {
	if df.w == nil {
		return _const.NotWritableErr
	}

	b := codec.Encode(entry)
	if b == nil {
		return _const.EncodeEntryErr
	}

	n, err := df.w.Write(b)
	if err != nil {
		logrus.Errorf("datafile(%d) put entry failed: %s", df.FileId(), err.Error())
		return err
	}
	df.offset += int64(n)

	if n != len(b) {
		logrus.Errorf("datafile(%d): length of putting is not expected", df.FileId())
		return err
	}

	return nil
}

func (df *Datafile) ReadAt(offset, size int64) (model.Entry, error) {
	b := make([]byte, size)
	n, err := df.r.ReadAt(b, offset)
	if err != nil {
		return model.Entry{}, err
	}

	if n != len(b) {
		return model.Entry{}, _const.SizeNotEqualErr
	}

	e := codec.Decode(b)
	if e == nil {
		return model.Entry{}, _const.DecodeEntryErr
	}

	return *e, nil
}

func (df *Datafile) Close() error {
	var err error
	if df.w != nil {
		err = df.w.Sync()
		if err != nil {
			return err
		}

		err = df.w.Close()
		if err != nil {
			return err
		}
	}

	return df.r.Close()
}

func (df *Datafile) AbandonWrite() {
	df.w.Close()
	df.w = nil
}

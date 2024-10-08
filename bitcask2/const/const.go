package _const

import "errors"

const Datafile_prefix = "data_"

const (
	EntryHeaderSize int64 = 20
	Int32Sz         int64 = 4
	Int64Sz         int64 = 8
)

const MergeDir = "merge_dir"

var (
	FileAlreadyExistsErr = errors.New("file already exists, can't create again")
	NotWritableErr       = errors.New("not writable datafile")
	EncodeEntryErr       = errors.New("encode entry error")
	DecodeEntryErr       = errors.New("decode entry bytes error")
	SizeNotEqualErr      = errors.New("expected size not equal to actually gotten")
	InvalidIndexEntErr   = errors.New("invalid index entry")
	InvalidIndexErr      = errors.New("invalid index")
	NoIndexErr           = errors.New("no index")
	FileLockErr          = errors.New("can't get file lock")
	ReadEntryErr         = errors.New("read entry bytes error")
	NoFileToMergeErr     = errors.New("has no file to merge")
	FileNotExist         = errors.New("file not exist")
	IncompleteWriteErr   = errors.New("incomplete write")
)

package _const

import "errors"

const Datafile_prefix = "data_"

var (
	FileAlreadyExistsErr = errors.New("file already exists, can't create again")
	NotWritableErr       = errors.New("not writable datafile")
	EncodeEntryErr       = errors.New("encode entry error")
	DecodeEntryErr       = errors.New("decode entry bytes error")
	SizeNotEqualErr      = errors.New("expected size not equal to actually gotten")
)

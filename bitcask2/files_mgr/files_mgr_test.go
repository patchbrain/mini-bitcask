package files_mgr

import (
	"github.com/stretchr/testify/require"
	_const "mini-bitcask/bitcask2/const"
	"mini-bitcask/bitcask2/model"
	"os"
	"path/filepath"
	"testing"
)

func TestDatafile_Put_Get(t *testing.T) {
	pwd, _ := os.Getwd()
	df, err := NewDatafile(pwd+"/bc", 1, true, 10000)
	require.NoError(t, err)

	err = df.Put(model.NewEntry([]byte("put_key"), []byte("put_my_value"), false))
	require.NoError(t, err)

	var entry model.Entry
	entry, err = df.ReadAt(0, 40)
	require.NoError(t, err)

	t.Logf("entry: %#v", entry)
}

func getFileMgr(dir string) *FileMgr {
	fm := NewFileMgr(dir, 10000)
	return fm
}

func TestFileMgr_LoadDfs(t *testing.T) {
	pwd, _ := os.Getwd()
	dir := filepath.Join(pwd, "bc2")
	os.MkdirAll(dir, 0666)

	files := []string{_const.Datafile_prefix + "1", _const.Datafile_prefix + "2", _const.Datafile_prefix + "3"}
	for _, s := range files {
		os.OpenFile(filepath.Join(dir, s), os.O_RDONLY|os.O_CREATE, 0666)
	}

	defer func() {
		for _, f := range files {
			os.Remove(filepath.Join(dir, f))
		}
	}()

	err := getFileMgr(dir).LoadDfs()
	require.NoError(t, err)
}

func TestFileMgr_Put(t *testing.T) {
	pwd, _ := os.Getwd()
	dir := filepath.Join(pwd, "bc2")
	os.MkdirAll(dir, 0666)
	fm := getFileMgr(dir)

	fid, off, valSz, err := fm.Put([]byte("put_key"), []byte("put_my_value"))
	t.Log(fid, off, valSz, err)

	err = fm.Del([]byte("put_key"))
	t.Log(fid, off, valSz, err)
}

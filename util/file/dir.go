package file

import (
	"fmt"
	"io/fs"
	"mini-bitcask/util/log"
	"os"
	"path/filepath"
)

// EnsureDir make sure that the dir exists
// if clean is true, clean the dir
func EnsureDir(dir string, clean bool) error {
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		log.FnLog("Directory created: %s", dir)
	} else if err != nil {
		// 其他错误
		return fmt.Errorf("failed to stat directory: %w", err)
	} else if !info.IsDir() {
		// 路径存在，但不是目录
		return fmt.Errorf("path exists but is not a directory: %s", dir)
	}

	log.FnLog("Directory already exists: %s, need clean: %t", dir, clean)
	if clean {
		err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if path == dir {
				return nil
			}

			if err != nil {
				log.FnErrLog("encounter a error when walking file(%s): %s", path, err.Error())
				return err
			}

			if d.IsDir() {
				err = os.RemoveAll(path)
				if err != nil {
					log.FnErrLog("remove all of dir(%s) failed: %s", path, err.Error())
				}
				return err
			}

			err = os.Remove(path)
			if err != nil {
				log.FnErrLog("remove file(%s) failed: %s", path, err.Error())
			}
			return err
		})
		if err != nil {
			return fmt.Errorf("walk dir error: %s", err.Error())
		}
	}

	return nil
}

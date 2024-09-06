package file

import (
	"github.com/sirupsen/logrus"
	"os"
)

func IsFileExist(fullpath string) bool {
	_, err := os.Stat(fullpath)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}

	logrus.Errorf("meet a error: %s, path: %s", err.Error(), fullpath)
	return false
}

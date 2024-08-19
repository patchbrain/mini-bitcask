package log

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"mini-bitcask/util/runtime"
)

var level = logrus.InfoLevel

func init() {
	logrus.SetLevel(level)
}

func FnLog(msg string, args ...interface{}) {
	logrus.Infof("@%s: %s", runtime.GetCurFuncName(2), fmt.Sprintf(msg, args...))
}

func FnErrLog(msg string, args ...interface{}) {
	logrus.Errorf("@%s: %s", runtime.GetCurFuncName(2), fmt.Sprintf(msg, args...))
}

func FnDebug(msg string, args ...interface{}) {
	logrus.Debugf("@%s: %s", runtime.GetCurFuncName(2), fmt.Sprintf(msg, args...))
}

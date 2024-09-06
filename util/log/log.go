package log

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"mini-bitcask/util/runtime"
	"os"
)

var level = logrus.InfoLevel

func init() {
	logFile := "./out.log"
	logrus.SetLevel(level)
	f, _ := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR, 0666)
	logrus.SetOutput(f)
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

package log

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"mini-bitcask/util/runtime"
)

func FnLog(msg string, args ...interface{}) {
	logrus.Infof("@%s: %s", runtime.GetCurFuncName(2), fmt.Sprintf(msg, args...))
}

func FnErrLog(msg string, args ...interface{}) {
	logrus.Errorf("@%s: %s", runtime.GetCurFuncName(2), fmt.Sprintf(msg, args...))
}

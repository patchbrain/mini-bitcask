package log

import (
	"github.com/sirupsen/logrus"
	"mini-bitcask/util/runtime"
)

func FnLog(msg string, args ...interface{}) {
	logrus.Infof("@%s: "+msg, runtime.GetCurFuncName(2), args)
}

func FnErrLog(msg string, args ...interface{}) {
	logrus.Errorf("@%s: "+msg, runtime.GetCurFuncName(2), args)
}

package runtime

import (
	"path/filepath"
	"runtime"
)

func GetCurFuncName(skip ...int) string {
	acSkip := 1
	if len(skip) == 1 {
		acSkip = skip[0]
	}
	if acSkip < 0 {
		acSkip = 1
	}

	pc, _, _, _ := runtime.Caller(acSkip) // 获取调用者的程序计数器
	fn := runtime.FuncForPC(pc)           // 获取对应的函数
	return filepath.Base(fn.Name())       // 获取函数名
}

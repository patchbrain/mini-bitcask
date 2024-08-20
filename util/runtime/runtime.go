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

	pc, _, _, _ := runtime.Caller(acSkip)
	fn := runtime.FuncForPC(pc)
	return filepath.Base(fn.Name())
}

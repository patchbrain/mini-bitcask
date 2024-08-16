package runtime

import (
	"log/slog"
	"testing"
)

func TestGetCurFuncName(t *testing.T) {
	fn := GetCurFuncName()
	slog.Info("1: " + fn)
	Outer()

	fn = GetCurFuncName(2)
	slog.Info("3: " + fn)

	fn = GetCurFuncName(-1)
	slog.Info("4: " + fn)

	fn = GetCurFuncName(0)
	slog.Info("5: " + fn)
}

func Outer() {
	fn := GetCurFuncName()
	slog.Info("2: " + fn)
}

package util

import "testing"

func TestFnLog(t *testing.T) {
	FnLog("test log: %s", "for test")
	FnErrLog("test err log: %s", "for test err log")
}

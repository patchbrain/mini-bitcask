package main

import (
	"testing"
)

func TestOpen_set_get(t *testing.T) {
	err := bc.Set("test1", 123)
	if err != nil {
		t.Errorf("set value1 error: %s", err.Error())
		return
	}

	err = bc.Set("test2", struct {
		Name string
		Age  int
	}{"kinghno3", 12})
	if err != nil {
		t.Errorf("set value2 error: %s", err.Error())
		return
	}

	v := bc.Get("test1")
	t.Logf("get value: %s", string(v))

	v2 := bc.Get("test2")
	t.Logf("get value: %s", string(v2))

	return
}

package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDelAndGet(t *testing.T) {
	key := "needDel"
	err := bc.Set(key, `{"name": "hno3"}`)
	if err != nil {
		t.Errorf("set error: %s", err.Error())
		return
	}

	v := bc.Get(key)
	t.Logf("get value: %s", string(v))
	assert.NotNil(t, v)

	err = bc.Del(key)
	t.Logf("del: %s", key)
	assert.NoError(t, err)

	v2 := bc.Get(key)
	t.Logf("get nil value: %s", string(v2))
	assert.Nil(t, v2)
	return
}

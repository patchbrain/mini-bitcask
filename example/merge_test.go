package main

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"mini-bitcask/bitcask"
	"mini-bitcask/util/strings"
	"testing"
	"time"
)

func TestRepeatKey(t *testing.T) {
	key := "test_key"
	size := 100000
	var lastVal string

	for i := 0; i < size; i++ {
		lastVal = strings.GetRandomStr(10)
		err := bc.Set(key, lastVal)
		if err != nil {
			t.Errorf("set error: %s", err.Error())
			return
		}

		if bitcask.GMerging == 1 {
			// meet merge and break
			break
		}
	}

	for bitcask.GMerging == 1 {
		// wait for merging
		t.Log("wait for merging")
		time.Sleep(time.Second)
	}
	t.Log("merge done")

	var actual string
	err := json.Unmarshal(bc.Get(key), &actual)
	assert.NoError(t, err)
	assert.Equal(t, lastVal, actual)

	return
}

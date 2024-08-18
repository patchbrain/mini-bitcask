package main

import (
	"math/rand"
	"mini-bitcask/util/strings"
	"testing"
)

// 目前只测试了value类型为string的情况
func TestMulFiles(t *testing.T) {
	r := rand.New(rand.NewSource(rand.Int63()))
	size := 120000
	kvM := make(map[string]interface{}, size)
	checkKeys := make([]string, 0)

	for i := 0; i < size; i++ {
		key := "key_" + strings.GetCurrentStr(r.Intn(10))
		val := "value_" + strings.GetCurrentStr(r.Intn(100))
		err := bc.Set(key, val)
		if err != nil {
			t.Errorf("set error: %s", err.Error())
			return
		}

		kvM[key] = val

		if r.Float64() < 0.001 {
			checkKeys = append(checkKeys, key)
		}
	}

	for _, key := range checkKeys {
		actual := string(bc.Get(key))
		want := `"` + kvM[key].(string) + `"`

		if actual != want {
			t.Errorf("incorrect value, actual: %s, want: %s", actual, want)
			return
		}
	}
}

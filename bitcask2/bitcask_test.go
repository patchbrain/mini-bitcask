package bitcask2

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"mini-bitcask/util/strings"
	"os"
	"path/filepath"
	"testing"
)

func TestBitcask_Put(t *testing.T) {
	pwd, _ := os.Getwd()
	dir := filepath.Join(pwd, "bc_test")
	os.MkdirAll(dir, 0666)

	b, err := Open(dir, NewOption(WithMaxFileSz(1024*5)))
	if err != nil {
		t.Fatal(err)
	}

	checkKeys := map[string][]byte{}
	for i := 0; i < 1000; i++ {
		key := strings.GetRandomStr(rand.Intn(20) + 1)
		value := strings.GetRandomStr(rand.Intn(100) + 1)
		err = b.Put([]byte(key), []byte(value))
		require.NoError(t, err)

		if rand.Float64() < 0.003 {
			checkKeys[key] = []byte(value)
			t.Logf("add check kv, key: %s, value: %s", key, value)
		}
	}

	for k, v := range checkKeys {
		val, err := b.Get([]byte(k))
		require.NoError(t, err)
		require.Equal(t, string(v), string(val))
	}
}

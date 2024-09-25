package index

import (
	"math/rand"
	"mini-bitcask/util/strings"
	"os"
	"path/filepath"
	"testing"
)

func TestIndexer_SaveIndexes(t *testing.T) {
	idxer := NewIndexer()

	keys := []string{}
	ies := []IndexEntry{}
	for i := 0; i < 1; i++ {
		key := strings.GetRandomStr(rand.Intn(20) + 1)
		ie := IndexEntry{
			FileId:  rand.Int31n(10) + 1,
			Offset:  rand.Int63n(10000000),
			ValueSz: rand.Int63n(10000000),
		}
		keys = append(keys, key)
		ies = append(ies, ie)
	}

	var err error
	for i, key := range keys {
		t.Logf("key: %s, ie: %#v", key, ies[i])
		err = idxer.Add(Key(key), ies[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	pwd, _ := os.Getwd()
	path := filepath.Join(pwd, "test_idx")
	err = idxer.SaveIndexes(path)
	if err != nil {
		t.Fatal(err)
	}
}

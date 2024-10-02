package index

import (
	"math/rand"
	"mini-bitcask/util/strings"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func saveIndexes(indexer Indexer, t *testing.T) {
	pwd, _ := os.Getwd()
	path := filepath.Join(pwd, "test_idx")
	err := indexer.SaveIndexes(path)
	if err != nil {
		t.Fatal(err)
	}
}

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

	saveIndexes(idxer, t)
}

func TestIndexer_LoadIndexes(t *testing.T) {
	idxer := NewIndexer()

	keys := []string{}
	ies := []IndexEntry{}
	for i := 0; i < 1000; i++ {
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

	saveIndexes(idxer, t)

	pwd, _ := os.Getwd()
	path := filepath.Join(pwd, "test_idx")
	idxer2 := NewIndexer()
	if err = idxer2.LoadIndexes(path); err != nil {
		t.Fatal(err)
	}

	if len(idxer2.Keys()) != len(idxer.Keys()) {
		t.Fatal("different keys")
	}

	idxer2.Foreach(func(key Key, entry IndexEntry) error {
		val, cErr := idxer.Get(key)
		if cErr != nil {
			t.Fatal(cErr)
		}

		if !reflect.DeepEqual(entry, val) {
			t.Fatalf("idxer2 entry: %#v, expect: %#v", entry, val)
		}

		return nil
	})
}

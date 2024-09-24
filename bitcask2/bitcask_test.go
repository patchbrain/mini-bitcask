package bitcask2

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"math/rand"
	"mini-bitcask/util/strings"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func init() {
	log := logrus.New()

	log.SetReportCaller(true)

	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			// 仅返回文件名和行号
			return "", fmt.Sprintf("%s:%d", filepath.Base(frame.File), frame.Line)
		},
	})

	file, err := os.OpenFile("bitcask.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err == nil {
		log.SetOutput(file)
	} else {
		log.Warn("Failed to log to file, using default stderr")
	}

	log.SetLevel(logrus.InfoLevel)

	logrus.SetFormatter(log.Formatter)
	logrus.SetReportCaller(log.ReportCaller)
	logrus.SetOutput(log.Out)
	logrus.SetLevel(logrus.GetLevel())
}

func putValues(t *testing.T, b *Bitcask, num int) (sample map[string][]byte, err error) {
	sample = make(map[string][]byte, num)
	for i := 0; i < num; i++ {
		key := strings.GetRandomStr(rand.Intn(20) + 1)
		value := strings.GetRandomStr(rand.Intn(100) + 1)
		err = b.Put([]byte(key), []byte(value))
		require.NoError(t, err)

		if rand.Float64() < 0.1 {
			sample[key] = []byte(value)
		}
	}

	return
}

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

func TestMerge(t *testing.T) {
	pwd, _ := os.Getwd()
	dir := filepath.Join(pwd, "bc_test")
	os.MkdirAll(dir, 0666)

	b, err := Open(dir, NewOption(WithMaxFileSz(1024*5)))
	if err != nil {
		t.Fatal(err)
	}

	check, err := putValues(t, b, 1000)
	if err != nil {
		t.Fatal(err)
	}

	if err = b.Merge(); err != nil {
		t.Fatal(err)
	}

	for k, v := range check {
		actual, err := b.Get([]byte(k))
		if err != nil {
			t.Fatalf("get kv failed: %s, key: %s", err.Error(), k)
		}

		require.Equal(t, string(v), string(actual))
	}

	return
}

func TestMergeAndPut(t *testing.T) {
	pwd, _ := os.Getwd()
	dir := filepath.Join(pwd, "bc_test")
	os.MkdirAll(dir, 0666)

	b, err := Open(dir, NewOption(WithMaxFileSz(1024*5)))
	if err != nil {
		t.Fatal(err)
	}

	check, err := putValues(t, b, 1000)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err = b.Merge(); err != nil {
			t.Fatal(err)
		}
	}()

	go func() {
		if err = b.Put([]byte("when_merge"), []byte("when_merge_value")); err != nil {
			t.Fatal(err)
		}
	}()

	check["when_merge"] = []byte("when_merge_value")
	for k, v := range check {
		actual, err := b.Get([]byte(k))
		if err != nil {
			t.Fatalf("get kv failed: %s, key: %s", err.Error(), k)
		}

		require.Equal(t, string(v), string(actual))
	}

	return
}

func TestMergeAfterDel(t *testing.T) {
	pwd, _ := os.Getwd()
	dir := filepath.Join(pwd, "bc_test")
	os.MkdirAll(dir, 0666)

	b, err := Open(dir, NewOption(WithMaxFileSz(1024*5)))
	if err != nil {
		t.Fatal(err)
	}

	check, err := putValues(t, b, 1000)
	if err != nil {
		t.Fatal(err)
	}

	var delKey string
	for k := range check {
		delKey = k
		if err = b.Del([]byte(k)); err != nil {
			t.Fatal(err)
		}
		break
	}

	if err = b.Merge(); err != nil {
		t.Fatal(err)
	}

	v, err := b.Get([]byte(delKey))
	if err != nil {
		t.Fatal(err)
	}

	require.Nil(t, v)

	return
}

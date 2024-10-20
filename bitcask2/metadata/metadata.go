package metadata

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Metadata struct {
	IsHintUpToDated bool `json:"isHintUpToDated"`
}

const MetadataFilename = ".metadata"

func (m *Metadata) Save(dir string) error {
	path := filepath.Join(dir, MetadataFilename)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	b, _ := json.Marshal(m)
	if _, err = f.Write(b); err != nil {
		return err
	}

	return nil
}

func (m *Metadata) Load(dir string) error {
	path := filepath.Join(dir, MetadataFilename)
	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, m); err != nil {
		return err
	}

	return nil
}

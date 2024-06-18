package io

import (
	"os"
	"path/filepath"

	icebergIo "github.com/apache/iceberg-go/io"
)

// LocalFS is an implementation of IO that implements interaction with
// the local file system.
type LocalFS struct{}

func (LocalFS) Open(name string) (icebergIo.File, error) {
	return os.Open(name)
}

func (LocalFS) OpenWrite(name string) (OutFile, error) {
	dirPath := filepath.Dir(name)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return os.Create(name)
}

func (LocalFS) Remove(name string) error {
	return os.Remove(name)
}

package io

import (
	"fmt"
	"io"
	"io/fs"
	"net/url"

	icebergIo "github.com/apache/iceberg-go/io"
)

// IO is an interface to a hierarchical file system.
//
// The IO interface is the minimum implementation required for a file
// system to utilize an iceberg table. A file system may implement
// additional interfaces, such as ReadFileIO, to provide additional or
// optimized functionality.
type IO interface {
	icebergIo.IO

	// Open opens the named file.
	//
	// When Open returns an error, it should be of type *PathError
	// with the Op field set to "open", the Path field set to name,
	// and the Err field describing the problem.
	//
	// Open should reject attempts to open names that do not satisfy
	// fs.ValidPath(name), returning a *PathError with Err set to
	// ErrInvalid or ErrNotExist.
	OpenWrite(name string) (OutFile, error)
}

// ReadFileIO is the interface implemented by a file system that
// provides an optimized implementation of ReadFile.
type ReadFileIO interface {
	IO

	// ReadFile reads the named file and returns its contents.
	// A successful call returns a nil error, not io.EOF.
	// (Because ReadFile reads the whole file, the expected EOF
	// from the final Read is not treated as an error to be reported.)
	//
	// The caller is permitted to modify the returned byte slice.
	// This method should return a copy of the underlying data.
	ReadFile(name string) ([]byte, error)
}

type WriteFileIO interface {
	IO

	WriteFile(name string) ([]byte, error)
}

// A File provides access to a single file. The File interface is the
// minimum implementation required for Iceberg to interact with a file.
// Directory files should also implement
type OutFile interface {
	icebergIo.File

	io.Writer
}

// A ReadDirFile is a directory file whose entries can be read with the
// ReadDir method. Every directory file should implement this interface.
// (It is permissible for any file to implement this interface, but
// if so ReadDir should return an error for non-directories.)
type ReadDirFile interface {
	icebergIo.File

	// ReadDir read the contents of the directory and returns a slice
	// of up to n DirEntry values in directory order. Subsequent calls
	// on the same file will yield further DirEntry values.
	//
	// If n > 0, ReadDir returns at most n DirEntry structures. In this
	// case, if ReadDir returns an empty slice, it will return a non-nil
	// error explaining why.
	//
	// At the end of a directory, the error is io.EOF. (ReadDir must return
	// io.EOF itself, not an error wrapping io.EOF.)
	//
	// If n <= 0, ReadDir returns all the DirEntry values from the directory
	// in a single slice. In this case, if ReadDir succeeds (reads all the way
	// to the end of the directory), it returns the slice and a nil error.
	// If it encounters an error before the end of the directory, ReadDir
	// returns the DirEntry list read until that point and a non-nil error.
	ReadDir(n int) ([]fs.DirEntry, error)
}

func LoadFS(path string) (IO, error) {
	parsed, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	switch parsed.Scheme {
	case "file", "":
		return LocalFS{}, nil
	// case "s3", "s3a", "s3n":
	// 	return BlobFileIO{blob.OpenBucket()}
	default:
		return nil, fmt.Errorf("IO for file '%s' not implemented", path)
	}
}

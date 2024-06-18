package io

import (
	"context"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	icebergIo "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob"
)

// iofsFileInfo describes a single file in an io/fs.FS.
// It implements fs.FileInfo and fs.DirEntry.
type iofsFileInfo struct {
	lo   *blob.ListObject
	name string
}

func (f *iofsFileInfo) Name() string               { return f.name }
func (f *iofsFileInfo) Size() int64                { return f.lo.Size }
func (f *iofsFileInfo) Mode() fs.FileMode          { return fs.ModeIrregular }
func (f *iofsFileInfo) ModTime() time.Time         { return f.lo.ModTime }
func (f *iofsFileInfo) IsDir() bool                { return false }
func (f *iofsFileInfo) Sys() interface{}           { return f.lo }
func (f *iofsFileInfo) Info() (fs.FileInfo, error) { return f, nil }
func (f *iofsFileInfo) Type() fs.FileMode          { return fs.ModeIrregular }

// iofsDir describes a single directory in an io/fs.FS.
// It implements fs.FileInfo, fs.DirEntry, and fs.File.
type iofsDir struct {
	b    *BlobFileIO
	key  string
	name string
	// If opened is true, we've read entries via openOnce().
	opened  bool
	entries []fs.DirEntry
	offset  int
}

func newDir(b *BlobFileIO, key, name string) *iofsDir {
	return &iofsDir{b: b, key: key, name: name}
}

func (d *iofsDir) Name() string               { return d.name }
func (d *iofsDir) Size() int64                { return 0 }
func (d *iofsDir) Mode() fs.FileMode          { return fs.ModeDir }
func (d *iofsDir) Type() fs.FileMode          { return fs.ModeDir }
func (d *iofsDir) ModTime() time.Time         { return time.Time{} }
func (d *iofsDir) IsDir() bool                { return true }
func (d *iofsDir) Sys() interface{}           { return d }
func (d *iofsDir) Info() (fs.FileInfo, error) { return d, nil }
func (d *iofsDir) Stat() (fs.FileInfo, error) { return d, nil }
func (d *iofsDir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.key, Err: fs.ErrInvalid}
}
func (d *iofsDir) ReadAt(p []byte, off int64) (int, error) {
	return 0, &fs.PathError{Op: "readAt", Path: d.key, Err: fs.ErrInvalid}
}
func (d *iofsDir) Seek(offset int64, whence int) (int64, error) {
	return 0, &fs.PathError{Op: "seek", Path: d.key, Err: fs.ErrInvalid}
}
func (d *iofsDir) Close() error { return nil }
func (d *iofsDir) ReadDir(count int) ([]fs.DirEntry, error) {
	if err := d.openOnce(); err != nil {
		return nil, err
	}
	n := len(d.entries) - d.offset
	if n == 0 && count > 0 {
		return nil, io.EOF
	}
	if count > 0 && n > count {
		n = count
	}
	list := make([]fs.DirEntry, n)
	for i := range list {
		list[i] = d.entries[d.offset+i]
	}
	d.offset += n
	return list, nil
}
func (d *iofsDir) openOnce() error {
	if d.opened {
		return nil
	}
	d.opened = true

	// blob expects directories to end in the delimiter, except at the top level.
	prefix := d.key
	if prefix != "" {
		prefix += "/"
	}
	listOpts := blob.ListOptions{
		Prefix:    prefix,
		Delimiter: "/",
	}
	ctx := d.b.ctx

	// Fetch all the directory entries.
	// Conceivably we could only fetch a few here, and fetch the rest lazily
	// on demand, but that would add significant complexity.
	iter := d.b.List(&listOpts)
	for {
		item, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := filepath.Base(item.Key)
		if item.IsDir {
			d.entries = append(d.entries, newDir(d.b, item.Key, name))
		} else {
			d.entries = append(d.entries, &iofsFileInfo{item, name})
		}
	}
	// There is no such thing as an empty directory in Bucket, so if
	// we didn't find anything, it doesn't exist.
	if len(d.entries) == 0 {
		return fs.ErrNotExist
	}
	return nil
}

type blobReadFile struct {
	*blob.Reader
	name string
}

func (f *blobReadFile) ReadAt(p []byte, off int64) (int, error) {
	// TODO: Find a way to implement using Bucket.NewReaderRange
	finalOff, err := f.Reader.Seek(off, io.SeekStart)
	if err != nil {
		return -1, err
	} else if finalOff != off {
		return -1, io.ErrUnexpectedEOF
	}

	return f.Read(p)
}

func (f *blobReadFile) Name() string               { return f.name }
func (f *blobReadFile) Mode() fs.FileMode          { return fs.ModeIrregular }
func (f *blobReadFile) Sys() interface{}           { return f.Reader }
func (f *blobReadFile) IsDir() bool                { return false }
func (f *blobReadFile) Stat() (fs.FileInfo, error) { return f, nil }

type BlobFileIO struct {
	*blob.Bucket
	ctx  context.Context
	opts *blob.ReaderOptions
}

func (io *BlobFileIO) Open(path string) (icebergIo.File, error) {
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{Op: "open", Path: path, Err: fs.ErrInvalid}
	}

	var isDir bool
	var key, name string // name is the last part of the path
	if path == "." {
		// Root is always a directory, but blob doesn't want the "." in the key.
		isDir = true
		key, name = "", "."
	} else {
		exists, _ := io.Bucket.Exists(io.ctx, path)
		isDir = !exists
		key, name = path, filepath.Base(path)
	}

	// If it's a directory, list the directory contents. We can't do this lazily
	// because we need to error out here if it doesn't exist.
	if isDir {
		dir := newDir(io, key, name)
		err := dir.openOnce()
		if err != nil {
			if err == fs.ErrNotExist && path == "." {
				// The root directory must exist.
				return dir, nil
			}
			return nil, &fs.PathError{Op: "open", Path: path, Err: err}
		}
		return dir, nil
	}

	// It's a file; open it and return a wrapper.
	r, err := io.Bucket.NewReader(io.ctx, path, io.opts)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: path, Err: err}
	}

	return &blobReadFile{Reader: r, name: name}, nil
}

// func (io *BlobFileIO) Create(path string) (OutFile, error) {
// 	f, _ := io.Bucket.NewWriter(io.ctx, path, nil)
// 	f.Write()
// 	io.Bucket.Delete()

// }

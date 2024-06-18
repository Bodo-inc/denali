package view

import (
	ourIo "github.com/Bodo-inc/denali/iceberg-overload/io"
	"github.com/apache/iceberg-go/table"
)

type View struct {
	Identifier       table.Identifier
	Metadata         *Metadata
	MetadataLocation string
	Fs               ourIo.IO
}

func New(ident table.Identifier, meta *Metadata, metaLoc string, fs ourIo.IO) *View {
	return &View{
		Identifier:       ident,
		Metadata:         meta,
		MetadataLocation: metaLoc,
		Fs:               fs,
	}
}

func NewFromLocation(ident table.Identifier, metaLoc string, fs ourIo.IO) (*View, error) {
	meta := new(Metadata)

	if rf, ok := fs.(ourIo.ReadFileIO); ok {
		data, err := rf.ReadFile(metaLoc)
		if err != nil {
			return nil, err
		}

		if meta, err = ParseMetadataBytes(data); err != nil {
			return nil, err
		}
	} else {
		f, err := fs.Open(metaLoc)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		if meta, err = ParseMetadata(f); err != nil {
			return nil, err
		}
	}
	return New(ident, meta, metaLoc, fs), nil
}

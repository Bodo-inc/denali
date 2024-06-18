package view

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"

	"github.com/Bodo-inc/denali/common"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

type Representation struct {
	Type    string `json:"type"`
	Sql     string `json:"sql"`
	Dialect string `json:"dialect"`
}

type Version struct {
	VersionId        int                  `json:"version-id"`
	TimestampMs      uint64               `json:"timestamp-ms"`
	SchemaId         int                  `json:"schema-id"`
	Summary          map[string]string    `json:"summary"`
	DefaultCatalog   *string              `json:"default-catalog" required:"false"`
	DefaultNamespace []common.NamespaceID `json:"default-namespace"`
	Representations  []Representation     `json:"representations"`
}

type VersionLog struct {
	TimestampMs uint64 `json:"timestamp-ms"`
	VersionId   int    `json:"version-id"`
}

type Metadata struct {
	ViewId           uuid.UUID          `json:"view-uuid"`
	FormatVersion    int                `json:"format-version"`
	Location         string             `json:"location"`
	Schemas          []*iceberg.Schema  `json:"schemas"`
	CurrentVersionId int                `json:"current-version-id"`
	Versions         []Version          `json:"versions"`
	VersionLog       []VersionLog       `json:"version-log"`
	Properties       iceberg.Properties `json:"properties,omitempty" required:"false" `
}

// ParseMetadata parses a reader of gzip compressed json metadata
// and returns a Metadata object, returning an error if one is encountered.
func ParseMetadata(r io.Reader) (*Metadata, error) {
	unzipper, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}

	defer unzipper.Close()

	ret := new(Metadata)
	return ret, json.NewDecoder(unzipper).Decode(ret)
}

// ParseMetadataBytes parses a reader of gzip compressed json metadata using [ParseMetadata]
func ParseMetadataBytes(b []byte) (*Metadata, error) {
	return ParseMetadata(bytes.NewReader(b))
}

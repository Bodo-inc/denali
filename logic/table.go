package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"github.com/Bodo-inc/denali/common"
	"github.com/apache/iceberg-go"
	icebergTable "github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

func (qtx *Tx) ListTables(ctx context.Context, nsID common.NamespaceID) ([]string, error) {
	// Find the namespace PK from the path
	nsPK, err := qtx.GetNamespacePKHelper(ctx, nsID)
	if err != nil {
		return nil, err
	}

	// Get all tables under this namespace
	return qtx.Queries.ListTables(ctx, nsPK)
}

type TableInfo struct {
	Pk                 int64
	MetadataLocation   string
	LastSequenceNumber int64
}

func (qtx *Tx) GetTableInfo(ctx context.Context, id common.TableIdentifier) (*TableInfo, error) {
	nsPk, err := qtx.GetNamespacePKHelper(ctx, id.Namespace)
	if err != nil {
		return nil, err
	}

	out, err := qtx.Queries.GetTableHelper(ctx, id.Name, nsPk)
	if err != nil {
		return nil, &common.TableNotFoundError{Id: id}
	}

	return &TableInfo{
		Pk:                 out.Pk,
		MetadataLocation:   out.MetadataLocation,
		LastSequenceNumber: out.LastSequenceNumber,
	}, nil
}

// TableFromMetadata loads an Iceberg table from the given location.
func (s State) TableFromMetadata(id common.TableIdentifier, metadataLoc string) (*icebergTable.Table, error) {
	return icebergTable.NewFromLocation(id.ToIcebergID(), metadataLoc, s.io)
}

// ------------- Writer Helper Functions -------------

func (s *State) CreateTableMetadata(
	schema *iceberg.Schema,
	properties iceberg.Properties,
	specFields []iceberg.PartitionField,
	sortFields []icebergTable.SortField,
) icebergTable.Metadata {

	// Construct base metadata
	meta := new(icebergTable.MetadataV2)
	meta.LastSequenceNumber = 0
	meta.FormatVersion = 2
	meta.UUID = uuid.New()
	meta.Loc = s.TableUUIDToPath(meta.UUID)
	meta.LastUpdatedMS = time.Now().UnixMilli()

	// Set schema
	schema.ID = 0
	meta.SchemaList = []*iceberg.Schema{schema}
	meta.CurrentSchemaID = 0
	meta.LastColumnId = schema.HighestFieldID()

	// Set given table properties
	meta.Props = properties

	// Set partition spec to given or unpartitioned if nothing
	meta.LastPartitionID = new(int)
	// General offset
	*meta.LastPartitionID = 999
	for _, f := range specFields {
		*meta.LastPartitionID = max(*meta.LastPartitionID, f.FieldID)
	}
	spec := iceberg.NewPartitionSpec(specFields...)
	meta.Specs = []iceberg.PartitionSpec{spec}
	meta.DefaultSpecID = 0

	// Set sort order to given or unsorted if nothing
	var sf []icebergTable.SortField = []icebergTable.SortField{}
	if sortFields != nil {
		sf = sortFields
	}
	sortOrder := icebergTable.SortOrder{OrderID: 0, Fields: sf}
	meta.SortOrderList = []icebergTable.SortOrder{sortOrder}
	meta.DefaultSortOrderID = 0

	// Fill out history fields with empty content
	// Note: Nil slices & maps are marshalled to null in JSON rather than empty
	// So need to explicitly set them
	meta.SnapshotList = []icebergTable.Snapshot{}
	meta.SnapshotLog = []icebergTable.SnapshotLogEntry{}
	meta.MetadataLog = []icebergTable.MetadataLogEntry{}
	meta.Refs = map[string]icebergTable.SnapshotRef{}

	return meta
}

func (s *State) WriteTableMetadata(id common.TableIdentifier, meta icebergTable.Metadata, lastSeqNum int64) (*icebergTable.Table, error) {

	// Determine location for metadata file
	metadataLoc, err := url.JoinPath(
		filepath.Join(s.TableUUIDToPath(meta.TableUUID()), "metadata"),
		fmt.Sprintf("v%d-%v.metadata.json", lastSeqNum, uuid.New()),
	)
	if err != nil {
		return nil, err
	}

	// Write metadata file
	table := icebergTable.New(id.ToIcebergID(), meta, metadataLoc, s.io)
	f, err := s.io.OpenWrite(table.MetadataLocation())
	if err != nil {
		return nil, err
	}

	out_bytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, err
	}

	f.Write(out_bytes)
	return table, nil
}

func (qtx *Tx) RegisterTable(ctx context.Context, table *icebergTable.Table) error {
	// Construct table id
	tableID := common.TableIdentifier{}.FromIcebergID(table.Identifier())

	// Find the namespace PK from the path
	nsPK, err := qtx.GetNamespacePKHelper(ctx, tableID.Namespace)
	if err != nil {
		return err
	}

	// Last Sequence Number if Available
	var lastSeqNum int64 = 0
	if table.Metadata().CurrentSnapshot() != nil {
		lastSeqNum = table.Metadata().CurrentSnapshot().SequenceNumber
	}

	// Register the table in the database
	err = qtx.Queries.RegisterTable(
		ctx, tableID.Name, nsPK, table.MetadataLocation(),
		lastSeqNum,
	)
	if err != nil {
		return err
	}

	return nil
}

// Rename a View and Optionally Move it to a New Namespace
// If a new namespace is not provided, the view will remain in the same namespace
func (qtx *Tx) RenameTable(ctx context.Context, pk int64, newName string, newNs common.NamespaceID) error {
	var rows int64
	var err error

	if newNs != nil {
		newNsPk, err := qtx.GetNamespacePKHelper(ctx, newNs)
		if err != nil {
			return err
		}

		rows, err = qtx.Queries.RenameAndMoveTable(ctx, newName, newNsPk, pk)
		if err != nil {
			return err
		}
	} else {
		rows, err = qtx.Queries.RenameTable(ctx, newName, pk)
	}

	if err != nil {
		return err
	} else if rows == 0 {
		return &common.TableNotFoundError{Id: common.TableIdentifier{Namespace: newNs, Name: newName}}
	} else if rows > 1 {
		return fmt.Errorf("renamed more than one view when renaming view")
	} else {
		return nil
	}
}

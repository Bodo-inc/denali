package logic

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"

	"github.com/apache/iceberg-go"
	icebergTable "github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	icebergView "github.com/Bodo-inc/denali/iceberg-overload/view"
)

// UpdateOp is an interface for all update operations for tables and views
// Not all operations are supported for each object
type UpdateOp interface{}

type TableUpdateOp interface {
	UpdateOp
	TableApply(table *icebergTable.Table) error
}

type ViewUpdateOp interface {
	UpdateOp
	ViewApply(view *icebergView.View) error
}

// -------------------- Common Update Defs --------------------

type AssignUUIDUpdate struct {
	Uuid uuid.UUID `json:"uuid" required:"true"`
}

func (up AssignUUIDUpdate) TableApply(table *icebergTable.Table) error {
	return fmt.Errorf("assign-uuid is not implemented for tables because transactions are not available")
}

func (up AssignUUIDUpdate) ViewApply(view *icebergView.View) error {
	return fmt.Errorf("assign-uuid is not implemented for views because it is set during creation")
}

// Upgrade the current format version
type UpgradeFormatVersionUpdate struct {
	FormatVersion int `json:"format-version"`
}

func (up UpgradeFormatVersionUpdate) TableApply(table *icebergTable.Table) error {
	meta := table.Metadata()

	if up.FormatVersion != 1 && up.FormatVersion != 2 {
		return fmt.Errorf("format version %v is invalid; the only allowed format versions for tables are v1 and v2", up.FormatVersion)
	} else if up.FormatVersion < meta.Version() {
		return fmt.Errorf("cannot downgrade format version from %v to %v", meta.Version(), up.FormatVersion)
	}

	// Convert MetadataV1 to MetadataV2 as necessary
	if metaV1, ok := meta.(*icebergTable.MetadataV1); ok && up.FormatVersion == 2 {
		metaV2 := metaV1.ToV2()
		icebergTable.New(table.Identifier(), &metaV2, table.MetadataLocation(), table.FS())
	}
	return nil
}

func (up UpgradeFormatVersionUpdate) ViewApply(view *icebergView.View) error {
	if up.FormatVersion != 1 {
		return fmt.Errorf("format version %v is invalid; the only allowed format version for views is v1", up.FormatVersion)
	}

	// Because views are always v1, we don't need to update anything
	return nil
}

type AddSchemaUpdate struct {
	Schema       *iceberg.Schema `json:"schema" required:"true"`
	LastColumnID int64           `json:"last-column-id" required:"false"`
}

// TODO: Figure out what lastColumnID is for
// Do we need to check schema compatibility?
func (up AddSchemaUpdate) TableApply(table *icebergTable.Table) error {
	meta := table.Metadata()
	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		// TODO: Recommend changing meta.Schema to *iceberg.Schema instead of iceberg.Schema
		// Overwriting it not very easy right now as a nested struct
		return fmt.Errorf("add-schema is not implemented for V1 tables yet")
	case *icebergTable.MetadataV2:
		meta.SchemaList = append(meta.SchemaList, up.Schema)
	}

	*table = *icebergTable.New(table.Identifier(), meta, table.MetadataLocation(), table.FS())
	return nil
}

func (up AddSchemaUpdate) ViewApply(view *icebergView.View) error {
	view.Metadata.Schemas = append(view.Metadata.Schemas, up.Schema)
	return nil
}

type SetLocationUpdate struct {
	Location string `json:"location" required:"true"`
}

func (up SetLocationUpdate) TableApply(table *icebergTable.Table) error {
	return fmt.Errorf("set-location is not implemented for tables or views yet")
}

func (up SetLocationUpdate) ViewApply(view *icebergView.View) error {
	return fmt.Errorf("set-location is not implemented for tables or views yet")
}

type SetPropertiesUpdate struct {
	Updates map[string]string `json:"updates" required:"true"`
}

func (up SetPropertiesUpdate) TableApply(table *icebergTable.Table) error {
	meta := table.Metadata()
	var props *iceberg.Properties
	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		props = &meta.Props
	case *icebergTable.MetadataV2:
		props = &meta.Props
	}

	for k, v := range up.Updates {
		(*props)[k] = v
	}
	return nil
}

func (up SetPropertiesUpdate) ViewApply(view *icebergView.View) error {
	for k, v := range up.Updates {
		view.Metadata.Properties[k] = v
	}
	return nil
}

type RemovePropertiesUpdate struct {
	Removals []string `json:"removals" required:"true"`
}

func (up RemovePropertiesUpdate) TableApply(table *icebergTable.Table) error {
	meta := table.Metadata()
	var props *iceberg.Properties
	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		props = &meta.Props
	case *icebergTable.MetadataV2:
		props = &meta.Props
	}

	for _, removal := range up.Removals {
		if _, ok := (*props)[removal]; !ok {
			return fmt.Errorf("property %v does not exist in view", removal)
		}

		delete(*props, removal)
	}
	return nil
}

func (up RemovePropertiesUpdate) ViewApply(view *icebergView.View) error {
	for _, removal := range up.Removals {
		if _, ok := view.Metadata.Properties[removal]; !ok {
			return fmt.Errorf("property %v does not exist in view", removal)
		}

		delete(view.Metadata.Properties, removal)
	}
	return nil
}

// ---------------------------- Table Updates ---------------------------- //

type SetCurrentSchemaUpdate struct {
	SchemaID int `json:"schema-id" required:"true"`
}

func (up SetCurrentSchemaUpdate) TableApply(table *icebergTable.Table) error {
	// TODO: Check if schema ID is valid

	meta := table.Metadata()
	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		meta.CurrentSchemaID = up.SchemaID
	case *icebergTable.MetadataV2:
		meta.CurrentSchemaID = up.SchemaID
	}

	*table = *icebergTable.New(table.Identifier(), meta, table.MetadataLocation(), table.FS())
	return nil
}

type AddPartitionSpecUpdate struct {
	Spec iceberg.PartitionSpec `json:"spec" required:"true"`
}

func (up AddPartitionSpecUpdate) TableApply(table *icebergTable.Table) error {
	meta := table.Metadata()
	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		meta.Specs = append(meta.Specs, up.Spec)
		fields := []iceberg.PartitionField{}
		for idx := range up.Spec.NumFields() {
			fields = append(fields, up.Spec.Field(idx))
		}
		meta.Partition = fields
	case *icebergTable.MetadataV2:
		meta.Specs = append(meta.Specs, up.Spec)
	}

	*table = *icebergTable.New(table.Identifier(), meta, table.MetadataLocation(), table.FS())
	return nil
}

type SetDefaultSpecUpdate struct {
	SpecID int `json:"spec-id" required:"true"`
}

func (up SetDefaultSpecUpdate) TableApply(table *icebergTable.Table) error {
	// TODO: Check if spec ID is valid

	meta := table.Metadata()
	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		*meta.LastPartitionID = up.SpecID
	case *icebergTable.MetadataV2:
		*meta.LastPartitionID = up.SpecID
	}

	*table = *icebergTable.New(table.Identifier(), meta, table.MetadataLocation(), table.FS())
	return nil
}

type AddSortOrderUpdate struct {
	SortOrder icebergTable.SortOrder `json:"sort-order" required:"true"`
}

func (up AddSortOrderUpdate) TableApply(table *icebergTable.Table) error {
	meta := table.Metadata()

	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		meta.SortOrderList = append(meta.SortOrderList, up.SortOrder)
	case *icebergTable.MetadataV2:
		meta.SortOrderList = append(meta.SortOrderList, up.SortOrder)
	}

	*table = *icebergTable.New(table.Identifier(), meta, table.MetadataLocation(), table.FS())
	return nil
}

type SetDefaultSortOrderUpdate struct {
	SortOrderID int `json:"sort-order-id" required:"true"`
}

func (up SetDefaultSortOrderUpdate) TableApply(table *icebergTable.Table) error {
	// TODO: Check if sort order ID is valid

	meta := table.Metadata()
	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		meta.DefaultSortOrderID = up.SortOrderID
	case *icebergTable.MetadataV2:
		meta.DefaultSortOrderID = up.SortOrderID
	}

	*table = *icebergTable.New(table.Identifier(), meta, table.MetadataLocation(), table.FS())
	return nil
}

type AddSnapshotUpdate struct {
	Snapshot icebergTable.Snapshot `json:"snapshot" required:"true"`
}

func (up AddSnapshotUpdate) TableApply(table *icebergTable.Table) error {

	meta := table.Metadata()

	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		meta.SnapshotList = append(meta.SnapshotList, up.Snapshot)
	case *icebergTable.MetadataV2:
		meta.SnapshotList = append(meta.SnapshotList, up.Snapshot)
	}
	*table = *icebergTable.New(table.Identifier(), meta, table.MetadataLocation(), table.FS())
	return nil
}

type refName struct {
	Name string `json:"ref-name" required:"true"`
}

type SetSnapshotRefUpdate struct {
	icebergTable.SnapshotRef
	refName
}

// Custom UnmarshalJSON for SetSnapshotRef
// Because SnapshotRef has a custom UnmarshalJSON which is messing with the embedded struct
func (up *SetSnapshotRefUpdate) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &up.SnapshotRef); err != nil {
		return err
	}
	if err := json.Unmarshal(data, &up.refName); err != nil {
		return err
	}
	return nil
}

func (up SetSnapshotRefUpdate) TableApply(table *icebergTable.Table) error {
	fmt.Println("Up", up)

	meta := table.Metadata()
	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		meta.Refs[up.refName.Name] = up.SnapshotRef
		fmt.Println("Meta", meta.Refs)
		if up.refName.Name == "main" {
			meta.CurrentSnapshotID = new(int64)
			*meta.CurrentSnapshotID = up.SnapshotID
			logEntry := icebergTable.SnapshotLogEntry{
				SnapshotID:  up.SnapshotID,
				TimestampMs: meta.SnapshotByID(up.SnapshotID).TimestampMs,
			}
			meta.SnapshotLog = append(meta.SnapshotLog, logEntry)
		}

	case *icebergTable.MetadataV2:
		meta.Refs[up.refName.Name] = up.SnapshotRef
		fmt.Println("Meta", meta.Refs)
		if up.refName.Name == "main" {
			meta.CurrentSnapshotID = new(int64)
			*meta.CurrentSnapshotID = up.SnapshotID
			logEntry := icebergTable.SnapshotLogEntry{
				SnapshotID:  up.SnapshotID,
				TimestampMs: meta.SnapshotByID(up.SnapshotID).TimestampMs,
			}
			meta.SnapshotLog = append(meta.SnapshotLog, logEntry)
		}

	}

	fmt.Println("Ref Name", up.refName.Name)
	fmt.Println("Snapshot Ref", up.SnapshotRef)

	*table = *icebergTable.New(table.Identifier(), meta, table.MetadataLocation(), table.FS())
	return nil
}

type RemoveSnapshotsUpdate struct {
	SnapshotIDs []int64 `json:"snapshot-ids" required:"true"`
}

func (up RemoveSnapshotsUpdate) TableApply(table *icebergTable.Table) error {
	filteredSnaps := []icebergTable.Snapshot{}
	for _, snap := range table.Metadata().Snapshots() {
		if !slices.Contains(up.SnapshotIDs, snap.SnapshotID) {
			filteredSnaps = append(filteredSnaps, snap)
		}
	}

	meta := table.Metadata()
	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		meta.SnapshotList = filteredSnaps
	case *icebergTable.MetadataV2:
		meta.SnapshotList = filteredSnaps
	}

	*table = *icebergTable.New(table.Identifier(), meta, table.MetadataLocation(), table.FS())
	return nil
}

type RemoveSnapshotRefUpdate struct {
	RefName string `json:"ref-name" required:"true"`
}

func (up RemoveSnapshotRefUpdate) TableApply(table *icebergTable.Table) error {
	meta := table.Metadata()
	switch meta := meta.(type) {
	case *icebergTable.MetadataV1:
		refs := meta.Refs
		delete(refs, up.RefName)
		meta.Refs = refs
	case *icebergTable.MetadataV2:
		refs := meta.Refs
		delete(refs, up.RefName)
		meta.Refs = refs
	}

	*table = *icebergTable.New(table.Identifier(), meta, table.MetadataLocation(), table.FS())
	return nil
}

type SetStatisticsUpdate struct {
	SnapshotID int64 `json:"snapshot-id" required:"true"`
	// TODO: Statistics file
	Statistics string `json:"statistics" required:"true"`
}

func (up SetStatisticsUpdate) TableApply(table *icebergTable.Table) error {
	return fmt.Errorf("set-statistics is not implemented for tables yet")
}

type RemoveStatisticsUpdate struct {
	SnapshotID int64 `json:"snapshot-id" required:"true"`
}

func (up RemoveStatisticsUpdate) TableApply(table *icebergTable.Table) error {
	return fmt.Errorf("remove-statistics is not implemented for tables yet")
}

// ---------------------------- View Updates ---------------------------- //

type AddViewVersionUpdate struct {
	ViewVersion icebergView.Version `json:"view-version" required:"true"`
}

func (up AddViewVersionUpdate) ViewApply(view *icebergView.View) error {
	for _, v := range view.Metadata.Versions {
		if v.VersionId == up.ViewVersion.VersionId {
			return fmt.Errorf("version ID %v in view already exists", up.ViewVersion.VersionId)
		}
	}

	view.Metadata.Versions = append(view.Metadata.Versions, up.ViewVersion)
	return nil
}

type SetCurrentViewVersionUpdate struct {
	VersionID int `json:"view-version-id" required:"true"`
}

func (up SetCurrentViewVersionUpdate) ViewApply(view *icebergView.View) error {
	var versionID int
	if up.VersionID == -1 {
		versionID = len(view.Metadata.Versions) - 1
	} else if up.VersionID < 0 {
		return fmt.Errorf("version ID %v is negative", up.VersionID)
	} else {
		if up.VersionID >= len(view.Metadata.Versions) {
			return fmt.Errorf("version ID %v is out of bounds", up.VersionID)
		}
		versionID = up.VersionID
	}

	view.Metadata.CurrentVersionId = versionID
	return nil
}

// ---------------------------- Aggregation ---------------------------- //

type InvalidUpdateError struct {
	Action string
	Op     string
}

func (e InvalidUpdateError) Error() string {
	return fmt.Sprintf("invalid update action %v for %v", e.Action, e.Op)
}

func (InvalidUpdateError) HttpCode() int {
	return http.StatusBadRequest
}

var tableUpdateMap = map[string]TableUpdateOp{
	"assign-uuid":            new(AssignUUIDUpdate),
	"upgrade-format-version": new(UpgradeFormatVersionUpdate),
	"add-schema":             new(AddSchemaUpdate),
	"set-location":           new(SetLocationUpdate),
	"set-properties":         new(SetPropertiesUpdate),
	"remove-properties":      new(RemovePropertiesUpdate),
	"set-current-schema":     new(SetCurrentSchemaUpdate),
	"add-spec":               new(AddPartitionSpecUpdate),
	"set-default-spec":       new(SetDefaultSpecUpdate),
	"add-sort-order":         new(AddSortOrderUpdate),
	"set-default-sort-order": new(SetDefaultSortOrderUpdate),
	"add-snapshot":           new(AddSnapshotUpdate),
	"set-snapshot-ref":       new(SetSnapshotRefUpdate),
	"remove-snapshots":       new(RemoveSnapshotsUpdate),
	"remove-snapshot-ref":    new(RemoveSnapshotRefUpdate),
	"set-statistics":         new(SetStatisticsUpdate),
	"remove-statistics":      new(RemoveStatisticsUpdate),
}

func CreateTableUpdate(raw json.RawMessage) (string, TableUpdateOp, error) {
	var updateAction struct {
		Action string `json:"action" required:"true"`
	}
	if err := json.Unmarshal(raw, &updateAction); err != nil {
		return "", nil, err
	}

	// Create the appropriate update
	var update TableUpdateOp
	if val, ok := tableUpdateMap[updateAction.Action]; ok {
		update = val
	} else {
		return "", nil, &InvalidUpdateError{Action: updateAction.Action, Op: "updateTable"}
	}

	// Unmarshal the raw message into the update
	if err := json.Unmarshal(raw, update); err != nil {
		return "", nil, err
	}

	return updateAction.Action, update, nil
}

var viewUpdateMap = map[string]ViewUpdateOp{
	"assign-uuid":              new(AssignUUIDUpdate),
	"upgrade-format-version":   new(UpgradeFormatVersionUpdate),
	"add-schema":               new(AddSchemaUpdate),
	"set-location":             new(SetLocationUpdate),
	"set-properties":           new(SetPropertiesUpdate),
	"remove-properties":        new(RemovePropertiesUpdate),
	"set-current-view-version": new(SetCurrentViewVersionUpdate),
	"add-view-version":         new(AddViewVersionUpdate),
}

func CreateViewUpdate(raw json.RawMessage) (string, ViewUpdateOp, error) {
	var updateAction struct {
		Action string `json:"action" required:"true"`
	}
	if err := json.Unmarshal(raw, &updateAction); err != nil {
		return "", nil, err
	}

	// Create the appropriate update
	var update ViewUpdateOp
	if val, ok := viewUpdateMap[updateAction.Action]; ok {
		update = val
	} else {
		return "", nil, &InvalidUpdateError{Action: updateAction.Action, Op: "replaceView"}
	}

	// Unmarshal the raw message into the update
	if err := json.Unmarshal(raw, update); err != nil {
		return "", nil, err
	}

	return updateAction.Action, update, nil
}

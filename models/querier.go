// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.26.0

package models

import (
	"context"
	"database/sql"
)

type Querier interface {
	WithTx(tx *sql.Tx) Querier

	CreateNamespace(ctx context.Context, name string, parentPk sql.NullInt64, parentPath string) (int64, error)
	CreateNamespaceProperty(ctx context.Context, nsPk int64, key string, value string) error
	CreateView(ctx context.Context, name string, nsPk int64, metadataLoc string) (int64, error)
	// Delete Namespace Property by Key
	DeleteNamespaceProperty(ctx context.Context, nsPk int64, key string) (int64, error)
	// Delete Namespace (Cascade Deletes Properties)
	DropNamespace(ctx context.Context, name string, parentPath string) (int64, error)
	DropTable(ctx context.Context, pk int64) (int64, error)
	DropView(ctx context.Context, pk int64) (int64, error)
	// Get Child Namespace Names
	GetChildNamespaceNames(ctx context.Context, nspath string) ([]string, error)
	// Note: IS NOT DISTINCT FROM is a null-safe equality operator
	// Equivalent to (col = val OR (col IS NULL AND val IS NULL))
	// Get Namespace PK from Path
	GetNamespacePKHelper(ctx context.Context, parentPath string, name string) (int64, error)
	GetNamespaceProperties(ctx context.Context, nspk int64) ([]GetNamespacePropertiesRow, error)
	GetTableHelper(ctx context.Context, name string, nsPk int64) (GetTableHelperRow, error)
	GetViewHelper(ctx context.Context, name string, nsPk int64) (GetViewHelperRow, error)
	ListTables(ctx context.Context, nspk int64) ([]string, error)
	ListViews(ctx context.Context, nspk int64) ([]string, error)
	RegisterTable(ctx context.Context, name string, nsPk int64, metadataLoc string, lastSeq int64) error
	RenameAndMoveTable(ctx context.Context, name string, nsPk int64, pk int64) (int64, error)
	RenameAndMoveView(ctx context.Context, name string, nsPk int64, pk int64) (int64, error)
	RenameTable(ctx context.Context, name string, pk int64) (int64, error)
	RenameView(ctx context.Context, name string, pk int64) (int64, error)
	// Update Namespace Property by Key
	UpdateNamespaceProperty(ctx context.Context, nsPk int64, key string, value string) (int64, error)
	UpdateTable(ctx context.Context, metadataLoc string, pk int64, lastSeq int64) (int64, error)
	UpdateView(ctx context.Context, metadataLoc string, pk int64) (int64, error)
}
type GetNamespacePropertiesRow struct {
	Key   string
	Value string
}
type GetTableHelperRow struct {
	Pk                 int64
	LastSequenceNumber int64
	MetadataLocation   string
}
type GetViewHelperRow struct {
	Pk               int64
	MetadataLocation string
}

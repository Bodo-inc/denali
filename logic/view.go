package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/Bodo-inc/denali/common"
	icebergView "github.com/Bodo-inc/denali/iceberg-overload/view"
	"github.com/google/uuid"
	"github.com/klauspost/compress/gzip"
)

// Get the View Info (such as PK and Metadata Location) for a given Table Identifier
// Returns an error if the view does not exist
func (qtx *Tx) GetViewInfo(ctx context.Context, id common.TableIdentifier) (int64, string, error) {
	nsPk, err := qtx.GetNamespacePKHelper(ctx, id.Namespace)
	if err != nil {
		return -1, "", err
	}

	out, err := qtx.Queries.GetViewHelper(ctx, id.Name, nsPk)
	if err != nil {
		return -1, "", &common.ViewNotFoundError{Id: id}
	}

	return out.Pk, out.MetadataLocation, nil
}

// Load a View from Storage given a Metadata Location
func (s *State) LoadView(id common.TableIdentifier, metaLoc string) (*icebergView.View, error) {
	return icebergView.NewFromLocation(id.ToIcebergID(), metaLoc, s.io)
}

// Drop a View given a Table Identifier
func (qtx *Tx) DropView(ctx context.Context, viewID common.TableIdentifier) error {
	// Find the view PK
	viewPk, _, err := qtx.GetViewInfo(ctx, viewID)
	if err != nil {
		return err
	}

	// Drop the view
	rows, err := qtx.Queries.DropView(ctx, viewPk)
	if err != nil {
		return err
	} else if rows == 0 {
		return &common.ViewNotFoundError{Id: viewID}
	} else if rows > 1 {
		return fmt.Errorf("dropped more than one view when dropping view")
	} else {
		return nil
	}
}

// Rename a View and Optionally Move it to a New Namespace
// If a new namespace is not provided, the view will remain in the same namespace
func (qtx *Tx) RenameView(ctx context.Context, pk int64, newName string, newNs common.NamespaceID) error {
	var rows int64
	var err error

	if newNs != nil {
		newNsPk, err := qtx.GetNamespacePKHelper(ctx, newNs)
		if err != nil {
			return err
		}

		rows, err = qtx.Queries.RenameAndMoveView(ctx, newName, newNsPk, pk)
		if err != nil {
			return err
		}
	} else {
		rows, err = qtx.Queries.RenameView(ctx, newName, pk)
	}

	if err != nil {
		return err
	} else if rows == 0 {
		return &common.ViewNotFoundError{Id: common.TableIdentifier{Namespace: newNs, Name: newName}}
	} else if rows > 1 {
		return fmt.Errorf("renamed more than one view when renaming view")
	} else {
		return nil
	}
}

type UpdateErr struct {
	Idx        int
	UpdateType string
	Err        error
}

func (e UpdateErr) Error() error {
	return fmt.Errorf("error applying view update %v at index %d: %v", e.UpdateType, e.Idx, e.Err)
}

type UpdateErrs []UpdateErr

// Update a View Given Update Operations
func UpdateView(view icebergView.View, uuidReq *uuid.UUID, ops []ViewUpdateOp, types []string) (UpdateErrs, error) {
	var errs UpdateErrs

	// Apply the requirement verification
	if uuidReq != nil && *uuidReq != view.Metadata.ViewId {
		return nil, fmt.Errorf("view ID mismatch when verifying update")
	}

	// Apply each operation
	for idx, op := range ops {
		err := op.ViewApply(&view)
		if err != nil {
			errs = append(errs, UpdateErr{Idx: idx, UpdateType: types[idx], Err: err})
		}
	}

	return errs, nil
}

func (s *State) writeView(view icebergView.View, viewUuid uuid.UUID, lastSeqNum int) (string, error) {
	newSeqNum := lastSeqNum + 1
	updateUuid := uuid.New()

	// Determine path to write new view
	viewPath := s.TableUUIDToPath(viewUuid)
	viewMetadataPath, err := url.JoinPath(viewPath, "metadata", fmt.Sprintf("%05d-%v.gz.metadata.json", newSeqNum, updateUuid))
	if err != nil {
		return "", err
	}

	outFile, err := view.Fs.OpenWrite(viewMetadataPath)
	defer outFile.Close()
	if err != nil {
		return "", err
	}

	writer := gzip.NewWriter(outFile)
	defer writer.Close()
	err = json.NewEncoder(writer).Encode(view.Metadata)
	if err != nil {
		return "", err
	}

	return viewMetadataPath, nil
}

// func (qtx *QueryTx) CreateView(ctx context.Context, id common.TableIdentifier, view *icebergView.View) error {
// 	nsPk, err := qtx.GetNamespacePKHelper(ctx, nsID)
// 	if err != nil {
// 		return err
// 	}

// 	// Create the view
// 	rows, err := qtx.Queries.CreateView(ctx, nsPk, view.MetadataLocation)
// 	if err != nil {
// 		return err
// 	} else if rows != 1 {
// 		return fmt.Errorf("created more than one view when creating view")
// 	}

// 	return nil
// }

func (qtx *Tx) CommitView(ctx context.Context, pk int64, view *icebergView.View) error {

	// Update the metadata location
	rows, err := qtx.Queries.UpdateView(ctx, view.MetadataLocation, pk)
	if err != nil {
		return err
	} else if rows == 0 {
		return &common.ViewNotFoundError{Id: common.TableIdentifier{}.FromIcebergID(view.Identifier)}
	} else if rows > 1 {
		return fmt.Errorf("updated more than one view when committing view")
	}

	return nil
}

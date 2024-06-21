package logic

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Bodo-inc/denali/common"
)

func (qtx *Tx) GetNamespacePKHelper(ctx context.Context, path common.NamespaceID) (int64, error) {
	log.Printf("Getting Namespace PK for Path `%v`", path)

	parentPath, name := path.ParentChild()
	pk, err := qtx.Queries.GetNamespacePKHelper(ctx, strings.Join(parentPath, "."), name)
	if err != nil {
		return -1, &common.NamespaceNotFoundError{Path: path}
	}

	return pk, nil
}

func (s *State) GetChildNamespaces(ctx context.Context, nsID common.NamespaceID) ([]string, error) {

	var ns_names []string
	var err error
	if nsID == nil {
		log.Println("Listing Root Namespaces")
		ns_names, err = s.queries.GetChildNamespaceNames(ctx, "")
		if err != nil {
			return nil, err
		}

	} else {
		log.Printf("Listing Namespaces with Parent Path `%v`", nsID)

		ns_names, err = s.queries.GetChildNamespaceNames(ctx, strings.Join(nsID, "."))
		if err != nil {
			return nil, err
		}
	}

	return ns_names, nil
}

type NamespaceContent struct {
	ID         common.NamespaceID `json:"namespace"`
	Properties map[string]string  `json:"properties"`
}

func (qtx *Tx) LoadNamespace(ctx context.Context, nsID common.NamespaceID) (*NamespaceContent, error) {
	log.Printf("Loading Namespace `%v`", nsID)

	// Find the namespace PK
	pk, err := qtx.GetNamespacePKHelper(ctx, nsID)
	if err != nil {
		return nil, err
	}

	dbProps, err := qtx.Queries.GetNamespaceProperties(ctx, pk)
	if err != nil {
		return nil, err
	}

	properties := make(map[string]string)
	for _, prop := range dbProps {
		properties[prop.Key] = prop.Value
	}

	return &NamespaceContent{
		ID:         nsID,
		Properties: properties,
	}, nil
}

type InvalidNamespacePathError struct {
	Path common.NamespaceID
}

func (e *InvalidNamespacePathError) Error() string {
	return fmt.Sprintf("Invalid Namespace Path `%v`", e.Path)
}

func (e *InvalidNamespacePathError) HttpCode() int {
	return http.StatusBadRequest
}

type NamespaceAlreadyExistsError struct {
	Path common.NamespaceID
}

func (e *NamespaceAlreadyExistsError) Error() string {
	return fmt.Sprintf("Namespace `%v` Already Exists", e.Path)
}

func (e *NamespaceAlreadyExistsError) HttpCode() int {
	return http.StatusConflict
}

func (qtx *Tx) CreateNamespace(ctx context.Context, ns NamespaceContent) error {
	log.Printf("Creating Namespace `%v`", ns.ID)

	if len(ns.ID) == 0 {
		return &InvalidNamespacePathError{Path: ns.ID}
	}
	parentPath, name := ns.ID.ParentChild()

	// Find the parent namespace
	var parentPk sql.NullInt64
	if len(parentPath) == 0 {
		parentPk = sql.NullInt64{Valid: false}
	} else {
		parentPkInner, err := qtx.GetNamespacePKHelper(ctx, parentPath)
		if err != nil {
			return err
		}
		parentPk = sql.NullInt64{Valid: true, Int64: parentPkInner}
	}

	pk, err := qtx.Queries.CreateNamespace(ctx, name, parentPk, strings.Join(parentPath, "."))
	if err != nil {
		if err == sql.ErrNoRows {
			return &NamespaceAlreadyExistsError{Path: ns.ID}
		}
		return err
	}

	if _, exists := ns.Properties["created_at"]; !exists {
		ns.Properties["created_at"] = strconv.FormatInt(time.Now().UnixMilli(), 10)
	}

	for key, value := range ns.Properties {
		err := qtx.Queries.CreateNamespaceProperty(ctx, pk, key, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (qtx *Tx) UpdateNamespaceProperties(ctx context.Context, nsID common.NamespaceID, properties map[string]*string) ([]string, error) {
	log.Printf("Updating Namespace `%v` Properties", nsID)

	// Find the namespace PK
	pk, err := qtx.GetNamespacePKHelper(ctx, nsID)
	if err != nil {
		return nil, err
	}

	var missingProps []string
	// Update Properties
	for key, value := range properties {
		if value == nil {
			// Removal
			rows, err := qtx.Queries.DeleteNamespaceProperty(ctx, pk, key)
			if err != nil {
				return nil, err
			}

			if rows == 0 {
				missingProps = append(missingProps, key)
			} else if rows > 1 {
				return nil, fmt.Errorf("deleted %v rows for namespace property key `%v`", rows, key)
			}
		} else {
			// Update
			rows, err := qtx.Queries.UpdateNamespaceProperty(ctx, pk, key, *value)
			if err != nil {
				return nil, err
			}

			if rows == 0 {
				return nil, fmt.Errorf("expected to upsert namespace property key `%v`", key)
			} else if rows > 1 {
				return nil, fmt.Errorf("upserted %v rows for namespace property key `%v`", rows, key)
			}
		}
	}

	return missingProps, nil
}

func (qtx *Tx) DropNamespace(ctx context.Context, nsID common.NamespaceID) error {
	log.Printf("Dropping Namespace `%v`", nsID)
	parentPath, name := nsID.ParentChild()

	// Perform Drop
	rows, err := qtx.Queries.DropNamespace(ctx, name, strings.Join(parentPath, "."))
	if err != nil {
		return err
	} else if rows == 0 {
		return &common.NamespaceNotFoundError{Path: nsID}
	}

	return nil
}

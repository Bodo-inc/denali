package routes

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"reflect"
	"slices"

	"github.com/Bodo-inc/denali/common"
	icebergView "github.com/Bodo-inc/denali/iceberg-overload/view"
	"github.com/Bodo-inc/denali/logic"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

func extractViewID(req *http.Request) common.TableIdentifier {
	return common.TableIdentifier{
		Namespace: ExtractNamespaceID(req),
		Name:      req.PathValue("view"),
	}
}

// --------------------------------------------------- listViews --------------------------------------------------- //

type ListViewsResponse struct {
	Identifiers []common.TableIdentifier `json:"identifiers"`
}

func (r *Router) listViews(ctx context.Context, req *http.Request) (*ListViewsResponse, error) {
	nsPath := ExtractNamespaceID(req)
	log.Printf("Listing views in namespace `%v`", nsPath)

	// Get all views under this namespace
	viewNames, err := logic.WrapTxRet(r.State, func(qtx *logic.Tx) ([]string, error) {
		nsPk, err := qtx.GetNamespacePKHelper(ctx, nsPath)
		if err != nil {
			return nil, err
		}

		// List views
		names, err := qtx.Queries.ListViews(ctx, nsPk)
		if err != nil {
			return nil, err
		}

		return names, nil
	})

	if err != nil {
		return nil, err
	}

	// Convert to response format
	out := new(ListViewsResponse)
	out.Identifiers = make([]common.TableIdentifier, len(viewNames))
	for i, ns := range viewNames {
		out.Identifiers[i] = common.TableIdentifier{
			Namespace: nsPath,
			Name:      ns,
		}
	}
	return out, nil
}

// -------------------------------------------------- createView --------------------------------------------------- //

type CreateViewReq struct {
	Name        string              `json:"name" required:"true"`
	Location    *string             `json:"location" required:"false"`
	Schema      iceberg.Schema      `json:"schema" required:"true"`
	ViewVersion icebergView.Version `json:"version" required:"true"`
	Properties  iceberg.Properties  `json:"properties" required:"true"`
}

type CreateViewRes struct{}

func (r *Router) createView(ctx context.Context, req *http.Request) (*CreateViewRes, error) {
	nsPath := ExtractNamespaceID(req)
	log.Printf("Creating view in namespace `%v`", nsPath)

	// Parse the request body
	var body CreateViewReq
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}

	// Create the view
	return nil, nil
}

// --------------------------------------------------- loadView ---------------------------------------------------- //

type LoadViewResponse struct {
	MetadataLoc string                `json:"metadata-location"`
	Metadata    *icebergView.Metadata `json:"metadata"`
}

func (r *Router) loadView(ctx context.Context, req *http.Request) (*LoadViewResponse, error) {
	viewID := extractViewID(req)
	log.Printf("Loading View `%v`", viewID)

	view, err := logic.WrapTxRet(r.State, func(qtx *logic.Tx) (*icebergView.View, error) {
		// Find the view PK
		_, metaLoc, err := qtx.GetViewInfo(ctx, viewID)
		if err != nil {
			return nil, err
		}

		// Load the view
		return r.State.LoadView(viewID, metaLoc)
	})

	if err != nil {
		return nil, err
	}

	// Reformat for the expected response
	out := new(LoadViewResponse)
	out.MetadataLoc = view.MetadataLocation
	out.Metadata = view.Metadata
	return out, nil
}

// -------------------------------------------------- replaceView -------------------------------------------------- //

type ReplaceViewReq struct {
	ID      *common.TableIdentifier `json:"identifier" required:"false"`
	Reqs    []json.RawMessage       `json:"requirements" required:"false"`
	Updates []json.RawMessage       `json:"updates" required:"true"`
}

func (r *Router) replaceView(ctx context.Context, req *http.Request) (*LoadViewResponse, error) {
	viewID := extractViewID(req)
	log.Printf("Replacing View `%v`", viewID)

	// Parse the request body
	var body ReplaceViewReq
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}

	// Validate the identifier in the request body
	if body.ID != nil && reflect.DeepEqual(viewID, *body.ID) {
		return nil, BadRequestError("replaceView: View Identifier in URL does not match View Identifier in Request Body")
	}

	// Parse the requirements
	// Currently, only assert-view-uuid is supported, so we just check if all the UUIDs are the same
	var assertUUID *uuid.UUID = nil
	var reqBase struct {
		Type string    `json:"type" required:"true"`
		Uuid uuid.UUID `json:"uuid" required:"true"`
	}

	for _, reqRaw := range body.Reqs {
		if err := json.Unmarshal(reqRaw, &reqBase); err != nil {
			return nil, err
		} else if reqBase.Type != "assert-view-uuid" {
			return nil, BadRequestError("replaceView: Unknown Requirement Type `" + reqBase.Type + "`")
		} else if assertUUID != nil && *assertUUID != reqBase.Uuid {
			return nil, BadRequestError("replaceView: Different UUIDs to Assert in Requirements")
		}

		assertUUID = &reqBase.Uuid
	}

	// Parse the updates
	names := make([]string, len(body.Updates))
	updates := make([]logic.ViewUpdateOp, len(body.Updates))
	for i, updateRaw := range body.Updates {
		name, update, err := logic.CreateViewUpdate(updateRaw)
		if err != nil {
			return nil, err
		}

		names[i] = name
		updates[i] = update
	}

	// Apply the updates
	view, err := logic.WrapTxRet(r.State, func(qtx *logic.Tx) (*icebergView.View, error) {
		// Find the view PK
		_, metadataLoc, err := qtx.GetViewInfo(ctx, viewID)
		if err != nil {
			return nil, err
		}

		// Load the view
		view, err := r.State.LoadView(viewID, metadataLoc)
		if err != nil {
			return nil, err
		}

		// TODO: Finish the implementation
		// // Apply the updates
		// updateErrs, err := logic.UpdateView(view, updates)
		// if err != nil {
		// 	return nil, err
		// } else if len(updateErrs) > 0 {
		// 	return nil, fmt.Errorf("Failed to apply %d updates", len(updateErrs))
		// }

		// // Commit the changes
		// r.State.
		// 	qtx.CommitView(ctx, pk, view)

		return view, nil
	})

	if err != nil {
		return nil, err
	}

	// Reformat for the expected response
	out := new(LoadViewResponse)
	out.MetadataLoc = view.MetadataLocation
	out.Metadata = view.Metadata
	return out, nil
}

// --------------------------------------------------- dropView ---------------------------------------------------- //

func (r *Router) dropView(ctx context.Context, req *http.Request) (*struct{}, error) {
	viewID := extractViewID(req)
	log.Printf("Dropping View `%v`", viewID)

	err := r.State.WrapTx(func(qtx *logic.Tx) error { return qtx.DropView(ctx, viewID) })
	return nil, err
}

// -------------------------------------------------- viewExists -------------------------------------------------- //

func (r *Router) viewExists(ctx context.Context, req *http.Request) (*struct{}, error) {
	viewID := extractViewID(req)
	log.Printf("Check if View `%v` Exists", viewID)

	// Check if table exists
	err := r.State.WrapTx(func(qtx *logic.Tx) error {
		_, _, err := qtx.GetViewInfo(ctx, viewID)
		return err
	})

	return nil, err
}

// -------------------------------------------------- renameView --------------------------------------------------- //

func (r *Router) renameView(ctx context.Context, req *http.Request) (*struct{}, error) {
	var body struct {
		Source common.TableIdentifier
		Target common.TableIdentifier
	}

	// Parse the request body
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}

	err := r.State.WrapTx(func(qtx *logic.Tx) error {
		// Find the view PK
		viewPk, _, err := qtx.GetViewInfo(ctx, body.Source)
		if err != nil {
			return err
		}

		// Perform the rename
		var newNs common.NamespaceID = nil
		if !slices.Equal(body.Target.Namespace, body.Source.Namespace) {
			newNs = body.Target.Namespace
		}

		err = qtx.RenameView(ctx, viewPk, body.Target.Name, newNs)
		return err
	})

	// Successfully renamed, so return 204 empty response
	return nil, err
}

// -------------------------------------------------- Top Router --------------------------------------------------- //

func (r *Router) ViewRoutes() {
	HandleAPI(r, "GET /v1/namespaces/{namespace}/views", r.listViews)
	HandleAPI(r, "POST /v1/namespaces/{namespace}/views", r.createView)
	HandleAPI(r, "GET /v1/namespaces/{namespace}/views/{view}", r.loadView)
	HandleAPI(r, "POST /v1/namespaces/{namespace}/views/{view}", r.replaceView)
	HandleAPI(r, "DELETE /v1/namespaces/{namespace}/views/{view}", r.dropView)
	HandleAPI(r, "HEAD /v1/namespaces/{namespace}/views/{view}", r.viewExists)
	HandleAPI(r, "POST /v1/views/rename", r.renameView)
}

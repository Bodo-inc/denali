package routes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"slices"
	"time"

	"github.com/Bodo-inc/denali/common"
	"github.com/Bodo-inc/denali/logic"

	"github.com/apache/iceberg-go"
	icebergTable "github.com/apache/iceberg-go/table"
)

// 	"slices"

func extractTableID(req *http.Request) common.TableIdentifier {
	return common.TableIdentifier{
		Namespace: ExtractNamespaceID(req),
		Name:      req.PathValue("table"),
	}
}

type TableResponse struct {
	MetadataLocation string                `json:"metadata-location"`
	Metadata         icebergTable.Metadata `json:"metadata"`
	Config           map[string]string     `json:"config"`
}

// --------------------------------------------------- listTables -------------------------------------------------- //

type ListTablesResponse struct {
	Identifiers []common.TableIdentifier `json:"identifiers"`
}

func (r *Router) listTables(ctx context.Context, req *http.Request) (*ListTablesResponse, error) {
	nsPath := ExtractNamespaceID(req)
	log.Printf("Listing tables in namespace `%v`", nsPath)

	var tableNames []string
	err := r.State.WrapTx(func(qtx *logic.Tx) (err error) {
		tableNames, err = qtx.ListTables(ctx, nsPath)
		return
	})
	if err != nil {
		return nil, err
	}

	// Convert to response format
	out := new(ListTablesResponse)
	out.Identifiers = make([]common.TableIdentifier, len(tableNames))
	for i, ns := range tableNames {
		out.Identifiers[i] = common.TableIdentifier{
			Namespace: nsPath,
			Name:      ns,
		}
	}

	return out, nil
}

// -------------------------------------------------- createTable -------------------------------------------------- //

type CreateTableReq struct {
	Name          string         `json:"name" required:"true"`
	Location      *string        `json:"location" required:"false"`
	Schema        iceberg.Schema `json:"schema" required:"true"`
	PartitionSpec struct {
		Fields []iceberg.PartitionField `json:"fields" required:"true"`
	} `json:"partition-spec" required:"false"`
	SortOrder struct {
		Fields []icebergTable.SortField `json:"fields" required:"true"`
	} `json:"sort-order" required:"false"`
	Properties  iceberg.Properties `json:"properties" required:"false"`
	StageCreate bool               `json:"stage-create" required:"false" default:"false"`
}

func (r *Router) createTable(ctx context.Context, req *http.Request) (*TableResponse, error) {
	nsPath := ExtractNamespaceID(req)
	log.Printf("Create a table in namespace `%v`", nsPath)

	// Parse the request body
	var body CreateTableReq
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}

	// Disallow custom locations for now
	if body.Location != nil {
		return nil, &CustomRestError{
			Code:    http.StatusBadRequest,
			Type:    "InvalidRequest",
			Message: "Creating tables at custom locations are not supported",
		}
	} else if body.StageCreate {
		return nil, &CustomRestError{
			Code:    http.StatusBadRequest,
			Type:    "InvalidRequest",
			Message: "Staged table creation is not supported yet",
		}
	}

	// Construct and Write Table Metadata
	meta := r.State.CreateTableMetadata(
		&body.Schema,
		body.Properties,
		body.PartitionSpec.Fields,
		body.SortOrder.Fields,
	)
	table, err := r.State.WriteTableMetadata(common.TableIdentifier{Namespace: nsPath, Name: body.Name}, meta, 0)
	if err != nil {
		return nil, err
	}

	// Register the table
	err = r.State.WrapTx(func(qtx *logic.Tx) error { return qtx.RegisterTable(ctx, table) })
	if err != nil {
		return nil, err
	}

	// Format the response
	return &TableResponse{
		MetadataLocation: table.MetadataLocation(),
		Metadata:         table.Metadata(),
		Config:           map[string]string{},
	}, nil
}

// ------------------------------------------------- registerTable ------------------------------------------------- //

type RegisterTableReq struct {
	Name             string `json:"name"`
	MetadataLocation string `json:"metadata-location"`
}

func (r *Router) registerTable(ctx context.Context, req *http.Request) (*TableResponse, error) {
	nsPath := ExtractNamespaceID(req)
	log.Printf("Register a table in namespace `%v`", nsPath)

	// Parse the request body
	var body RegisterTableReq
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}
	id := common.TableIdentifier{Namespace: nsPath, Name: body.Name}

	// Verify and load metadata file
	table, err := r.State.TableFromMetadata(id, body.MetadataLocation)
	if err != nil {
		return nil, &CustomRestError{
			Code:    http.StatusNotFound,
			Type:    "MetadataFileNotFound",
			Message: fmt.Sprintf("Metadata file not found at location `%v`", body.MetadataLocation),
		}
	}

	var lastSeqNum int64 = 0
	switch meta := table.Metadata().(type) {
	case *icebergTable.MetadataV2:
		lastSeqNum = int64(meta.LastSequenceNumber)
	case *icebergTable.MetadataV1:
		lastSeqNum = meta.CurrentSnapshot().SequenceNumber
	}

	// Write the metadata in the server's warehouse
	table, err = r.State.WriteTableMetadata(id, table.Metadata(), lastSeqNum)
	if err != nil {
		return nil, err
	}

	// Register the table
	err = r.State.WrapTx(func(qtx *logic.Tx) error { return qtx.RegisterTable(ctx, table) })
	if err != nil {
		return nil, err
	}

	// Format the response
	return &TableResponse{
		MetadataLocation: table.MetadataLocation(),
		Metadata:         table.Metadata(),
		Config:           map[string]string{},
	}, nil
}

// --------------------------------------------------- loadTable --------------------------------------------------- //

func (r *Router) loadTable(ctx context.Context, req *http.Request) (*TableResponse, error) {
	tableID := extractTableID(req)
	_ = req.URL.Query().Get("snapshots") == "refs"
	log.Printf("Load Table `%v`", tableID)

	// See if a namespace in this path is found
	var info *logic.TableInfo
	err := r.State.WrapTx(func(qtx *logic.Tx) error {
		var err error
		info, err = qtx.GetTableInfo(ctx, tableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	// Load the table metadata
	table, err := r.State.TableFromMetadata(tableID, info.MetadataLocation)
	if err != nil {
		return nil, fmt.Errorf("error loading table: %w", err)
	}

	// Format response
	return &TableResponse{
		MetadataLocation: table.MetadataLocation(),
		Metadata:         table.Metadata(),
		Config:           map[string]string{},
	}, nil
}

// -------------------------------------------------- updateTable -------------------------------------------------- //

type UpdateTableReq struct {
	ID      *common.TableIdentifier `json:"identifier" required:"false"`
	Reqs    []json.RawMessage       `json:"requirements" required:"false"`
	Updates []json.RawMessage       `json:"updates" required:"true"`
}

func (r *Router) updateTable(ctx context.Context, req *http.Request) (*TableResponse, error) {
	tableID := extractTableID(req)
	log.Printf("Update Table `%v`", tableID)

	// Parse the request body
	var body UpdateTableReq
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}

	// Validate the identifier in the request body
	if body.ID != nil && reflect.DeepEqual(tableID, *body.ID) {
		return nil, BadRequestError("updateTable: Table Identifier in URL does not match Table Identifier in Request Body")
	}

	// Parse the requirements
	reqs := make([]logic.TableReqOp, len(body.Reqs))
	for i, reqRaw := range body.Reqs {
		req, err := logic.CreateTableReq(reqRaw)
		if err != nil {
			return nil, err
		}
		reqs[i] = req
	}

	// Parse the updates
	names := make([]string, len(body.Updates))
	updates := make([]logic.TableUpdateOp, len(body.Updates))
	for i, updateRaw := range body.Updates {
		actionName, update, err := logic.CreateTableUpdate(updateRaw)
		if err != nil {
			return nil, err
		}
		names[i] = actionName
		updates[i] = update
	}

	// Apply the updates
	var newTable *icebergTable.Table
	err := r.State.WrapTx(func(qtx *logic.Tx) error {
		// Find the table info
		info, err := qtx.GetTableInfo(ctx, tableID)
		if err != nil {
			return err
		}

		// Load the view
		table, err := r.State.TableFromMetadata(tableID, info.MetadataLocation)
		if err != nil {
			return err
		}

		// Execute each update
		var updateErrs []logic.UpdateErr
		for idx, op := range updates {
			err := op.TableApply(table)
			if err != nil {
				updateErrs = append(updateErrs, logic.UpdateErr{Idx: idx, UpdateType: names[idx], Err: err})
			}
		}

		if len(updateErrs) > 0 {
			for _, uerr := range updateErrs {
				log.Printf("Update %d (%s) failed: %v", uerr.Idx, names[uerr.Idx], uerr.Err)
			}
			return fmt.Errorf("failed to apply %d updates", len(updateErrs))
		}

		// Mark lastSequenceNumber and lastUpdatedTime
		newLastSeq := info.LastSequenceNumber + 1
		meta := table.Metadata()
		switch meta := meta.(type) {
		case *icebergTable.MetadataV2:
			meta.LastSequenceNumber = int(newLastSeq)
			meta.LastUpdatedMS = time.Now().UnixMilli()
		case *icebergTable.MetadataV1:
			meta.LastUpdatedMS = time.Now().Unix()
		}

		// Write the updated metadata to warehouse
		newTable, err = r.State.WriteTableMetadata(tableID, meta, newLastSeq)
		if err != nil {
			return err
		}

		// Mark as updated
		rows, err := qtx.Queries.UpdateTable(ctx, newTable.MetadataLocation(), info.Pk, info.LastSequenceNumber)
		if err != nil {
			return err
		} else if rows == 0 {
			return fmt.Errorf("expected to update 1 table, but none were")
		} else if rows > 1 {
			return fmt.Errorf("updated more than one table when updating table")
		}

		return nil
	})

	return &TableResponse{
		MetadataLocation: newTable.MetadataLocation(),
		Metadata:         newTable.Metadata(),
		Config:           map[string]string{},
	}, err
}

// --------------------------------------------------- dropTable --------------------------------------------------- //

func (r *Router) dropTable(ctx context.Context, req *http.Request) (*struct{}, error) {
	// Parse the request path and query
	tableID := extractTableID(req)
	purge := req.URL.Query().Get("purge") == "true"

	var purgeStr string = ""
	if purge {
		purgeStr = "and purge "
	}
	log.Printf("Drop %vtable `%v`", purgeStr, tableID)

	// If purge is requested, throw error because unsupported
	if purge {
		return nil, NotImplementedError()
	}

	err := r.State.WrapTx(func(qtx *logic.Tx) error {
		info, err := qtx.GetTableInfo(ctx, tableID)
		if err != nil {
			return err
		}

		// Attempt to drop the table
		rows, err := qtx.Queries.DropTable(ctx, info.Pk)
		if err != nil {
			return err
		} else if rows == 0 {
			return fmt.Errorf("expected to drop 1 table, but none were")
		} else if rows > 1 {
			return fmt.Errorf("dropped more than one table when dropping table")
		}

		return nil
	})

	// Successfully dropped, so return 204 empty response
	return nil, err
}

// -------------------------------------------------- tableExists -------------------------------------------------- //

func (r *Router) tableExists(ctx context.Context, req *http.Request) (*struct{}, error) {
	tableID := extractTableID(req)
	log.Printf("Check if table `%v` exists", tableID)

	// Find the tables PK
	err := r.State.WrapTx(func(qtx *logic.Tx) error {
		_, err := qtx.GetTableInfo(ctx, tableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	// Found, so return 204 empty response
	return nil, nil
}

// -------------------------------------------------- renameTable -------------------------------------------------- //

type RenameTableRequest struct {
	Source common.TableIdentifier
	Target common.TableIdentifier
}

func (r *Router) renameTable(ctx context.Context, req *http.Request) (*struct{}, error) {
	// Parse the request body
	var body RenameTableRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}

	err := r.State.WrapTx(func(qtx *logic.Tx) error {
		// Find the tables PK
		info, err := qtx.GetTableInfo(ctx, body.Source)
		if err != nil {
			return err
		}

		// Perform the rename
		var newNs common.NamespaceID = nil
		if !slices.Equal(body.Target.Namespace, body.Source.Namespace) {
			newNs = body.Target.Namespace
		}

		err = qtx.RenameTable(ctx, info.Pk, body.Target.Name, newNs)
		return err
	})

	// Successfully renamed, so return 204 empty response
	return nil, err
}

// ------------------------------------------------- reportMetrics ------------------------------------------------- //

func (r *Router) reportMetrics(ctx context.Context, req *http.Request) (*struct{}, error) {
	// Not implemented, don't throw error
	return nil, nil
}

// ----------------------------------------------- commitTransaction ----------------------------------------------- //

// type TableRequirement struct {
// 	reqType string `json:"type" required:"true"`
// }

// type TableUpdate struct {
// 	action string         `json:"action" required:"true"`
// 	others map[string]any `json:"others" required:"false"`
// }

// type CommitTableRequest struct {
// 	id      *common.TableIdentifier `json:"identifier" required:"false"`
// 	reqs    []TableRequirement      `json:"requirements" required:"true"`
// 	updates []TableUpdate           `json:"updates" required:"true"`
// }

func (r *Router) commitTransaction(ctx context.Context, req *http.Request) (*struct{}, error) {
	return nil, NotImplementedError()
}

// -------------------------------------------------- Top Router --------------------------------------------------- //

func (r *Router) TableRoutes() {
	HandleAPI(r, "GET /v1/namespaces/{namespace}/tables", r.listTables)
	HandleAPI(r, "POST /v1/namespaces/{namespace}/tables", r.createTable)
	HandleAPI(r, "POST /v1/namespaces/{namespace}/register", r.registerTable)
	HandleAPI(r, "GET /v1/namespaces/{namespace}/tables/{table}", r.loadTable)
	HandleAPI(r, "POST /v1/namespaces/{namespace}/tables/{table}", r.updateTable)
	HandleAPI(r, "DELETE /v1/namespaces/{namespace}/tables/{table}", r.dropTable)
	HandleAPI(r, "HEAD /v1/namespaces/{namespace}/tables/{table}", r.tableExists)
	HandleAPI(r, "POST /v1/tables/rename", r.renameTable)
	HandleAPI(r, "POST /v1/namespaces/{namespace}/tables/{table}/metrics", r.reportMetrics)
	HandleAPI(r, "POST /v1/transactions/commit", r.commitTransaction)
}

package logic

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	icebergTable "github.com/apache/iceberg-go/table"
)

type ReqOp interface {
	ReqName() string
}

type TableReqOp interface {
	ReqOp
	Check(table *icebergTable.Table) bool
}

type AssertCreateReq struct{}

func (AssertCreateReq) ReqName() string {
	return "assert-create"
}

func (AssertCreateReq) Check(table *icebergTable.Table) bool {
	panic(fmt.Errorf("not implemented yet"))
}

type AssertTableUUIDReq struct {
	Uuid string `json:"uuid"`
}

func (AssertTableUUIDReq) ReqName() string {
	return "assert-table-uuid"
}

func (r AssertTableUUIDReq) Check(table *icebergTable.Table) bool {
	return table.Metadata().TableUUID().String() == r.Uuid
}

type AssertRefSnapshotIdReq struct {
	Ref        string `json:"ref"`
	SnapshotId int64  `json:"snapshot-id"`
}

func (AssertRefSnapshotIdReq) ReqName() string {
	return "assert-ref-snapshot-id"
}

func (r AssertRefSnapshotIdReq) Check(table *icebergTable.Table) bool {
	return table.Metadata().SnapshotByName(r.Ref).SnapshotID == r.SnapshotId
}

type AssertLastAssignedFieldIdReq struct {
	LastAssignedFieldId int `json:"last-assigned-field-id"`
}

func (AssertLastAssignedFieldIdReq) ReqName() string {
	return "assert-last-assigned-field-id"
}

func (r AssertLastAssignedFieldIdReq) Check(table *icebergTable.Table) bool {
	return table.Metadata().LastColumnID() == r.LastAssignedFieldId
}

type AssertCurrentSchemaIdReq struct {
	CurrentSchemaID int `json:"current-schema-id"`
}

func (AssertCurrentSchemaIdReq) ReqName() string {
	return "assert-current-schema-id"
}

func (r AssertCurrentSchemaIdReq) Check(table *icebergTable.Table) bool {
	return table.Metadata().CurrentSchema().ID == r.CurrentSchemaID
}

type AssertLastAssignedPartitionIdReq struct {
	LastAssignedPartitionID int `json:"last-assigned-partition-id"`
}

func (AssertLastAssignedPartitionIdReq) ReqName() string {
	return "assert-last-assigned-partition-id"
}

func (r AssertLastAssignedPartitionIdReq) Check(table *icebergTable.Table) bool {
	return *table.Metadata().LastPartitionSpecID() == r.LastAssignedPartitionID
}

type AssertDefaultSpecIdReq struct {
	DefaultSpecID int `json:"default-spec-id"`
}

func (AssertDefaultSpecIdReq) ReqName() string {
	return "assert-default-spec-id"
}

func (r AssertDefaultSpecIdReq) Check(table *icebergTable.Table) bool {
	return table.Metadata().DefaultPartitionSpec() == r.DefaultSpecID
}

type AssertDefaultSortOrderIdReq struct {
	DefaultSortOrderID int `json:"default-sort-order-id"`
}

func (AssertDefaultSortOrderIdReq) ReqName() string {
	return "assert-default-sort-order-id"
}

func (r AssertDefaultSortOrderIdReq) Check(table *icebergTable.Table) bool {
	return table.Metadata().SortOrder().OrderID == r.DefaultSortOrderID
}

type InvalidReqError struct {
	Action string
	Op     string
}

func (e InvalidReqError) Error() string {
	return fmt.Sprintf("invalid request action %v for %v", e.Action, e.Op)
}

func (InvalidReqError) HttpCode() int {
	return http.StatusBadRequest
}

var reqs = []TableReqOp{
	&AssertCreateReq{},
	&AssertTableUUIDReq{},
	&AssertRefSnapshotIdReq{},
	&AssertLastAssignedFieldIdReq{},
	&AssertCurrentSchemaIdReq{},
	&AssertLastAssignedPartitionIdReq{},
	&AssertDefaultSpecIdReq{},
	&AssertDefaultSortOrderIdReq{},
}

var reqMap = make(map[string]TableReqOp)

func init() {
	for _, req := range reqs {
		reqMap[req.ReqName()] = req
	}
}

func CreateTableReq(raw json.RawMessage) (TableReqOp, error) {
	var reqInit struct {
		ReqType string `json:"type" required:"true"`
	}
	if err := json.Unmarshal(raw, &reqInit); err != nil {
		return nil, err
	}

	// Create the appropriate req
	var req TableReqOp
	if val, ok := reqMap[reqInit.ReqType]; ok {
		req = val
	} else {
		return nil, &InvalidReqError{Action: reqInit.ReqType, Op: "updateTable"}
	}

	// Unmarshal the raw message into the req
	log.Println(string(raw))
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}

	return req, nil
}

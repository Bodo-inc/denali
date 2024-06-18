package routes

import (
	"net/http"

	"github.com/Bodo-inc/denali/common"

	icebergTable "github.com/apache/iceberg-go/table"
)

// ---------------- Common Request Body or Response Structures ----------------

type TableMetadata struct {
	MetadataLocation string                  `json:"metadata-location" required:"false"`
	Metadata         icebergTable.MetadataV2 `json:"metadata" required:"true"`
	Config           map[string]string       `json:"config" required:"false"`
}

// ---------------- Common Request Fields ----------------

type PageParams struct {
	PageToken string `query:"pageToken" required:"false"`
	PageSize  int    `query:"pageSize" required:"false"`
}

func ExtractNamespaceID(req *http.Request) common.NamespaceID {
	return common.NamespaceIDFromPath(req.PathValue("namespace"))
}

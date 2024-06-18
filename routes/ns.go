package routes

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"slices"

	"github.com/Bodo-inc/denali/common"
	"github.com/Bodo-inc/denali/logic"
)

// ------------------------------------------------- listNamespaces ------------------------------------------------ //

type ListNamespacesResponse struct {
	Namespaces [][]string `json:"namespaces"`
}

func (r *Router) listNamespaces(ctx context.Context, req *http.Request) (*ListNamespacesResponse, error) {
	parentPath := common.NamespaceIDFromPath(req.URL.Query().Get("parent"))
	log.Printf("listNamespaces with Parent Path `%v`", parentPath)

	// Get child namespaces
	ns_names, err := r.State.GetChildNamespaces(ctx, parentPath)
	if err != nil {
		return nil, err
	}

	// Generate output
	out := new(ListNamespacesResponse)
	out.Namespaces = make([][]string, len(ns_names))
	for i, ns := range ns_names {
		out.Namespaces[i] = append(parentPath, ns)
	}

	return out, nil
}

// ------------------------------------------------ createNamespace ------------------------------------------------ //

type CreateNamespaceReqRes struct {
	Namespace  []string          `json:"namespace"`
	Properties map[string]string `json:"properties"`
}

func (r *Router) createNamespace(ctx context.Context, req *http.Request) (*logic.NamespaceContent, error) {
	body := new(logic.NamespaceContent)
	err := json.NewDecoder(req.Body).Decode(body)
	if err != nil {
		return nil, InvalidJsonBodyError("createNamespace")
	}

	// Create namespace
	err = r.State.WrapTx(func(qtx *logic.Tx) error { return qtx.CreateNamespace(ctx, *body) })
	if err != nil {
		return nil, err
	}

	return body, nil
}

// ------------------------------------------------ namespaceExists ------------------------------------------------ //

func (r *Router) headNamespace(ctx context.Context, req *http.Request) (*struct{}, error) {
	nsID := common.NamespaceIDFromPath(req.PathValue("namespace"))
	log.Printf("Checking Namespace `%v` Exists", nsID)

	// See if a namespace in this path is found
	err := r.State.WrapTx(func(qtx *logic.Tx) error {
		_, err := qtx.GetNamespacePKHelper(ctx, nsID)
		return err
	})

	// Found, so return 204 empty response
	return nil, err
}

// --------------------------------------------- loadNamespaceMetadata --------------------------------------------- //

func (r *Router) loadNamespaceMetadata(ctx context.Context, req *http.Request) (*logic.NamespaceContent, error) {
	nsPath := common.NamespaceIDFromPath(req.PathValue("namespace"))
	log.Printf("Load Namespace `%v`", nsPath)

	return logic.WrapTxRet(r.State, func(qtx *logic.Tx) (*logic.NamespaceContent, error) {
		return qtx.LoadNamespace(ctx, nsPath)
	})
}

// ------------------------------------------------- dropNamespace ------------------------------------------------- //

func (r *Router) dropNamespace(ctx context.Context, req *http.Request) (*struct{}, error) {
	nsPath := ExtractNamespaceID(req)
	log.Printf("Dropping Namespace `%v`", nsPath)

	err := r.State.WrapTx(func(qtx *logic.Tx) error { return qtx.DropNamespace(ctx, nsPath) })
	return nil, err
}

// ----------------------------------------------- updateProperties ------------------------------------------------ //

type UpdatePropertiesReq struct {
	Removals []string          `json:"removals"`
	Updates  map[string]string `json:"updates"`
}

type UpdatePropertiesRes struct {
	Updated []string `json:"updated"`
	Removed []string `json:"removed"`
	Missing []string `json:"missing"`
}

func RepeatedKeyError(key string) *CustomRestError {
	return &CustomRestError{
		Code:    http.StatusUnprocessableEntity,
		Type:    "UnprocessableInputError",
		Message: "Namespace properties cannot be updated since the key `" + key + "` is repeated in the request",
	}
}

func (r *Router) updateProperties(ctx context.Context, req *http.Request) (*UpdatePropertiesRes, error) {
	nsPath := ExtractNamespaceID(req)
	log.Printf("Update Namespace Properties `%v`", nsPath)

	var changeReq UpdatePropertiesReq
	err := json.NewDecoder(req.Body).Decode(&changeReq)
	if err != nil {
		return nil, InvalidJsonBodyError("updateProperties")
	}

	// Validate the request and construct input
	changes := make(map[string]*string)
	for _, k := range changeReq.Removals {
		if _, ok := changes[k]; ok {
			return nil, RepeatedKeyError(k)
		}
		changes[k] = nil
	}

	for k, v := range changeReq.Updates {
		if _, ok := changes[k]; ok {
			return nil, RepeatedKeyError(k)
		}
		changes[k] = &v
	}

	// Update the properties
	missingProps, err := logic.WrapTxRet(r.State, func(qtx *logic.Tx) ([]string, error) {
		return qtx.UpdateNamespaceProperties(ctx, nsPath, changes)
	})
	if err != nil {
		return nil, err
	}

	// Format the output
	out := new(UpdatePropertiesRes)
	out.Missing = missingProps
	for _, k := range changeReq.Removals {
		if !slices.Contains(missingProps, k) {
			out.Removed = append(out.Removed, k)
		}
	}

	return out, nil
}

// -------------------------------------------------- Top Router --------------------------------------------------- //

func (r *Router) NsRoutes() {
	HandleAPI(r, "GET /v1/namespaces", r.listNamespaces)
	HandleAPI(r, "POST /v1/namespaces", r.createNamespace)
	HandleAPI(r, "HEAD /v1/namespaces/{namespace}", r.headNamespace)
	HandleAPI(r, "GET /v1/namespaces/{namespace}", r.loadNamespaceMetadata)
	HandleAPI(r, "DELETE /v1/namespaces/{namespace}", r.dropNamespace)
	HandleAPI(r, "POST /v1/namespaces/{namespace}/properties", r.updateProperties)
}

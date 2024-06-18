package routes

import (
	_ "embed"

	"context"
	"log"
	"net/http"
)

type ConfigResponse struct {
	Defaults  map[string]string `json:"defaults" required:"true"`
	Overrides map[string]string `json:"overrides" required:"true"`
}

//go:embed openapi.yml
var apiStr []byte

func (r *Router) ConfigRoutes() {
	HandleAPI(r, "GET /v1/config", func(ctx context.Context, req *http.Request) (*ConfigResponse, error) {
		warehouse := req.URL.Query().Get("warehouse")
		log.Printf("Get Config: `%v`", warehouse)

		// If server warehouse is different from requested warehouse, error
		if warehouse != "" && r.Config().Warehouse.Path != warehouse {
			return nil, &CustomRestError{
				Code:    http.StatusBadRequest,
				Type:    "InvalidConfigError",
				Message: "The requested warehouse does not match the server warehouse",
			}
		}

		return &ConfigResponse{
			Defaults:  map[string]string{"warehouse": r.Config().Warehouse.Path},
			Overrides: map[string]string{},
		}, nil
	})

	r.Mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	// Register API Docs Routes
	r.Mux.HandleFunc("GET /openapi-file.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write(apiStr)
	})

	r.Mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<!doctype html>
	<html>
		<head>
		<title>API Reference</title>
		<meta charset="utf-8" />
		<meta
			name="viewport"
			content="width=device-width, initial-scale=1" />
		</head>
		<body>
		<script
			id="api-reference"
			data-url="/openapi-file.yaml"></script>
		<script>
			var configuration = {
			  theme: 'purple',
			  hideModels: true,
			}
			document.getElementById('api-reference').dataset.configuration =
			  JSON.stringify(configuration)
		</script>
		<script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
		</body>
	</html>`))
	})
}

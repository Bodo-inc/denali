package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/handlers"
	"github.com/urfave/cli/v2"

	"github.com/Bodo-inc/denali/common"
	"github.com/Bodo-inc/denali/logic"
	"github.com/Bodo-inc/denali/routes"
)

var Version = func() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				return setting.Value
			}
		}
	}

	// Dev Commit
	return strings.TrimSpace(uuid.New().String())
}()

func RunServer(config *logic.Config) error {
	// Setup by loading config and connect to database
	s := logic.NewStateFromConfig(config)

	// Create Router and API routes
	mux := http.NewServeMux()
	r := routes.Router{Mux: mux, State: s}
	r.ConfigRoutes()
	r.NsRoutes()
	r.ViewRoutes()
	r.TableRoutes()

	var handler http.Handler = mux
	handler = handlers.LoggingHandler(os.Stdout, handler)

	// Net Listener for Extracting Final Address
	// If port == 0, a random available port is assigned
	l, err := net.Listen("tcp", fmt.Sprintf(":%v", config.Api.Port))
	if err != nil {
		return err
	}

	log.Printf("Started the Denali Catalog Server at `%v`", l.Addr().String())
	return http.Serve(l, handler)
}

func main() {
	f, path := logic.FindConfigPath(nil)
	f.Close()

	// Global Flags
	globalFlags := []cli.Flag{
		&cli.PathFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "Path to the configuration file",
			Value:   path,
			EnvVars: []string{"DENALI_CONFIG"},
		},
	}

	cli.CommandHelpTemplate = fmt.Sprintf(`{{template "helpNameTemplate" .}}

Usage: {{template "usageTemplate" .}}{{if .VisibleFlagCategories}}

Flags:{{template "visibleFlagCategoryTemplate" .}}{{else if .VisibleFlags}}

Flags:{{template "visibleFlagTemplate" .}}{{end}}

Global Flags:
%s
`, strings.Join(common.SliceMap(func(x cli.Flag) string { return "   " + x.String() }, globalFlags), "\n"))

	// Main CLI Configuration
	app := &cli.App{
		Name:    "denali",
		Version: Version,
		Usage:   "An open-source REST catalog for Apache Iceberg!",

		HideHelpCommand:      true,
		EnableBashCompletion: false,

		Flags: globalFlags,
		Commands: []*cli.Command{
			{
				Name:  "start",
				Usage: "Start the REST API server",
				Flags: []cli.Flag{
					&cli.Uint64Flag{
						Name:        "port",
						Aliases:     []string{"p"},
						Usage:       "Port to run the REST server on",
						Value:       0, // 0 means random port
						DefaultText: "From config file, or randomly chosen/generated if unset",
						EnvVars:     []string{"DENALI_API_PORT"},
					},
					&cli.BoolFlag{
						Name:               "temp",
						Aliases:            []string{"t"},
						DisableDefaultText: true,
						Usage:              "If set, run in temporary mode (in-memory database, temp local storage)",
						Value:              false,
					},
				},
				Action: func(ctx *cli.Context) error {
					var config *logic.Config
					if ctx.Bool("temp") {
						config = new(logic.Config)
						config.Database.Url = ":memory:"
						config.Database.Dialect = "sqlite3"

						// Create temporary directory
						path, err := os.MkdirTemp("", "denali-wh-")
						if err != nil {
							return fmt.Errorf("failed to create temporary directory: %v", err)
						}
						config.Warehouse.Path = path

					} else {
						path := ctx.Path("config")
						config = logic.LoadConfig(&path)
					}

					if port := ctx.Uint64("port"); port != 0 {
						config.Api.Port = port
					}

					err := RunServer(config)
					if err != nil {
						return fmt.Errorf("failed when running server: %v", err)
					}

					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

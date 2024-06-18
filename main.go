package main

import (
	"fmt"
	"log"
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

func runServer(config *logic.Config) error {
	// Setup by loading config and connect to database
	s := logic.NewState()

	// Create Router and API routes
	mux := http.NewServeMux()
	r := routes.Router{Mux: mux, State: s}
	r.ConfigRoutes()
	r.NsRoutes()
	r.ViewRoutes()
	r.TableRoutes()

	var handler http.Handler = mux
	handler = handlers.LoggingHandler(os.Stdout, handler)
	log.Printf("Starting server on port `%v`", config.Api.Port)
	return http.ListenAndServe(fmt.Sprintf(":%v", s.Config.Api.Port), handler)
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
						Name: "port", Aliases: []string{"p"},
						Usage:       "Port to run the REST server on",
						Value:       0,
						DefaultText: "config file, or 8080",
						EnvVars:     []string{"DENALI_API_PORT"},
					},
				},
				Action: func(ctx *cli.Context) error {
					path := ctx.Path("config")
					config := logic.LoadConfig(&path)
					if port := ctx.Uint64("port"); port != 0 {
						config.Api.Port = port
					}

					return runServer(config)
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

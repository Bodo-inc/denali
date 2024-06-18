package main

import (
	"fmt"
	"strings"

	"github.com/urfave/cli/v2"
)

func init() {
	cli.HelpFlag = &cli.BoolFlag{
		Name:               "help",
		Aliases:            []string{"h"},
		Usage:              "Show help information for denali",
		DisableDefaultText: true,
	}

	cli.AppHelpTemplate = `{{template "helpNameTemplate" .}}
Version: {{.Version}}

Usage: {{if .UsageText}}{{wrap .UsageText 3}}{{else}}{{.HelpName}} {{if .VisibleFlags}}[global flags]{{end}}{{if .Commands}} command [command flags]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}{{if .Args}}[arguments...]{{end}}{{end}}{{end}}{{if .VisibleCommands}}

Available Commands:{{template "visibleCommandCategoryTemplate" .}}{{end}}{{if .VisibleFlagCategories}}

Global Flags:{{template "visibleFlagCategoryTemplate" .}}{{else if .VisibleFlags}}

Global Flags:{{template "visibleFlagTemplate" .}}{{end}}
`

	cli.FlagStringer = func(fl cli.Flag) string {
		df, ok := fl.(cli.DocGenerationFlag)
		if !ok {
			return ""
		}

		var placeholder string = ""
		if df.TakesValue() {
			switch df.(type) {
			case *cli.StringFlag:
				placeholder = "string"
			case *cli.IntFlag, *cli.Int64Flag:
				placeholder = "int"
			case *cli.UintFlag, *cli.Uint64Flag:
				placeholder = "uint"
			case *cli.Float64Flag:
				placeholder = "float"
			case *cli.BoolFlag:
				placeholder = "bool"
			case *cli.DurationFlag:
				placeholder = "duration"
			case *cli.PathFlag:
				placeholder = "path"
			default:
				placeholder = "value"
			}
		}

		defaultValueString := ""
		if bf, ok := fl.(*cli.BoolFlag); !ok || !bf.DisableDefaultText {
			if s := df.GetDefaultText(); s != "" {
				defaultValueString = fmt.Sprintf("(default: %s)", s)
			}
		}

		names := df.Names()
		fullName := names[0]
		var nameStr string
		if len(names) > 1 && len(names[1]) == 1 {
			shortName := names[1]
			nameStr = fmt.Sprintf("-%s, --%s", shortName, fullName)
		} else {
			nameStr = fmt.Sprintf("     --%s", fullName)
		}

		envVars := df.GetEnvVars()
		envVarStr := ""
		if len(envVars) > 0 {
			envVarStr = fmt.Sprintf(" [$%s]", strings.Join(df.GetEnvVars(), ", $"))
		}

		usage := df.GetUsage()
		return fmt.Sprintf("%-21s%s %s%s", fmt.Sprintf("%s %s", nameStr, placeholder), usage, defaultValueString, envVarStr)
	}
}

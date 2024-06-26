package logic

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3/database"
	"github.com/sethvargo/go-envconfig"

	// Config Parsing
	toml "github.com/pelletier/go-toml/v2"

	"github.com/Bodo-inc/denali/common"
	ourIo "github.com/Bodo-inc/denali/iceberg-overload/io"
	models "github.com/Bodo-inc/denali/models"
	"github.com/Bodo-inc/denali/sqlgen"

	// Import generated SQL queries
	pg "github.com/Bodo-inc/denali/models/pg"
	sqlite "github.com/Bodo-inc/denali/models/sqlite"

	// Database Drivers
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

// ----------------------------- Config Parsing ---------------------------- //

type Config struct {
	Api struct {
		Port uint64 `env:"PORT"`
	} `env:", prefix=API_"`

	Warehouse struct {
		Path string `env:"PATH"`
	} `env:", prefix=WAREHOUSE_"`

	Database struct {
		Url     string `env:"URL"`
		Dialect string `env:"DIALECT"`
	} `env:", prefix=DATABASE_"`
}

func (c *Config) EmptyField() (string, string) {
	if c.Warehouse.Path == "" {
		return "warehouse", "path"
	} else if c.Database.Url == "" {
		return "database", "url"
	} else if c.Database.Dialect == "" {
		return "database", "dialect"
	} else {
		return "", ""
	}
}

var FolderName = "iceberg-server"
var ConfigName = "config.toml"
var DefaultConfig = `# Template for Iceberg REST Server Configuration

[api]
port = 8080  # Port to run the REST server on

[warehouse]
path = ""  # TODO: Provide path to local, S3, ABFS, or GCS location where Iceberg tables can be stored

[database]
url = ""         # Provide connection string to database 
type = "sqlite"  # Options: postgres, sqlite

`

func createConfig(path string) *os.File {
	newConfigPath := filepath.Join(path, FolderName, ConfigName)
	err := os.MkdirAll(filepath.Join(path, FolderName), 0755)
	if err != nil {
		log.Fatalf("Error creating config directory: %v", err)
	}

	f, err := os.Create(newConfigPath)
	if err != nil {
		log.Fatalf("Error creating config file: %v", err)
	}
	defer f.Close()

	// Write config template
	_, err = f.WriteString(DefaultConfig)
	if err != nil {
		log.Fatalf("Error writing config file: %v", err)
	}

	// Open File in Text Editor in Interactive Mode
	if os.Getenv("EDITOR") != "" {
		cmd := exec.Command(os.Getenv("EDITOR"), newConfigPath)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Run()
	}

	f, err = os.Open(newConfigPath)
	if err != nil {
		log.Fatalf("Error opening config file: %v", err)
	}
	return f
}

func FindConfigPath(initPath *string) (*os.File, string) {
	var configPaths []string
	if initPath != nil && *initPath != "" {
		configPaths = []string{*initPath}
	}

	// Check user home directory
	// Special case for MacOS, treat like Unix
	if runtime.GOOS == "darwin" && os.Getenv("HOME") != "" {
		configPaths = append(configPaths, filepath.Join(os.Getenv("HOME"), ".config"))
	}
	home, err := os.UserConfigDir()
	if err == nil {
		configPaths = append(configPaths, home)
	}

	// Check system config directory
	var paths []string
	switch runtime.GOOS {
	case "windows":
		if os.Getenv("PROGRAMDATA") != "" {
			paths = []string{os.Getenv("PROGRAMDATA")}
		} else {
			paths = []string{`C:\ProgramData`}
		}
	case "darwin":
		paths = []string{"/Library/Application Support"}
	default:
		if dirs := os.Getenv("XDG_CONFIG_DIRS"); dirs != "" {
			paths = strings.Split(dirs, ":")
		} else {
			paths = []string{"/etc"}
		}
	}
	configPaths = append(configPaths, paths...)

	// Go through all paths
	for _, path := range append(
		[]string{ConfigName},
		common.SliceMap(func(p string) string { return filepath.Join(p, FolderName, ConfigName) }, configPaths)...,
	) {
		f, err := os.Open(path)
		if err == nil {
			return f, path
		} else if !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("Error opening config file at `%v`: %v", path, err)
		}
	}

	// Create a new config file in an available directory
	if len(configPaths) == 0 {
		log.Fatalf("No config directory found")
	}

	return nil, filepath.Join(configPaths[0], FolderName, ConfigName)
}

func LoadConfig(optConfigPath *string) *Config {
	// New config in struct
	// Default Port: 0 = Randomly assigned available port
	config := new(Config)

	// Determine config path
	configFile, configPath := FindConfigPath(optConfigPath)
	if configFile == nil {
		configFile = createConfig(filepath.Dir(configPath))
	}
	log.Printf("Loading Config File at `%v`", configPath)

	// Load config from TOML
	if err := toml.NewDecoder(configFile).Decode(config); err != nil {
		log.Fatalf("Error Decoding Config: %v", err)
	}

	// Load from Environment Variables
	err := envconfig.ProcessWith(context.Background(), &envconfig.Config{
		Target:           config,
		Lookuper:         envconfig.PrefixLookuper("DENALI_", envconfig.OsLookuper()),
		DefaultOverwrite: true,
	})
	if err != nil {
		log.Fatalf("Error loading environment variables: %v", err)
	}

	// Check for empty fields
	if field, subfield := config.EmptyField(); field != "" {
		panic(fmt.Sprintf(
			"Required configuration parameter `%v.%v` is missing. Please either:\n"+
				"  - Set environment variable `DENALI_%v` or\n"+
				"  - Set in your `config.toml` file: \n\n"+
				"1| [%v]\n"+
				"2| %v = \"<value>\"\n\n",
			field, subfield,
			strings.ToUpper(field+"_"+subfield),
			field,
			subfield,
		))
	}

	// Verify that the database type is supported
	if !slices.Contains([]string{"pgx", "postgres", "sqlite", "sqlite3"}, config.Database.Dialect) {
		panic(fmt.Sprintf("Database `%s` is not supported as the storage backend. Only `postgres` and `sqlite` is.", config.Database.Dialect))
	}

	// Always use ths pgx driver for postgres
	if config.Database.Dialect == "postgres" {
		config.Database.Dialect = string(database.DialectPostgres)
	} else if config.Database.Dialect == "sqlite" {
		config.Database.Dialect = string(database.DialectSQLite3)
	}

	return config
}

// ---------------------------- State Management --------------------------- //

type State struct {
	queries models.Querier
	db      *sql.DB
	io      ourIo.IO
	Config  *Config
}

func NewStateFromConfig(config *Config) *State {
	// Get the server configuration
	if config == nil {
		config = LoadConfig(nil)
	}

	// Get Driver from Config Dialect
	var driver string
	switch config.Database.Dialect {
	case string(database.DialectPostgres):
		driver = "pgx"
	case string(database.DialectSQLite3):
		driver = "sqlite3"
	}

	// Connect to database
	log.Printf("Connecting to database of dialect `%v` with driver `%v`", config.Database.Dialect, driver)
	db, err := sql.Open(driver, config.Database.Url)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	// Check DB Version and Recreate Tables
	ctx := context.Background()
	err = sqlgen.SetupDB(db, config.Database.Dialect, context.Background())
	if err != nil {
		log.Fatalf("Unable to setup database: %v\n", err)
	}

	// Construct and Prepare Queries
	var queries models.Querier
	if driver == "pgx" {
		queries, err = pg.Prepare(ctx, db)
	} else {
		queries, err = sqlite.Prepare(ctx, db)
	}
	if err != nil {
		log.Fatalf("Unable to prepare queries: %v\n", err)
	}

	// Construct an Iceberg IO
	log.Printf("Using Path `%v` for Iceberg Warehouse", config.Warehouse.Path)
	io, err := ourIo.LoadFS(config.Warehouse.Path)
	if err != nil {
		log.Fatalf("failure when connecting to Iceberg warehouse: %v", err)
	}

	return &State{queries, db, io, config}
}

func NewState() *State {
	return NewStateFromConfig(nil)
}

func (s *State) TableUUIDToPath(uuid uuid.UUID) string {
	out, _ := url.JoinPath(s.Config.Warehouse.Path, uuid.String())
	return out
}

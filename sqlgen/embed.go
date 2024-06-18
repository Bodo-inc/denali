package sqlgen

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

//go:embed migrations/postgres/*.sql
var postgresMigrations embed.FS

//go:embed migrations/sqlite/*.sql
var sqliteMigrations embed.FS

func SetupDB(db *sql.DB, dialect string, ctx context.Context) error {
	var migration fs.FS
	var gooseDialect database.Dialect
	if dialect == "postgres" {
		gooseDialect = database.DialectPostgres
		migration, _ = fs.Sub(postgresMigrations, "migrations/postgres")
	} else {
		gooseDialect = database.DialectSQLite3
		migration, _ = fs.Sub(sqliteMigrations, "migrations/sqlite")
	}

	provider, err := goose.NewProvider(gooseDialect, db, migration)
	if err != nil {
		return fmt.Errorf("failed to create migration provider: %w", err)
	}

	// List status of migrations before applying them.
	_, err = provider.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get database version: %w", err)
	}

	_, err = provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("failed to apply schema: %w", err)
	}

	return nil
}

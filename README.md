# Denali Catalog

An open-source Iceberg Catalog that implements the REST API spec designed to be:

- Simple & Extensible: Written in Go to be easily extended, with minimal dependencies and <5000 lines of hand-written code.
- Customizable: Supports multiple database backends and warehouse storage locations. A ~20MB binary that can be easily deployed to most environments.
  - If you want a local ephemeral catalog, use SQLite and local filesystem storage.
  - If you want a scalable catalog, use PostgreSQL and any object store for storage.
- Performant & Scalable: Can be easily scaled horizontally (across multiple instances) and vertically (by increasing resources).

Written with ❤️ for Iceberg + Go, we want to build a REST catalog for the community with the community. Let's explore the full potential of Iceberg catalogs together. Open to contributions, feedback, and feature requests!

# Install
### Recommended:
```shell
go install github.com/Bodo-inc/denali
```
To install to your `$GOPATH/bin` directory.

### Manual:
```shell
git clone github.com/Bodo-inc/denali
cd denali
go build .
```
For manual builds.

# Getting Started

1) Start the server: `denali start`
2) If the server does not find a `config.toml` file, it will create one in the config directory (note, this depends on your OS on where it is). Your default text editor will be opened to edit the configuration file. 

More details of configuration options are provided below in:
- [Database Config](#supported-database-backends)
- [Warehouse Config](#supported-warehouse-storage-locations)

# Implemented REST API Endpoints 

| Type | Endpoint | Implemented |
| --- | --- | --- |
| GET | `/v1/config` | ![done]  |
| POST | `/v1/oauth/tokens` | ❌ Oauth Not Supported Yet |
| GET | `/v1/{prefix}/namespaces` | ![done] |
| POST | `/v1/{prefix}/namespaces` | ![done] |
| GET | `/v1/{prefix}/namespaces/{namespace}` | ![done] |
| HEAD | `/v1/{prefix}/namespaces/{namespace}` | ![done] |
| DELETE | `/v1/{prefix}/namespaces/{namespace}` | ![done] |
| POST | `/v1/{prefix}/namespaces/{namespace}/properties` | ![done] |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables` | ![done] |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables` | ![done] |
| POST | `/v1/{prefix}/namespaces/{namespace}/register` | 🟡 Untested |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | ![done] |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | ![done]  |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | ![done] |
| HEAD | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | ![done] |
| POST | `/v1/{prefix}/tables/rename` | 🟡 Untested |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` | ❌ Implemented but Metrics Not Tracked Yet |
| POST | `/v1/{prefix}/transactions/commit` | ❌ Multi-Table Transactions Not Supported Yet |
| GET | `/v1/{prefix}/namespaces/{namespace}/views` | ![done] |
| POST | `/v1/{prefix}/namespaces/{namespace}/views` | ❌ Not Implemented Yet |
| GET | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 🟡 Untested |
| POST | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | ❌ Not Implemented Yet |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 🟡 Untested |
| HEAD | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 🟡 Untested |
| POST | `/v1/{prefix}/views/rename` | 🟡 Untested  |

# Supported Database Backends

To configure the database backend the server should use, set the following properties in the configuration file:

```toml
[database]
url = "..."      # URL string to database. Env: $DENALI_DATABASE_URL
dialect = "..."  # Database dialect / type. Env: $DENALI_DATABASE_DIALECT
```

The server currently supports the following databases / dialects:

| Database | Supported | Dialect in Settings |  Driver Used |
| --- | --- | --- | --- |
| PostgreSQL | ![done] | `postgres` or `pgx` | [pgx](https://github.com/jackc/pgx)
| SQLite | ![done] | `sqlite` or `sqlite3` | [sqlite3](https://github.com/mattn/go-sqlite3)
| MySQL | 🟡 In Discussion

If you want to use a different database, please open an issue!

On initialization, the server will automatically set up the database with the necessary tables.

# Supported Warehouse Storage Locations

To configure where the warehouse is located, set the following properties in the configuration file:

```toml
[warehouse]
path = "..."  # Absolute path to warehouse. Env: $DENALI_WAREHOUSE_PATH
```

| Storage | Supported | 
| --- | --- |
| Local Filesystem | ![done] |
| S3 | 🟡 Untested | 
| Azure | 🟡 Untested |
| GCS | 🟡 Untested |

Note:
- Both the server and client is required to have access to the storage location. The server does not currently support sending delegated credentials.
- The server will not automatically set up the storage location or check on initialization.

# Docs

OpenAPI-based generated docs are hosted by the server at `http://<base-url>:<port>/docs`

[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg

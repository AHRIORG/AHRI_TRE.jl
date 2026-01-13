# AHRI_TRE.jl Copilot Instructions

## Project Overview
AHRI_TRE is a Julia-based Trusted Research Environment (TRE) managing health research data. The system combines PostgreSQL metadata storage with a DuckDB-based data lake (using the `ducklake` extension) to provide traceable data ingestion, transformation, and versioning for cohort studies.

**Key Architecture**: Dual-database system where PostgreSQL (`store`) holds metadata (studies, entities, variables, transformations) and DuckDB (`lake`) stores actual datasets via the ducklake catalog. Non-tabular data (BLOBs) are stored as DataFiles in the lake with SHA256 checksums.

## Core Concepts

### DataStore Structure
The `DataStore` object manages both connections:
- `store`: PostgreSQL connection for metadata (domains, studies, entities, variables, assets, transformations)
- `lake`: DuckDB connection with ducklake extension for dataset storage
- Initialize with `opendatastore(store)` and cleanup with `closedatastore(store)`
- Create new datastore with `createdatastore(store; superuser, superpwd)` - WARNING: destructive operation if a datalake with the same name exists

### Module Organization
Main module (`src/AHRI_TRE.jl`) includes files in this order:
1. `constants.jl` - TRE_TYPE_* constants for data types
2. `utils.jl` - SHA256 hashing, URI utilities
3. `tredatabase.jl` - PostgreSQL schema creation, core CRUD operations
4. `redcap.jl` - REDCap API integration
5. `meta_common.jl` - SQL metadata extraction base
6. `meta_*.jl` - Database-flavour-specific metadata extractors (MSSQL, PostgreSQL, DuckDB, SQLite, MySQL)

### Database Flavours (Multiple Dispatch Pattern)
Use multiple dispatch for flexible data sourcing from different SQL databases:
- `PostgreSQLFlavour`, `MSSQLFlavour`, `DuckDBFlavour`, `SQLiteFlavour`, `MySQLFlavour`
- Each flavour overrides: `map_sql_type_to_tre()`, `get_column_comment()`, `is_enum_type()`, etc.
- Pass flavour as last argument: `sql_meta(conn, sql, domain_id, PostgreSQLFlavour())`
- String shorthand also accepted: `sql_to_dataset(..., conn, "MSSQL", sql, ...)`

## Critical Functions

### `sql_meta(conn, sql, domain_id, flavour)`
Core metadata extraction from SQL queries:
- Executes query with LIMIT 0 to get column metadata without fetching data
- Maps SQL types to TRE_TYPE_* constants (INTEGER, FLOAT, STRING, DATE, DATETIME, TIME, CATEGORY)
- Auto-detects CATEGORY types from ENUMs, CHECK constraints, or small reference tables (<250 rows)
- Returns `Vector{Variable}` with type info and domain-scoped vocabularies for categorical data
- All vocabularies created during SQL metadata extraction are automatically associated with the provided `domain_id`

### Data Ingestion Workflows

#### REDCap Ingestion
1. `ingest_redcap_project()` - Downloads metadata + records as EAV CSV, stores as DataFile
2. `transform_eav_to_dataset()` - Pivots EAV format to wide dataset in ducklake
3. `register_redcap_datadictionary()` - Extract Variable metadata from REDCap data dictionary

#### SQL Database Ingestion
Most common sources: **SQL Server** (via ODBC) and **DuckDB** files
1. Connect to source: `connect_mssql(server, database, user, pwd)` for SQL Server (auto-registers ODBC driver)
2. Extract data and metadata: `sql_to_dataset(store, study, domain, dataset_name, conn, flavour, sql; description=...)`
3. The `sql_to_dataset()` function:
   - Executes SQL query on source database
   - Extracts column metadata via `sql_meta()`
   - Stores data in ducklake with full provenance tracking
   - Automatically creates transformation records

**Multi-database support**: The `meta_*.jl` files provide flexibility to ingest from any supported SQL database (MSSQL, PostgreSQL, DuckDB, SQLite, MySQL) into the TRE

### Data Provenance Pattern
All data operations tracked via transformations:
```julia
transformation = create_transformation(store, name, type_id, status_id, study, domain)
add_transformation_input(store, transformation, input_asset_version)
add_transformation_output(store, transformation, output_asset_version)
```
### Transformation Types
- **Ingest** (1) Ingest data from external sources into the TRE. Transformation inputs will be empty, and outputs will be the newly created asset versions.
- **Transform** (2) Apply data transformations within the TRE, such as cleaning or reshaping datasets. Both inputs and outputs will reference existing asset versions.
- **Entity** (3) Create or modify entities and their relationships within the TRE. Inputs are datasets, and outputs will be the affected entity records - but not listed as asset versions.
- **Export** (4) Export data or entities from the TRE to external systems or formats. Inputs will be asset versions, and outputs will be empty.
- **Repository** (5) Manage repository-related transformations within the TRE. Transfer datasets to an associated NADA repository. Inputs will be asset versions, and outputs will be empty.

## Development Workflows

### Environment Configuration
- Load `.env` files with `ConfigEnv.dotenv()` for database credentials
- Test suite uses layered config: root `.env` loaded first, then `test/.env` overrides
- Required env vars: `TRE_SERVER`, `TRE_USER`, `TRE_PWD`, `TRE_DBNAME`, `LAKE_PASSWORD`, `LAKE_USER`, `TRE_LAKE_PATH`
- MSSQL ODBC driver automatically registered by `connect_mssql()` from system installation paths
- The underlying storage for the data lake is accessed via a samba share. TRE_LAKE_PATH should be the path to this share.

### Testing Pattern
All test files follow this structure:
```julia
using ConfigEnv
dotenv(ENV_FILE)  # Load environment first
using AHRI_TRE    # Then load module

@testset "Test Suite Name" begin
    # Test database connections as fixtures
    # Use try-finally to ensure cleanup
end
```
Run with: `julia --project=. test/runtests.jl` or via Pkg.test()

### Database Initialization
```julia
# Create new datastore (destructive - drops existing DB and lake if present)
createdatastore(store; superuser="postgres", superpwd="password")

# Setup includes: createstudies(), createtransformations(), createvariables(), 
# createassets(), createentities()
```

### DuckDB Deprecation Handling
DuckDB.jl has deprecated result-chunk APIs. In `meta_duckdb.jl`, use `duckdb_query_columntable()` helper instead of standard DataFrames materialization to avoid deprecation warnings during tests.

## Data Model Key Points

### Entity-Relation Pattern
- `Entity` - Domain-scoped entities (e.g., Individual, Household) with ontology links
- `EntityRelation` - Relationships between entities with ontology classification
- Use `create_entity!()` and `create_entity_relation!()` - note the bang for mutation
- **Note**: Full ontology integration is work in progress; basic structure is in place

### Asset Versioning
- Assets have versions (tracked in `asset_versions`)
- DataFiles are BLOBs with SHA256 checksums stored in lake
- Datasets are ducklake-managed tables with column metadata in `dataset_variables`
- Always use `get_latest_version(asset)` to retrieve current version

### Domain Scoping
Domains provide namespace isolation:
- Variable names unique within domain
- Vocabularies scoped within domains (UNIQUE constraint on `domain_id`, `name`)
- Studies link to domains via `study_domains`
- Entities and relations scoped to domains

### Vocabulary Management
Vocabularies are domain-scoped and provide controlled value sets for categorical variables:
- `Vocabulary` struct includes `domain_id::Int` field linking to a specific domain
- Database constraint: `UNIQUE(domain_id, name)` ensures vocabulary names are unique within each domain
- Same vocabulary name can exist in different domains with different value sets
- Create/retrieve vocabularies using `ensure_vocabulary!(store, domain_id, name, description, items)`
- Three `get_vocabulary()` overloads:
  - `get_vocabulary(store, vocab_id)` - retrieve by ID
  - `get_vocabulary(store, domain_id, name)` - domain-scoped lookup
  - `get_vocabulary(store, name)` - unique name lookup (errors if name exists in multiple domains)
- REDCap and SQL metadata extraction automatically scope vocabularies to their domain context

## Common Patterns

### Type Constants
Always use named constants:
- Value types: `TRE_TYPE_INTEGER`, `TRE_TYPE_STRING`, `TRE_TYPE_CATEGORY`, etc.

### Upsert Pattern
Several functions use "upsert" naming for insert-or-update semantics:
- `upsert_study!()`, `upsert_entity!()`, `upsert_variable!()`, `upsert_domain!()`

### Lake Alias
DuckDB lake attached with constant `LAKE_ALIAS = "tre_lake"` - used in queries referencing lake tables.

## Working with Datasets

### Reading Datasets
```julia
# By study and dataset name
df = read_dataset(store, "study_name", "dataset_name")

# By DataSet object
dataset = get_dataset(store, "study_name", "dataset_name")
df = read_dataset(store, dataset)

# Access all versions
versions = get_dataset_versions(store, "study_name", "dataset_name")
```

### Exporting Datasets
```julia
# To Arrow format (efficient for large data)
path = dataset_to_arrow(store, dataset, output_dir)

# To CSV (optionally compressed)
path = dataset_to_csv(store, dataset, output_dir; compress=true)
```

### Querying Datasets in DuckDB
Datasets are stored as tables in the ducklake catalog with alias `LAKE_ALIAS = "tre_lake"`:
```julia
# Direct SQL query on lake connection
sql = "SELECT * FROM tre_lake.schema_name.dataset_name WHERE condition"
result = DuckDB.query(store.lake, sql) |> DataFrame

# Dataset table names follow pattern: domain_id.dataset_name
table_name = get_datasetname(dataset, include_schema=true)
```

## Gotchas
- Always close connections: use try-finally blocks with `closedatastore()`
- REDCap data is EAV format - must transform to dataset before analysis
- `transform_eav_to_dataset()` can fail and returns `Union{DataSet, Nothing}`; callers should handle `nothing` (e.g. log and abort) rather than assuming a dataset is always returned.
- SHA256 hashes are lowercase hex strings
- PostgreSQL role creation includes conditional existence check to avoid errors
- DuckDB requires `LOAD ducklake` and `LOAD postgres` extensions before attaching
- MSSQL ODBC driver automatically discovered and registered from system paths
- `git_commit_info()` must not default to `@__DIR__` / `@__FILE__` inside the package, because those point at the installed package source (often under `.julia/...`) rather than the user's script.
    - Current behavior: resolves an external caller script by scanning the stacktrace; falls back to `Base.PROGRAM_FILE`, then `pwd()`.
    - If no git repo is found, it returns `repo_url`/`commit` as `missing`, but keeps `script_relpath` as the absolute script path when available.
    - In VS Code, avoid accidentally recording the Julia extension `terminalserver.jl` as the "caller" by filtering internal/editor frames.
- In the Asset struct asset_type is a string field ("dataset","file"), not an integer constant
- In the datastore asset_types are represented by an enum type (dataset, file)
- In the Transformation transformation_type is a string field ("ingest","transform", "entity", "export", "repository"), not an integer constant
- In the datastore transformation_types are represented by an enum type (ingest, transform, entity, export, repository)
- Julia container types are invariant: prefer `AbstractVector{<:T}` in method signatures when accepting vectors of subtype elements (e.g. vocabulary items).
- DataFrames joins: ensure you are joining DataFrames (not vectors of structs). When joining on differently named columns, prefer renaming to a common name before `innerjoin` to avoid `makeunique` column renaming surprises.
- In test sets never use @skip_test; instead let the test fail with an informative error message
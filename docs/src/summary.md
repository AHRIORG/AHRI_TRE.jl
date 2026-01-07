# AHRI_TRE.jl – Codebase Summary

AHRI_TRE.jl is a Julia package for operating a *Trusted Research Environment (TRE)* that stores **research metadata** in PostgreSQL while storing **actual data assets** (datasets and files) in a DuckDB-based data lake managed through the `ducklake` catalog.

At a high level, the package provides:
- A dual-database `DataStore` abstraction (PostgreSQL “store” + DuckDB “lake”).
- A metadata model for studies, domains, variables, assets, versions, and vocabularies.
- Ingestion workflows for files, REDCap projects, and SQL query results from multiple database flavours.
- Transformation tracking (provenance) that links inputs/outputs to versioned assets.
- Convenience APIs to retrieve datasets/files and export datasets to common formats.

## Architecture

### Dual storage design
- **PostgreSQL (metadata store)**: holds structured metadata such as studies, domains, variables, vocabularies, assets, asset versions, transformations, and entity/relationship definitions.
- **DuckDB (data lake)**: holds tabular datasets as DuckDB tables under a `ducklake` catalog attached via the DuckDB `postgres` + `ducklake` extensions.

The `DataStore` type carries the connection parameters and open connections:
- `store.store`: PostgreSQL connection
- `store.lake`: DuckDB connection (with `ducklake` attached)

## Core Data Model (as implemented)

### Studies and domains
- **Study**: a research study. Studies are linked to one or more **domains**.
- **Domain**: a namespace for variable/entity identifiers and ontology context.

### Variables and vocabularies
- **Variable**: column-level metadata (name, type, description, key role, optional ontology links).
- **Vocabulary / VocabularyItem**: categorical “code lists” for variables of category-like types (e.g. enum/dropdown fields).

### Assets and versions
- **Asset**: a named digital asset in a study (type is typically `"dataset"` or `"file"`).
- **AssetVersion**: versioned identity for an asset (major/minor/patch; `is_latest` flag).

### Data assets
- **DataSet**: a tabular dataset backed by a DuckDB table and described by variables in the metadata store.
- **DataFile**: a file-based asset stored in the lake filesystem with metadata such as storage URI, EDAM format identifier, and SHA-256 digest.

### Entities and relations (semantic layer)
The codebase includes tables and APIs for:
- **Entity** and **EntityRelation** definitions (with optional ontology namespace/class).
- **Entity instances** and **relation instances** in a study, enabling linking assets to real-world entities.

## Provenance / Transformations

All major operations are designed to be recorded as **Transformations**:
- Types include `ingest`, `transform`, `entity`, `export`, `repository`.
- Transformation records can store optional git provenance: repository URL, commit hash, and script path.
- Transformation inputs/outputs link transformations to the versioned assets they consume/produce.

This provides traceability across:
- Raw file ingest → file version
- SQL ingest → dataset version
- EAV-to-wide transforms → dataset versions linked back to source EAV exports

## Main Workflows

### 1) Open/close a datastore
- `opendatastore(store)` opens both PostgreSQL and DuckDB connections.
- `closedatastore(store)` closes them.

### 2) Ingest files into the TRE
- `ingest_file(...)` copies a local file into the lake path, optionally compresses (zstd), computes SHA-256, registers/versions the asset, and records an `ingest` transformation.
- `ingest_file_version(...)` ingests a new version of an existing file asset.

### 3) REDCap ingestion
REDCap integration is implemented via HTTP POSTs to the REDCap API:
- `register_redcap_datadictionary(...)` downloads metadata and registers variables (including vocabularies for choice fields).
- `ingest_redcap_project(...)` exports records to EAV CSV, registers the file in the lake, and records provenance.
- `transform_eav_to_dataset(...)` pivots an EAV CSV into a wide DuckDB table (dataset), links variables, and records a `transform` transformation.

### 4) SQL query → dataset ingestion (multi-database)
The `sql_to_dataset(...)` workflow:
- Extracts column metadata via `sql_meta(...)`.
- Creates or versions the dataset asset.
- Persists dataset variables (schema) to the metadata store.
- Loads query results into the DuckDB lake table.
- Records an `ingest` transformation.

## SQL Metadata Extraction (multi-dispatch)

The codebase supports extracting schema metadata from SQL queries using a flavour-based dispatch model:
- `MSSQLFlavour`, `PostgreSQLFlavour`, `DuckDBFlavour`, `SQLiteFlavour`, `MySQLFlavour`

Core behaviors include:
- Mapping SQL types → TRE type constants.
- Best-effort extraction of column descriptions.
- Category detection via enums, constraints, or small lookup/code tables.

## Retrieval and Export

- `get_dataset(store, study_name, dataset_name)` and `get_dataset_versions(...)` return dataset objects with their variables loaded.
- `read_dataset(...)` reads a dataset from the DuckDB lake into a `DataFrame`.
- `dataset_to_csv(...)` and `dataset_to_arrow(...)` export datasets to common file formats.

## Utilities & Conventions

The package includes utilities used throughout workflows:
- SHA-256 digesting and verification for file integrity.
- Conversion between local paths and `file://` URIs.
- NCName sanitization (`to_ncname`) to ensure stable, safe identifiers for datasets/assets.
- Git provenance helpers (`git_commit_info`) used when recording transformations.

## Operational Notes

- Most ingest/transform operations run inside explicit transactions; failures attempt rollback.
- Configuration is commonly provided via environment variables (often loaded from `.env`).
- DuckDB is attached to the lake using the `ducklake:postgres:...` connection string and an alias (e.g. `tre_lake`).

## Current Limitations (visible in code)

- File encryption is not implemented (encryption flags exist but currently raise).
- Some metadata extraction is “best effort” for complex SQL (table-name extraction and comments/constraints can be flavour-dependent).

---

*This summary is derived from the package’s module exports and the implemented workflows in the `src/` code, especially `AHRI_TRE.jl`, `tredatabase.jl`, `meta_common.jl`, and `redcap.jl`.*

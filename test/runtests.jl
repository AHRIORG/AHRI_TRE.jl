using Test
using ConfigEnv
const TEST_DIR = dirname(@__FILE__)
const PROJECT_ROOT = dirname(TEST_DIR)
const ROOT_ENV_FILE = joinpath(PROJECT_ROOT, ".env")
const TEST_ENV_FILE = joinpath(TEST_DIR, ".env")
const DUCKDB_ENV_SQL = joinpath(TEST_DIR, "duckdb_testenv.sql")
const SQLITE_ENV_SQL = joinpath(TEST_DIR, "sqlite_testenv.sql")
const POSTGRES_ENV_SQL = joinpath(TEST_DIR, "postgres_test.sql")
const SQL_QUERY = "SELECT cause, COUNT(*) AS n FROM deaths GROUP BY cause"
const DUCKDB_TEST_DB = joinpath(TEST_DIR, "duckdb_test.db")
const SQLITE_TEST_DB = joinpath(TEST_DIR, "sqlite_test.db")

# Load root env first, then allow test/.env to override for test runs.
if isfile(ROOT_ENV_FILE)
    dotenv(ROOT_ENV_FILE)
end

if isfile(TEST_ENV_FILE)
    dotenv(TEST_ENV_FILE; overwrite=true)
end

using AHRI_TRE

@testset verbose = true "AHRI_TRE.jl" begin
    # Include MSSQL connection tests
    include("test_mssql_connection.jl")
    
    # Include sql_meta tests
    include("test_sql_meta.jl")

    # Include DuckDB sql_meta tests
    include("test_duckdb_sql_meta.jl")

    # Include SQLite sql_meta tests
    include("test_sqlite_sql_meta.jl")

    # Include PostgreSQL sql_meta tests (TRE_SERVER)
    include("test_postgresql_sql_meta.jl")

    # Include sql_to_dataset integration tests
    include("sql_to_dataset_tests.jl")

    # Include domain/study CRUD tests
    include("test_domain_study.jl")

    # Include datafile ingest + metadata tests
    include("test_datafiles.jl")

    # Include git_commit_info script path tests
    include("test_git_commit_info.jl")
end

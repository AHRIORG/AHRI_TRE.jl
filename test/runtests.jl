using Test
using ConfigEnv
const TEST_DIR = dirname(@__FILE__)
const PROJECT_ROOT = dirname(TEST_DIR)
const ROOT_ENV_FILE = joinpath(PROJECT_ROOT, ".env")
const TEST_ENV_FILE = joinpath(TEST_DIR, ".env")

# Load root env first, then allow test/.env to override for test runs.
if isfile(ROOT_ENV_FILE)
    dotenv(ROOT_ENV_FILE)
end

if isfile(TEST_ENV_FILE)
    dotenv(TEST_ENV_FILE; overwrite=true)
end

using AHRI_TRE

@testset "AHRI_TRE.jl" begin
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
end

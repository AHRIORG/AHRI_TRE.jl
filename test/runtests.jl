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

# Set up global test datastore
function setup_tre_test_datastore()
    keys = [
        "TRE_SERVER",
        "TRE_USER",
        "TRE_PWD",
        "TRE_TEST_DBNAME",
        "TRE_TEST_LAKE_PATH",
        "LAKE_USER",
        "LAKE_PASSWORD",
        "TRE_TEST_LAKE_DB",
    ]
    values = Dict{String,String}()
    missing_keys = String[]
    for key in keys
        value = get(ENV, key, "")
        if isempty(value)
            push!(missing_keys, key)
        else
            values[key] = value
        end
    end
    
    if !isempty(missing_keys)
        @warn "Test datastore not configured. Missing environment variables: $(join(missing_keys, ", "))"
        return nothing
    end
    
    store = AHRI_TRE.DataStore(
        server=values["TRE_SERVER"],
        user=values["TRE_USER"],
        password=values["TRE_PWD"],
        dbname=values["TRE_TEST_DBNAME"],
        lake_data=values["TRE_TEST_LAKE_PATH"],
        lake_user=values["LAKE_USER"],
        lake_password=values["LAKE_PASSWORD"],
        lake_db=values["TRE_TEST_LAKE_DB"],
    )
    try
        return AHRI_TRE.opendatastore(store)
    catch e
        @warn "Unable to open TRE test datastore" exception=(e, catch_backtrace())
        return nothing
    end
end

const TRE_TEST_STORE = setup_tre_test_datastore()

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

    # Include vocabulary tests
    include("test_vocabulary.jl")

    # Include variable CRUD tests
    include("test_variable_crud.jl")
end

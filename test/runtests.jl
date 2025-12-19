using AHRI_TRE
using Test
using ConfigEnv

dotenv()
const TEST_DIR = dirname(@__FILE__)
const PROJECT_ROOT = dirname(dirname(@__FILE__))
const ENV_FILE = joinpath(PROJECT_ROOT, ".env")

if isfile(ENV_FILE)
    dotenv(ENV_FILE)
else
    @warn "No .env file found at: $ENV_FILE"
end

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
end

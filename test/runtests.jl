using AHRI_TRE
using Test

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
end

using AHRI_TRE
using Test

@testset "AHRI_TRE.jl" begin
    # Include MSSQL connection tests
    include("test_mssql_connection.jl")
    
    # Include sql_meta tests
    include("test_sql_meta.jl")
end

using AHRI_TRE
using Test
using ODBC
using DBInterface
using ConfigEnv
using DataFrames

# Load environment variables from .env file
# Use the project root directory to find .env file (works when run via Pkg.test())

@testset "MSSQL Connection Test" begin
    # Load environment variables
    mssql_server = get(ENV, "MSQLServer", "")
    mssql_db = get(ENV, "MSQLServerDB", "")
    mssql_user = get(ENV, "MSQLServerUser", "")
    mssql_pw = get(ENV, "MSQLServerPW", "")
    
    if isempty(mssql_server) || isempty(mssql_db) || isempty(mssql_user) || isempty(mssql_pw)
        @warn "MSSQL connection test SKIPPED: Missing environment variables (MSQLServer, MSQLServerDB, MSQLServerUser, MSQLServerPW)"
        @test_skip "MSSQL environment variables not configured"
        return
    end
    
    @testset "Connect to MSSQL Server" begin
        # Check if ODBC driver is installed
        if !isfile(AHRI_TRE.ODBC_DRIVER_PATH)
            @warn "MSSQL ODBC Driver not found at: $(AHRI_TRE.ODBC_DRIVER_PATH)"
            @test_skip "ODBC Driver 18 for SQL Server not installed"
            return
        end
        
        conn = nothing
        try
            conn = AHRI_TRE.connect_mssql(mssql_server, mssql_db, mssql_user, mssql_pw)
            @test conn !== nothing
            
            # Test a simple query
            @testset "Execute Simple Query" begin
                result = DBInterface.execute(conn, "SELECT @@VERSION AS version") |> DataFrame
                @test nrow(result) == 1
                @test hasproperty(result, :version)
            end
            
            # Test getting database name
            @testset "Verify Database" begin
                result = DBInterface.execute(conn, "SELECT DB_NAME() AS current_db") |> DataFrame
                @test nrow(result) == 1
                @test result[1, :current_db] == mssql_db
            end
            
        catch e
            @error "MSSQL connection test FAILED" exception=(e, catch_backtrace())
            @test false
        finally
            if !isnothing(conn)
                DBInterface.close!(conn)
            end
        end
    end
end

using AHRI_TRE
using Test
using ODBC
using DBInterface
using DataFrames

@testset "MSSQL Connection Test" begin
    # Load environment variables
    mssql_server = get(ENV, "MSQLServer", "")
    mssql_db = get(ENV, "MSQLServerDB", "")
    mssql_user = get(ENV, "MSQLServerUser", "")
    mssql_pw = get(ENV, "MSQLServerPW", "")

    @testset "Connect to MSSQL Server" begin

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
            @error "MSSQL connection test FAILED" exception = (e, catch_backtrace())
            @test false
        finally
            if !isnothing(conn)
                DBInterface.close!(conn)
            end
        end
    end
end

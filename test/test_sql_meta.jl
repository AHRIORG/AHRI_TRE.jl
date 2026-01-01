using AHRI_TRE
using Test
using ODBC
using DBInterface
using ConfigEnv
using DataFrames

# Load environment variables from .env file
# const PROJECT_ROOT = dirname(dirname(@__FILE__))
# const ENV_FILE = joinpath(PROJECT_ROOT, ".env")

if isfile(ENV_FILE)
    dotenv(ENV_FILE)
else
    @warn "No .env file found at: $ENV_FILE"
end

"""
    check_test_tables_exist(conn) -> Bool

Check if the test tables from mssql_testenv.sql exist in the database.
"""
function check_test_tables_exist(conn)
    result = DBInterface.execute(conn, """
        SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME IN ('causes', 'deaths', 'sites', 'sources', 'cause_categories')
    """) |> DataFrame
    return result[1, :cnt] == 5
end

@testset "AHRI_TRE.sql_meta Tests" begin
    # Load environment variables
    mssql_server = get(ENV, "MSQLServer", "")
    mssql_db = get(ENV, "MSQLServerDB", "")
    mssql_user = get(ENV, "MSQLServerUser", "")
    mssql_pw = get(ENV, "MSQLServerPW", "")
    
    if isempty(mssql_server) || isempty(mssql_db) || isempty(mssql_user) || isempty(mssql_pw)
        @warn "AHRI_TRE.sql_meta tests SKIPPED: Missing environment variables"
        @test_skip "MSSQL environment variables not configured"
        return
    end
    
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
        
        @testset "Test Environment Check" begin
            tables_exist = check_test_tables_exist(conn)
            
            if !tables_exist
                @warn "Test tables not found. Please run mssql_testenv.sql to create them."
                @test_skip "Test tables not available"
                return
            end
            
            @test tables_exist
            
            # Verify data exists
            causes_count = DBInterface.execute(conn, "SELECT COUNT(*) as cnt FROM causes") |> DataFrame
            @test causes_count[1, :cnt] >= 2
            
            deaths_count = DBInterface.execute(conn, "SELECT COUNT(*) as cnt FROM deaths") |> DataFrame
            @test deaths_count[1, :cnt] >= 1
        end
        
        # Only run AHRI_TRE.sql_meta tests if tables exist
        if !check_test_tables_exist(conn)
            @warn "Skipping AHRI_TRE.sql_meta tests: test tables not available"
            return
        end
        
        @testset "AHRI_TRE.sql_meta Basic Query" begin
            sql = "SELECT death_id, site_id, cause FROM deaths"
            # domain_id is an Int, not a String
            variables = AHRI_TRE.sql_meta(conn, sql, 1, "MSSQL")
            
            @test variables isa Vector
            @test length(variables) == 3  # Three columns
            
            # Check variable names
            var_names = [v.name for v in variables]
            @test "death_id" in var_names
            @test "site_id" in var_names
            @test "cause" in var_names
            
            # Check domain_id is set correctly
            @test all(v.domain_id == 1 for v in variables)
            
        end
        
        @testset "AHRI_TRE.sql_meta with JOIN Query" begin
            sql = """
                SELECT d.death_id, d.site_id, d.cause, 
                       c.label as cause_label, c.description as cause_description,
                       s.source_id
                FROM deaths d
                JOIN causes c ON d.cause = c.code
                JOIN sites s ON d.site_id = s.site_id
            """
            variables = AHRI_TRE.sql_meta(conn, sql, 2, "MSSQL")
            
            @test variables isa Vector
            @test length(variables) == 6  # Six columns in the query
            
            var_names = [v.name for v in variables]
            @test "death_id" in var_names
            @test "cause_label" in var_names
            @test "cause_description" in var_names
            @test "source_id" in var_names
            
        end
        
        @testset "AHRI_TRE.sql_meta Type Detection" begin
            # Test with a query that includes different types
            sql = "SELECT code, label, description FROM causes"
            variables = AHRI_TRE.sql_meta(conn, sql, 3, "MSSQL")
            
            @test variables isa Vector
            @test length(variables) == 3
            
            # Find each variable by name
            code_var = first(filter(v -> v.name == "code", variables))
            label_var = first(filter(v -> v.name == "label", variables))
            
            # code should be integer type (TRE_TYPE_INTEGER = 1)
            @test code_var.value_type_id == AHRI_TRE.TRE_TYPE_INTEGER
            
            # label should be string type (TRE_TYPE_STRING = 3)
            @test label_var.value_type_id == AHRI_TRE.TRE_TYPE_STRING
            
        end
        
        @testset "AHRI_TRE.sql_meta Vocabulary Detection" begin
            # Test that variables with foreign key to small tables get vocabulary populated
            sql = "SELECT death_id, site_id, cause FROM deaths"
            variables = AHRI_TRE.sql_meta(conn, sql, 4, "MSSQL")
            
            @test variables isa Vector
            @test length(variables) == 3
            
            # Check that vocabulary is populated for some variables (cause references causes table)
            vars_with_vocab = filter(v -> !ismissing(v.vocabulary) && !isempty(v.vocabulary.items), variables)
            
            # The cause column should have vocabulary from the causes table
            cause_var = first(filter(v -> v.name == "cause", variables))
            @test cause_var !== nothing
            @test !ismissing(cause_var.vocabulary)
            @test cause_var.value_type_id == AHRI_TRE.TRE_TYPE_CATEGORY
            @test length(cause_var.vocabulary.items) >= 2
            @test all(i -> i isa AHRI_TRE.VocabularyItem, cause_var.vocabulary.items)
            codes = [i.code for i in cause_var.vocabulary.items]
            values = [i.value for i in cause_var.vocabulary.items]
            @test "Natural" in codes
            @test "Unnatural" in codes
            @test 1 in values
            @test 2 in values
        end
        
        @testset "AHRI_TRE.sql_meta Variable Structure" begin
            sql = "SELECT code, label FROM causes"
            variables = AHRI_TRE.sql_meta(conn, sql, 5, "MSSQL")
            
            @test length(variables) >= 2
            
            # Check Variable structure fields exist
            v = first(variables)
            @test hasfield(typeof(v), :variable_id)
            @test hasfield(typeof(v), :name)
            @test hasfield(typeof(v), :domain_id)
            @test hasfield(typeof(v), :value_type_id)
            
        end
        
    catch e
        @error "AHRI_TRE.sql_meta tests FAILED" exception=(e, catch_backtrace())
        @test false
    finally
        if !isnothing(conn)
            DBInterface.close!(conn)
        end
    end
end

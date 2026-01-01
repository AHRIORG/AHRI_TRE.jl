using AHRI_TRE
using Test
using LibPQ
using DBInterface
using DataFrames
using Random

const TEST_DIR = dirname(@__FILE__)
const POSTGRES_ENV_SQL = joinpath(TEST_DIR, "postgres_test.sql")

function _tre_pg_port()::Int
    port_str = get(ENV, "TRE_PORT", "5432")
    try
        return parse(Int, port_str)
    catch
        return 5432
    end
end

function pg_connect(dbname::AbstractString; server::AbstractString, user::AbstractString, password::AbstractString, port::Int)
    conninfo = "host=$(server) port=$(port) dbname=$(dbname) user=$(user) password=$(password)"
    return DBInterface.connect(LibPQ.Connection, conninfo)
end

function exec_sql_file(conn, sql_path::AbstractString)
    sql_text = read(sql_path, String)
    # Remove full-line comments
    sql_text = join(filter(line -> !startswith(strip(line), "--"), split(sql_text, '\n')), "\n")

    for stmt in split(sql_text, ';')
        s = strip(stmt)
        isempty(s) && continue
        DBInterface.execute(conn, s)
    end
end

function check_postgresql_test_tables_exist(conn)::Bool
    result = DBInterface.execute(conn, """
        SELECT COUNT(*) as cnt
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN ('causes', 'deaths', 'sites', 'sources', 'cause_categories')
    """) |> DataFrame
    return result[1, :cnt] == 5
end

@testset "AHRI_TRE.sql_meta PostgreSQL Tests" begin
    @test isfile(POSTGRES_ENV_SQL)

    server = get(ENV, "TRE_SERVER", "")
    user = get(ENV, "SUPER_USER", "")
    password = get(ENV, "SUPER_PWD", "")
    port = _tre_pg_port()

    if isempty(server) || isempty(user) || isempty(password)
        @warn "PostgreSQL AHRI_TRE.sql_meta tests SKIPPED: Missing environment variables (TRE_SERVER, SUPER_USER, SUPER_PWD)"
        @test_skip "TRE PostgreSQL credentials not configured"
        return
    end

    # Create a temporary database name (safe identifier characters only)
    test_dbname = "tre_sql_meta_test_" * lowercase(randstring(10))

    admin_conn = nothing
    test_conn = nothing

    try
        admin_conn = pg_connect("postgres"; server=server, user=user, password=password, port=port)

        # Create temporary database (requires CREATEDB privilege)
        try
            DBInterface.execute(admin_conn, "CREATE DATABASE $test_dbname")
        catch e
            msg = sprint(showerror, e)
            @warn "PostgreSQL AHRI_TRE.sql_meta tests SKIPPED: Could not create temporary database '$test_dbname'" exception=(e, catch_backtrace())
            @test_skip msg
            return
        end

        test_conn = pg_connect(test_dbname; server=server, user=user, password=password, port=port)

        # Load schema + data
        exec_sql_file(test_conn, POSTGRES_ENV_SQL)

        @testset "Test Environment Check" begin
            @test check_postgresql_test_tables_exist(test_conn)

            causes_count = DBInterface.execute(test_conn, "SELECT COUNT(*) as cnt FROM causes") |> DataFrame
            @test causes_count[1, :cnt] >= 2

            deaths_count = DBInterface.execute(test_conn, "SELECT COUNT(*) as cnt FROM deaths") |> DataFrame
            @test deaths_count[1, :cnt] >= 1
        end

        @testset "AHRI_TRE.sql_meta Basic Query" begin
            sql = "SELECT death_id, site_id, cause FROM deaths"
            variables = AHRI_TRE.sql_meta(test_conn, sql, 1, "POSTGRESQL")

            @test variables isa Vector
            @test length(variables) == 3

            var_names = [v.name for v in variables]
            @test "death_id" in var_names
            @test "site_id" in var_names
            @test "cause" in var_names

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
            variables = AHRI_TRE.sql_meta(test_conn, sql, 2, "POSTGRESQL")

            @test variables isa Vector
            @test length(variables) == 6

            var_names = [v.name for v in variables]
            @test "death_id" in var_names
            @test "cause_label" in var_names
            @test "cause_description" in var_names
            @test "source_id" in var_names
        end

        @testset "AHRI_TRE.sql_meta Type Detection" begin
            sql = "SELECT code, label, description FROM causes"
            variables = AHRI_TRE.sql_meta(test_conn, sql, 3, "POSTGRESQL")

            @test variables isa Vector
            @test length(variables) == 3

            code_var = first(filter(v -> v.name == "code", variables))
            label_var = first(filter(v -> v.name == "label", variables))

            @test code_var.value_type_id == AHRI_TRE.TRE_TYPE_INTEGER
            @test label_var.value_type_id == AHRI_TRE.TRE_TYPE_STRING
        end

        @testset "AHRI_TRE.sql_meta Vocabulary Detection" begin
            sql = "SELECT death_id, site_id, cause FROM deaths"
            variables = AHRI_TRE.sql_meta(test_conn, sql, 4, "POSTGRESQL")

            @test variables isa Vector
            @test length(variables) == 3

            vars_with_vocab = filter(v -> !ismissing(v.vocabulary) && !isempty(v.vocabulary.items), variables)

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
            variables = AHRI_TRE.sql_meta(test_conn, sql, 5, "POSTGRESQL")

            @test length(variables) >= 2

            v = first(variables)
            @test hasfield(typeof(v), :variable_id)
            @test hasfield(typeof(v), :name)
            @test hasfield(typeof(v), :domain_id)
            @test hasfield(typeof(v), :value_type_id)
        end

    finally
        if !isnothing(test_conn)
            try
                DBInterface.close!(test_conn)
            catch
            end
        end

        if !isnothing(admin_conn)
            # Ensure no remaining connections before dropping.
            try
                DBInterface.execute(admin_conn, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = \$1 AND pid <> pg_backend_pid()", [test_dbname])
            catch
            end

            try
                DBInterface.execute(admin_conn, "DROP DATABASE IF EXISTS $test_dbname")
            catch e
                @warn "Failed to drop temporary PostgreSQL database '$test_dbname'" exception=(e, catch_backtrace())
            end

            try
                DBInterface.close!(admin_conn)
            catch
            end
        end
    end
end

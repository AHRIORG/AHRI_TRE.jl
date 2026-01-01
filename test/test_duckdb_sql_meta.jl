using AHRI_TRE
using Test
using DuckDB
using DBInterface
using DataFrames


"""
    setup_duckdb_test_db() -> DBInterface.Connection

Create (or re-create) the DuckDB test database under `test/` from duckdb_testenv.sql.
Returns an open connection.
"""
function setup_duckdb_test_db()
    # Create/open file-backed DuckDB database
    db, conn = AHRI_TRE.connect_duckdb(DUCKDB_TEST_DB)

    sql_text = read(DUCKDB_ENV_SQL, String)
    # Remove full-line comments
    sql_text = join(filter(line -> !startswith(strip(line), "--"), split(sql_text, '\n')), "\n")

    # Execute statements one-by-one (keeps failures localized)
    for stmt in split(sql_text, ';')
        s = strip(stmt)
        isempty(s) && continue
        DBInterface.execute(conn, s)
    end

    return conn
end

nothing

"""Fetch a DuckDB result into a column NamedTuple without triggering deprecated result chunk APIs."""
function duckdb_query_columntable(conn, sql::AbstractString, params=nothing)
    q = isnothing(params) ? DBInterface.execute(conn, sql) : DBInterface.execute(conn, sql, params)
    column_count = DuckDB.duckdb_column_count(q.handle)
    chunks = DuckDB.DataChunk[]
    while true
        chunk_handle = DuckDB.duckdb_fetch_chunk(q.handle[])
        if chunk_handle == C_NULL
            break
        end
        chunk = DuckDB.DataChunk(chunk_handle, true)
        if DuckDB.get_size(chunk) == 0
            break
        end
        push!(chunks, chunk)
    end
    return DuckDB.convert_columns(q, chunks, column_count)
end

function duckdb_scalar(conn, sql::AbstractString, col::Symbol)
    # tbl = duckdb_query_columntable(conn, sql)
    tbl = DBInterface.execute(conn, sql) |> DataFrame
    return tbl[1, col]
end

"""
    check_test_tables_exist(conn) -> Bool

Check if the test tables exist in DuckDB.
"""
function check_duckdb_test_tables_exist(conn)
    cnt = duckdb_scalar(conn, """
        SELECT COUNT(*)::INTEGER as cnt
        FROM duckdb_tables()
        WHERE table_name IN ('causes', 'deaths', 'sites', 'sources', 'cause_categories')
    """, :cnt)
    return cnt == 5
end

@testset "AHRI_TRE.sql_meta DuckDB Tests" begin
    @test isfile(DUCKDB_ENV_SQL)

    conn = nothing
    try
        conn = setup_duckdb_test_db()
        @test conn !== nothing

        @testset "Test Environment Check" begin
            @test check_duckdb_test_tables_exist(conn)

            causes_cnt = duckdb_scalar(conn, "SELECT COUNT(*) as cnt FROM causes", :cnt)
            @test causes_cnt >= 2

            deaths_cnt = duckdb_scalar(conn, "SELECT COUNT(*) as cnt FROM deaths", :cnt)
            @test deaths_cnt >= 1
        end

        @testset "AHRI_TRE.sql_meta Basic Query" begin
            sql = "SELECT death_id, site_id, cause FROM deaths"
            variables = AHRI_TRE.sql_meta(conn, sql, 1, "DUCKDB")

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
            variables = AHRI_TRE.sql_meta(conn, sql, 2, "DUCKDB")

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
            variables = AHRI_TRE.sql_meta(conn, sql, 3, "DUCKDB")

            @test variables isa Vector
            @test length(variables) == 3

            code_var = first(filter(v -> v.name == "code", variables))
            label_var = first(filter(v -> v.name == "label", variables))

            @test code_var.value_type_id == AHRI_TRE.TRE_TYPE_INTEGER
            @test label_var.value_type_id == AHRI_TRE.TRE_TYPE_STRING
        end

        @testset "AHRI_TRE.sql_meta Vocabulary Detection" begin
            sql = "SELECT death_id, site_id, cause, cause_category FROM deaths"
            variables = AHRI_TRE.sql_meta(conn, sql, 4, "DUCKDB")

            @test variables isa Vector
            @test length(variables) == 4

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

            cause_category_var = first(filter(v -> v.name == "cause_category", variables))
            @test cause_category_var !== nothing
            @test !ismissing(cause_category_var.vocabulary)
            @test cause_category_var.value_type_id == AHRI_TRE.TRE_TYPE_CATEGORY
            @test length(cause_category_var.vocabulary.items) >= 2
            @test all(i -> i isa AHRI_TRE.VocabularyItem, cause_category_var.vocabulary.items)
            cat_codes = [i.code for i in cause_category_var.vocabulary.items]
            cat_values = [i.value for i in cause_category_var.vocabulary.items]
            @test "Medical" in cat_codes
            @test "External" in cat_codes
            @test 1 in cat_values
            @test 2 in cat_values
        end

        @testset "AHRI_TRE.sql_meta Variable Structure" begin
            sql = "SELECT code, label FROM causes"
            variables = AHRI_TRE.sql_meta(conn, sql, 5, "DUCKDB")

            @test length(variables) >= 2

            v = first(variables)
            @test hasfield(typeof(v), :variable_id)
            @test hasfield(typeof(v), :name)
            @test hasfield(typeof(v), :domain_id)
            @test hasfield(typeof(v), :value_type_id)
        end

    finally
        if !isnothing(conn)
            DBInterface.close!(conn)
        end
    end
end

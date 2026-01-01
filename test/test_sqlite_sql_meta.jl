using AHRI_TRE
using Test
using SQLite
using DBInterface
using DataFrames

const TEST_DIR = dirname(@__FILE__)
const SQLITE_ENV_SQL = joinpath(TEST_DIR, "sqlite_testenv.sql")
const SQLITE_TEST_DB = joinpath(TEST_DIR, "sqlite_test.db")

"""
    setup_sqlite_test_db() -> SQLite.DB

Create (or re-create) the SQLite test database under `test/` from sqlite_testenv.sql.
Returns an open SQLite.DB handle.
"""
function setup_sqlite_test_db()
    db = SQLite.DB(SQLITE_TEST_DB)

    sql_text = read(SQLITE_ENV_SQL, String)
    # Remove full-line comments
    sql_text = join(filter(line -> !startswith(strip(line), "--"), split(sql_text, '\n')), "\n")

    for stmt in split(sql_text, ';')
        s = strip(stmt)
        isempty(s) && continue
        DBInterface.execute(db, s)
    end

    return db
end

"""
    check_sqlite_test_tables_exist(db) -> Bool

Check if the test tables exist in SQLite.
"""
function check_sqlite_test_tables_exist(db)
    result = DBInterface.execute(db, """
        SELECT COUNT(*) as cnt
        FROM sqlite_master
        WHERE type='table' AND name IN ('causes', 'deaths', 'sites', 'sources', 'cause_categories')
    """) |> DataFrame
    return result[1, :cnt] == 5
end

@testset "AHRI_TRE.sql_meta SQLite Tests" begin
    @test isfile(SQLITE_ENV_SQL)

    db = nothing
    try
        db = setup_sqlite_test_db()
        @test db !== nothing

        @testset "Test Environment Check" begin
            @test check_sqlite_test_tables_exist(db)

            causes_count = DBInterface.execute(db, "SELECT COUNT(*) as cnt FROM causes") |> DataFrame
            @test causes_count[1, :cnt] >= 2

            deaths_count = DBInterface.execute(db, "SELECT COUNT(*) as cnt FROM deaths") |> DataFrame
            @test deaths_count[1, :cnt] >= 1
        end

        @testset "AHRI_TRE.sql_meta Basic Query" begin
            sql = "SELECT death_id, site_id, cause FROM deaths"
            variables = AHRI_TRE.sql_meta(db, sql, 1, "SQLITE")

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
            variables = AHRI_TRE.sql_meta(db, sql, 2, "SQLITE")

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
            variables = AHRI_TRE.sql_meta(db, sql, 3, "SQLITE")

            @test variables isa Vector
            @test length(variables) == 3

            code_var = first(filter(v -> v.name == "code", variables))
            label_var = first(filter(v -> v.name == "label", variables))

            @test code_var.value_type_id == AHRI_TRE.TRE_TYPE_INTEGER
            @test label_var.value_type_id == AHRI_TRE.TRE_TYPE_STRING
        end

        @testset "AHRI_TRE.sql_meta Vocabulary Detection" begin
            sql = "SELECT death_id, site_id, cause FROM deaths"
            variables = AHRI_TRE.sql_meta(db, sql, 4, "SQLITE")

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
            variables = AHRI_TRE.sql_meta(db, sql, 5, "SQLITE")

            @test length(variables) >= 2

            v = first(variables)
            @test hasfield(typeof(v), :variable_id)
            @test hasfield(typeof(v), :name)
            @test hasfield(typeof(v), :domain_id)
            @test hasfield(typeof(v), :value_type_id)
        end

    finally
        if !isnothing(db)
            try
                DBInterface.close!(db)
            catch
                try
                    SQLite.close(db)
                catch
                end
            end
        end
    end
end

using AHRI_TRE
using Test
using ConfigEnv
using DBInterface
using DataFrames
using Random
using DuckDB
using SQLite
using LibPQ
using ODBC

function gather_env(keys::Vector{String})
    values = Dict{String,String}()
    missing = String[]
    for key in keys
        value = get(ENV, key, "")
        if isempty(value)
            push!(missing, key)
        else
            values[key] = value
        end
    end
    return values, missing
end

function open_tre_store()
    keys = [
        "TRE_SERVER",
        "TRE_USER",
        "TRE_PWD",
        "TRE_DBNAME",
        "TRE_LAKE_PATH",
        "LAKE_USER",
        "LAKE_PASSWORD"
    ]
    values, missing = gather_env(keys)
    if !isempty(missing)
        return nothing, missing
    end
    store = AHRI_TRE.DataStore(
        server=values["TRE_SERVER"],
        user=values["TRE_USER"],
        password=values["TRE_PWD"],
        dbname=values["TRE_DBNAME"],
        lake_data=values["TRE_LAKE_PATH"],
        lake_user=values["LAKE_USER"],
        lake_password=values["LAKE_PASSWORD"],
        lake_db=get(ENV, "TRE_LAKE_DB", "ducklake_catalog")
    )
    try
        return AHRI_TRE.opendatastore(store), String[]
    catch e
        @warn "Unable to open TRE datastore" exception = (e, catch_backtrace())
        return nothing, ["TRE datastore connection failed"]
    end
end

function ensure_test_study_and_domain!(store::AHRI_TRE.DataStore)
    study = AHRI_TRE.get_study(store, "Test")
    if isnothing(study)
        study = AHRI_TRE.Study(
            name="Test",
            description="Test study",
            external_id="test",
            study_type_id=AHRI_TRE.TRE_STUDY_TYPE_SURVEY
        )
        study = AHRI_TRE.upsert_study!(store, study)
    end
    domain = AHRI_TRE.get_domain(store, "Test")
    if isnothing(domain)
        domain = AHRI_TRE.Domain(name="Test", description="Test domain")
        domain = AHRI_TRE.upsert_domain!(store, domain)
    end
    AHRI_TRE.add_study_domain!(store, study, domain)
    return study, domain
end

function load_sql_statements_sql2ds(path::AbstractString)
    sql_text = read(path, String)
    lines = split(sql_text, '\n')
    filtered = filter(line -> !startswith(strip(line), "--"), lines)
    cleaned = join(filtered, "\n")
    statements = String[]
    for chunk in split(cleaned, ';')
        stmt = strip(chunk)
        isempty(stmt) && continue
        push!(statements, stmt)
    end
    return statements
end

function exec_sql_file_sql2ds(conn, path::AbstractString)
    for stmt in load_sql_statements_sql2ds(path)
        DBInterface.execute(conn, stmt)
    end
end

random_dataset_name(flavour::AbstractString) =
    "cause_counts_$(lowercase(flavour))_" * "test"

function normalize_counts(df::DataFrame)
    df2 = DataFrame(df)
    rename!(df2, Dict(name => Symbol(lowercase(String(name))) for name in names(df2)))
    select!(df2, [:cause, :n])
    df2[!, :cause] = normalize_int_column(df2.cause)
    df2[!, :n] = normalize_int_column(df2.n)
    sort!(df2, [:cause])
    return df2
end

function normalize_int_column(col)
    if eltype(col) <: Integer
        return Int.(col)
    elseif eltype(col) <: Union{Integer,Missing}
        return [Int(ensure_not_missing(v)) for v in col]
    else
        return [parse(Int, string(v)) for v in col]
    end
end

ensure_not_missing(v) = ismissing(v) ? error("Encountered missing value in integer column") : v

function assert_dataset_state(store::AHRI_TRE.DataStore, dataset::AHRI_TRE.DataSet, expected::DataFrame)
    @test dataset.version !== nothing
    @test dataset.version.version_id !== nothing
    actual = normalize_counts(AHRI_TRE.dataset_to_dataframe(store, dataset))
    @test actual == expected

    version_id = dataset.version.version_id
    stmt_version = DBInterface.prepare(
        store.store,
        raw"""
    SELECT version_id
      FROM asset_versions
     WHERE version_id = $1
"""
    )
    df_version = DBInterface.execute(stmt_version, (version_id,)) |> DataFrame
    @test nrow(df_version) == 1

    stmt_transformation = DBInterface.prepare(
        store.store,
        raw"""
    SELECT t.transformation_type
      FROM transformations t
      JOIN transformation_outputs o ON t.transformation_id = o.transformation_id
     WHERE o.version_id = $1
"""
    )
    df_transformation = DBInterface.execute(stmt_transformation, (version_id,)) |> DataFrame
    @test nrow(df_transformation) == 1
    @test df_transformation[1, :transformation_type] == "ingest"
end

function ingest_and_validate(store, study, domain, conn, flavour::String, description::String)
    expected = normalize_counts(DBInterface.execute(conn, SQL_QUERY) |> DataFrame)
    dataset_name = random_dataset_name(flavour)
    dataset = AHRI_TRE.sql_to_dataset(store, study, domain, dataset_name, conn, flavour, SQL_QUERY;
        description=description, replace=true)
    @test dataset !== nothing
    dataset === nothing && return
    assert_dataset_state(store, dataset, expected)
end

function setup_duckdb_source()
    if !isfile(DUCKDB_ENV_SQL)
        return nothing, "duckdb_testenv.sql not found"
    end
    db_path = joinpath(TEST_DIR, "duckdb_sql_to_dataset.db")
    if isfile(db_path)
        rm(db_path)
    end
    db, conn = AHRI_TRE.connect_duckdb(db_path)
    try
        exec_sql_file_sql2ds(conn, DUCKDB_ENV_SQL)
        return (db=db, conn=conn), nothing
    catch e
        DBInterface.close!(conn)
        DuckDB.close(db)
        return nothing, sprint(showerror, e)
    end
end

function setup_sqlite_source()
    if !isfile(SQLITE_ENV_SQL)
        return nothing, "sqlite_testenv.sql not found"
    end
    db_path = joinpath(TEST_DIR, "sqlite_sql_to_dataset.db")
    if isfile(db_path)
        rm(db_path)
    end
    db = SQLite.DB(db_path)
    try
        exec_sql_file_sql2ds(db, SQLITE_ENV_SQL)
        return db, nothing
    catch e
        try
            DBInterface.close!(db)
        catch
            try
                SQLite.close(db)
            catch
            end
        end
        return nothing, sprint(showerror, e)
    end
end

function setup_postgres_source()
    keys = ["TRE_SERVER", "SUPER_USER", "SUPER_PWD"]
    values, missing = gather_env(keys)
    if !isempty(missing)
        return nothing, "Missing PostgreSQL admin env vars: $(join(missing, ", "))"
    end
    port = try
        parse(Int, get(ENV, "TRE_PORT", "5432"))
    catch
        5432
    end
    admin_conn = DBInterface.connect(LibPQ.Connection,
        "host=$(values["TRE_SERVER"]) port=$(port) dbname=postgres user=$(values["SUPER_USER"]) password=$(values["SUPER_PWD"])"
    )
    dbname = "sql_to_dataset_pg_" * lowercase(randstring(8))
    try
        DBInterface.execute(admin_conn, "CREATE DATABASE $(dbname)")
    catch e
        DBInterface.close!(admin_conn)
        return nothing, sprint(showerror, e)
    end
    conn = DBInterface.connect(LibPQ.Connection,
        "host=$(values["TRE_SERVER"]) port=$(port) dbname=$(dbname) user=$(values["SUPER_USER"]) password=$(values["SUPER_PWD"])"
    )
    try
        exec_sql_file_sql2ds(conn, POSTGRES_ENV_SQL)
        return (conn=conn, admin=admin_conn, dbname=dbname), nothing
    catch e
        DBInterface.close!(conn)
        DBInterface.execute(admin_conn, "DROP DATABASE IF EXISTS $(dbname)")
        DBInterface.close!(admin_conn)
        return nothing, sprint(showerror, e)
    end
end

function teardown_postgres_source(handles)
    conn = handles.conn
    admin = handles.admin
    dbname = handles.dbname
    try
        DBInterface.close!(conn)
    catch
    end
    try
        DBInterface.execute(admin, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$(dbname)' AND pid <> pg_backend_pid();")
    catch
    end
    try
        DBInterface.execute(admin, "DROP DATABASE IF EXISTS $(dbname);")
    catch
    end
    try
        DBInterface.close!(admin)
    catch
    end
end

function setup_mssql_source()
    keys = ["MSQLServer", "MSQLServerDB", "MSQLServerUser", "MSQLServerPW"]
    values, missing = gather_env(keys)
    if !isempty(missing)
        return nothing, "Missing MSSQL env vars: $(join(missing, ", "))"
    end
    conn = AHRI_TRE.connect_mssql(values["MSQLServer"], values["MSQLServerDB"], values["MSQLServerUser"], values["MSQLServerPW"])
    if isnothing(conn)
        return nothing, "Unable to connect to MSSQL"
    end
    if !check_mssql_tables_exist(conn)
        DBInterface.close!(conn)
        return nothing, "MSSQL test tables missing"
    end
    return conn, nothing
end

function check_mssql_tables_exist(conn)
    df = DBInterface.execute(
        conn,
        """
    SELECT COUNT(*) as cnt
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME IN ('causes','deaths','sites','sources')
"""
    ) |> DataFrame
    return df[1, :cnt] == 4
end

@testset "sql_to_dataset" begin
    store, missing = open_tre_store()
    study, domain = ensure_test_study_and_domain!(store)

    try
        @testset "DuckDB source" begin
            handles, err = setup_duckdb_source()
            try
                @info "Running DuckDB sql_to_dataset test"
                ingest_and_validate(store, study, domain, handles.conn, "DUCKDB", "DuckDB cause counts")
            finally
                DBInterface.close!(handles.conn)
                DuckDB.close(handles.db)
            end
        end

        @testset "SQLite source" begin
            db, err = setup_sqlite_source()
            try
                @info "Running SQLite sql_to_dataset test"
                ingest_and_validate(store, study, domain, db, "SQLITE", "SQLite cause counts")
            finally
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

        @testset "PostgreSQL source" begin
            handles, err = setup_postgres_source()
            try
                @info "Running PostgreSQL sql_to_dataset test"
                ingest_and_validate(store, study, domain, handles.conn, "POSTGRESQL", "PostgreSQL cause counts")
            finally
                teardown_postgres_source(handles)
            end
        end

        conn, err = setup_mssql_source()
        if isnothing(conn)
            @error "MSSQL source setup failed: $err"
            @test false
        end
        @testset "MSSQL source" begin
            try
                @info "Running MSSQL sql_to_dataset test"
                ingest_and_validate(store, study, domain, conn, "MSSQL", "MSSQL cause counts")
            finally
                try
                    DBInterface.close!(conn)
                catch
                end
            end
        end
    finally
        AHRI_TRE.closedatastore(store)
    end
end

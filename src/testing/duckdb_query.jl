using AHRI_TRE
using ConfigEnv
using DataFrames
using DBInterface
using Tables
using DuckDB

function duckdb_scalar(conn, sql::AbstractString, col::Symbol)
    # tbl = duckdb_query_columntable(conn, sql)
    @info "About to execute SQL: $sql"
    tbl = DBInterface.execute(conn, sql) |> DataFrame
    @info "Executed SQL: $sql"
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
        WHERE table_name IN ('causes', 'deaths', 'sites', 'sources')
    """, :cnt)
    return cnt == 4
end

#get environment variables
# dotenv()
# const DUCKDB_TEST_DB = "test/duckdb_test.db"
# db, conn = AHRI_TRE.connect_duckdb(DUCKDB_TEST_DB)
# result = check_duckdb_test_tables_exist(conn)
# println("DuckDB test tables exist: ", result)
db = DuckDB.DB()

df = DuckDB.query(db, "SELECT version() AS version;") |> DataFrame
show(df)
DuckDB.close(db)
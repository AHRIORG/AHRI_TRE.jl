using DuckDB
using DBInterface
using SQLite
using DataFrames

db = DuckDB.DB()
conn = DBInterface.connect(db)
try
  global df = DuckDB.query(db,"SELECT version() as duckdb_version;") |> DataFrame
  show(df)
finally
    DBInterface.close(conn)
    DuckDB.close(db)
end
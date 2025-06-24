using DuckDB
using DBInterface
using SQLite
using DataFrames

db = DuckDB.DB()
conn = DBInterface.connect(db)
try
  df = DuckDB.query(db,"SELECT version() as duckdb_version;") |> DataFrame
  show(df)
  println()
  DBInterface.execute(conn, "LOAD 'ducklake';")
  # DBInterface.execute(conn, "ATTACH 'ducklake:sqlite:/home/kherbst/repos/RDALake.jl/database/metadata.sqlite' AS my_ducklake")
  # DBInterface.execute(conn, "CREATE TABLE my_ducklake.demo (i INTEGER);")
  # DBInterface.execute(conn, "INSERT INTO my_ducklake.demo VALUES (42), (43);")
  # df = DuckDB.query(conn, "FROM my_ducklake.demo;")
  show(df)
finally
  DBInterface.close(conn)
  DuckDB.close(db)
end
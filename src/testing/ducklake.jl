using DuckDB
using DBInterface
using LibPQ
using DataFrames

db = DuckDB.DB()
conn = DBInterface.connect(db)
try
  ver = DuckDB.query(db, "PRAGMA version;") |> DataFrame
  plat = DuckDB.query(db, "PRAGMA platform;") |> DataFrame
  show(ver)
  println()
  show(plat)
  println()
  DuckDB.query(db, "LOAD postgres;")
  println("Postgres extension loaded.")
  DuckDB.query(db, "INSTALL ducklake; LOAD ducklake;")
  println("DuckDB extensions loaded.")
  df = DuckDB.query(db, "UPDATE EXTENSIONS;") |> DataFrame
  show(df)
  DBInterface.execute(conn, "ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost user=ducklake_user password=Nzy-f6y@brNF_6AFaC2MrZAU' AS my_ducklake (DATA_PATH 'C:\\data\\datalake');")
  DBInterface.execute(conn, "USE my_ducklake;")
  # DBInterface.execute(conn, "CREATE TABLE my_ducklake.demo (i INTEGER);")
  # DBInterface.execute(conn, "INSERT INTO my_ducklake.demo VALUES (42), (43);")
  df = DuckDB.query(conn, "FROM my_ducklake.demo;") |> DataFrame
  # df = DuckDB.query(conn, "FROM my_ducklake.nl_train_stations;") |> DataFrame
  # df = DuckDB.query(conn, "SELECT name_long FROM nl_train_stations WHERE code = 'ASB';") |> DataFrame
  # df = DuckDB.query(conn, "SELECT name_long FROM nl_train_stations AT (VERSION => 1) WHERE code = 'ASB';") |> DataFrame
  show(df)
  println()
finally
  DBInterface.close(conn)
  DuckDB.close(db)
end
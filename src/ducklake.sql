INSTALL ducklake;
INSTALL mysql;

-- Make sure that the database `ducklake_catalog` exists in MySQL
ATTACH 'ducklake:mysql:host=localhost port=3306 user=ducklake_user password=Nzy-f6y@brNF_6AFaC2MrZAU database=ducklake_catalog' AS my_ducklake (DATA_PATH '/data/datalake');
USE my_ducklake;
CREATE TABLE my_ducklake.nl_train_stations AS FROM 'https://blobs.duckdb.org/nl_stations.csv';
ducklake:mysql:db=x6_catalog user=root
ATTACH 'ducklake:sqlite:/home/kherbst/repos/RDALake.jl/database/metadata.sqlite' AS my_ducklake (DATA_PATH '/data/datalake');
# path in startup.jl
ENV["JULIA_DUCKDB_LIBRARY"] = "/full/path/to/libduckdb.so"
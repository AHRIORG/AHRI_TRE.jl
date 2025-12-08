using DuckDB
using Pkg

# Method 1: Find the DuckDB package directory
duckdb_pkg_dir = pkgdir(DuckDB)
println("DuckDB package directory: ", duckdb_pkg_dir)

# Method 2: Look for the artifacts directory
artifacts_dir = joinpath(duckdb_pkg_dir, "deps", "usr", "lib")
if isdir(artifacts_dir)
    println("Artifacts lib directory: ", artifacts_dir)
    # List library files
    lib_files = filter(f -> occursin("duckdb", f), readdir(artifacts_dir))
    println("DuckDB library files: ", lib_files)
end
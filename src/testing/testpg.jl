using LibPQ, Tables, DataFrames

conn = LibPQ.Connection("dbname=postgres user=ducklake_user password = Nzy-f6y@brNF_6AFaC2MrZAU") # omit username for now
result = execute(conn, "SELECT typname FROM pg_type WHERE oid = 16")
data = DataFrame(result)
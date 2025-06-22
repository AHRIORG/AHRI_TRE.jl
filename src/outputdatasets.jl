using ConfigEnv
using RDAIngest
using DBInterface
using Logging
#get environment variables
dotenv()

#region Setup Logging
l = open("log.log", "a+")
io = IOContext(l, :displaysize => (100, 100))
logger = SimpleLogger(io)
old_logger = global_logger(logger)
@info "Output dataset execution started $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
flush(io)
#endregion

t = now()
@info "============================== Using sqlite database: $(ENV["RDA_DATABASE_PATH"])"

db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")
try
    @time dataset_to_csv(db, 1, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 2, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 3, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 4, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 5, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 6, ENV["DATA_INGEST_PATH"])
finally
    DBInterface.close!(db)
end
d = now() - t
@info "===== Outputting datasets from sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)

println("SQL Server")
t = now()
@info "============================== Using SQL Server database: $(ENV["RDA_SERVER"])"
db = opendatabase(ENV["RDA_SERVER"], "RDA", sqlite=false)
try
    @time dataset_to_csv(db, 1, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 2, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 3, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 4, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 5, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 6, ENV["DATA_INGEST_PATH"])
finally
    DBInterface.close!(db)
end
d = now() - t
@info "===== Outputting datasets from SQL Server completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)
#region clean up
global_logger(old_logger)
close(io)
#endregion

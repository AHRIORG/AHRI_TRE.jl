using ConfigEnv
using AHRI_TRE
using DBInterface
using Logging, LoggingExtras
using Dates
#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/output.log", "w")) do io, args
    # Write the module, level and message only
    println(io, args._module, " | ", "[", args.level, "] ", args.message)
end
minlogger = MinLevelLogger(logger, Logging.Info)
old_logger = global_logger(minlogger)
#endregion

t = now()
@info "============================== Using sqlite database: $(ENV["RDA_DATABASE_PATH"])"

db, lake = opendatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], true, ENV["RDA_LAKE_PATH"], ENV["RDA_LAKE_DB"])
try
    @time dataset_to_csv(db, 1, ENV["DATA_INGEST_PATH"], false, lake)
    @time dataset_to_csv(db, 2, ENV["DATA_INGEST_PATH"], false, lake)
    @time dataset_to_csv(db, 3, ENV["DATA_INGEST_PATH"], false, lake)
    @time dataset_to_csv(db, 4, ENV["DATA_INGEST_PATH"], false, lake)
    @time dataset_to_csv(db, 5, ENV["DATA_INGEST_PATH"], false, lake)
    @time dataset_to_csv(db, 6, ENV["DATA_INGEST_PATH"], false, lake)
    elapsed = now() - t
    @info "===== Outputting datasets from sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(canonicalize(Dates.CompoundPeriod(elapsed)))"
finally
    DBInterface.close!(db)
    if !isnothing(lake)
        DBInterface.close!(lake)
    end
    global_logger(old_logger)
end

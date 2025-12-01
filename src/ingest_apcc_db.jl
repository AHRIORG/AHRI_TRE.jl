using AHRI_TRE
using ConfigEnv
using Logging, LoggingExtras

using DBInterface
using DataFrames
using Dates


#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/ingest_file.log", "w")) do io, args
    # Write the module, level and message only
    println(io, args._module, " | ", "[", args.level, "] ", args.message)
end
minlogger = MinLevelLogger(logger, Logging.Info)
old_logger = global_logger(minlogger)

start_time = Dates.now()

datastore = AHRI_TRE.DataStore(
    server=ENV["TRE_SERVER"],
    user=ENV["TRE_USER"],
    password=ENV["TRE_PWD"],
    dbname=ENV["TRE_DBNAME"],
    lake_password=ENV["LAKE_PASSWORD"],
    lake_user=ENV["LAKE_USER"],
    lake_data=ENV["TRE_LAKE_PATH"]
)
@info "Execution started at: ", Dates.now()
datastore = AHRI_TRE.opendatastore(datastore)
try
    # Retrieve APCC study
    study = AHRI_TRE.get_study(datastore, "APCC")
    if study === nothing
        error("Study 'APCC' not found in the datastore. Please create it first.")
    end
    @info "Retrieved study: $(study.name) with ID $(study.study_id)"
    file_path = expanduser("~/data/apcc.duckdb")
    if !isfile(file_path)
        error("Data file '$file_path' not found. Please ensure the file exists.")
    end
    @info "Ingesting APCC data from '$file_path' into the datastore"
    datafile = AHRI_TRE.ingest_file(datastore, study, "apccdb", file_path, "duckdb"; description="APCC cohort database", new_version=true)
    if isnothing(datafile)
        @info "Failed to ingest the APCC data file."
    else
        @info "APCC data '$(datafile.version.asset.name)' ingested successfully into '$(AHRI_TRE.file_uri_to_path(datafile.storage_uri))'"
    end
finally
    closedatastore(datastore)
    elapsed = now() - start_time
    @info "===== Completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(canonicalize(Dates.CompoundPeriod(elapsed)))"
    global_logger(old_logger)  # Restore the old logger
end

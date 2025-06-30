using RDALake
using ConfigEnv
using Logging, LoggingExtras

using DBInterface
using DataFrames
using Dates
using CSV
using SQLite


#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/log.log", "w")) do io, args
    # Write the module, level and message only
    println(io, args._module, " | ", "[", args.level, "] ", args.message)
end
minlogger = MinLevelLogger(logger, Logging.Info)
old_logger = global_logger(minlogger)

@info "Execution started $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
@info "Environmental variables"
@info "RDA_DATABASE_PATH = $(ENV["RDA_DATABASE_PATH"])"
@info "RDA_DBNAME = $(ENV["RDA_DBNAME"])"
@info "DATA_INGEST_PATH = $(ENV["DATA_INGEST_PATH"])"
@info "DATA_DICTIONARY_PATH = $(ENV["DATA_DICTIONARY_PATH"])"
@info "ISO3_PATH = $(ENV["ISO3_PATH"])"
@info "RDA_LAKE_PATH = $(ENV["RDA_LAKE_PATH"])"
@info "RDA_LAKE_DB = $(ENV["RDA_LAKE_DB"])"
#endregion

#"""
#CREATE RDA FROM SCRATCH
#"""
const creatdb = true
const ingestCHAMPS = true
const ingestCOMSAMZ = true
if creatdb
    createdatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], replace=true, sqlite=true)
    @info "===== Creating RDA database completed $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
end
t = now()

#"""
#INGEST CHAMPS DATA
#"""
if ingestCHAMPS
    @info "Ingesting CHAMPS data"

    source = CHAMPSSource()
    ingest = CHAMPSIngest()

    @info "Ingesting CHAMPS source"
    ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], ENV["ISO3_PATH"])

    @info "Ingesting CHAMPS dictionaries"
    ingest_dictionary(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"])

    @info "Ingesting CHAMPS deaths"
    ingestion_id = ingest_deaths(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"])

    @info "Ingesting CHAMPS datasets"
    ingest_data(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=true, lake_data=ENV["RDA_LAKE_PATH"], lake_db=ENV["RDA_LAKE_DB"])

    elapsed = now() - t
    @info "===== Ingesting CHAMPS into sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(canonicalize(Dates.CompoundPeriod(elapsed)))"
end

t = now()
if ingestCOMSAMZ
    #"""
    #INGEST COMSA MZ DATA
    #"""

    @info "Ingesting COMSA MZ data"

    source = COMSAMZSource()
    ingest = COMSAMZIngest()

    @info "Ingesting COMSA MZ source"
    ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], ENV["ISO3_PATH"])

    @info "Ingesting COMSA MZ dictionaries"
    ingest_dictionary(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"])

    @info "Ingesting COMSA MZ deaths"
    ingestion_id = ingest_deaths(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"])

    @info "Ingesting COMSA MZ datasets"
    ingest_data(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=true, lake_data=ENV["RDA_LAKE_PATH"], lake_db=ENV["RDA_LAKE_DB"])

    elapsed = now() - t
    @info "===== Ingesting COMSA MZ into sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(canonicalize(Dates.CompoundPeriod(elapsed)))"
end
global_logger(old_logger)

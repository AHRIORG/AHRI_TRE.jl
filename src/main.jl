# Set working directory
cd("/Users/chu.282/Dropbox/OSU/RDA_private/RDAORG/RDAIngest.jl")
using Pkg
#Pkg.develop(PackageSpec(path="/Users/chu.282/Dropbox/OSU/RDA_private/RDAORG/RDAIngest.jl"))

using RDAIngest
using ConfigEnv
using Logging

using DBInterface
using DataFrames
using Dates
using CSV
using SQLite


#get environment variables
dotenv()
#region Setup Logging
l = open("log.log", "a+")
io = IOContext(l, :displaysize => (100, 100))
logger = SimpleLogger(io)
old_logger = global_logger(logger)
@info "Execution started $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
flush(io)
#endregion

#"""
#CREATE RDA FROM SCRATCH
#"""

@time createdatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], replace=true, sqlite=true)
db = opendatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"]; sqlite=true)

t = now()

#"""
#INGEST CHAMPS DATA
#"""

@info "Ingesting CHAMPS data"

source = CHAMPSSource()
ingest = CHAMPSIngest()

@info "Ingesting CHAMPS source"
flush(io)
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], ENV["ISO3_PATH"], sqlite=true)

@info "Ingesting CHAMPS dictionaries"
flush(io)
@time ingest_dictionary(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"], sqlite=true)

@info "Ingesting CHAMPS deaths"
flush(io)
@time ingestion_id = ingest_deaths(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)

@info "Ingesting CHAMPS datasets"
flush(io)
@time ingest_data(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=true)

d = now() - t
@info "===== Ingesting CHAMPS into sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)


t = now()

#"""
#INGEST COMSA MZ DATA
#"""

@info "Ingesting COMSA MZ data"

source = COMSAMZSource()
ingest = COMSAMZIngest()

@info "Ingesting COMSA MZ source"
flush(io)
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], ENV["ISO3_PATH"], sqlite=true)

@info "Ingesting COMSA MZ dictionaries"
flush(io)
@time ingest_dictionary(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"], sqlite=true)

@info "Ingesting COMSA MZ deaths"
flush(io)
@time ingestion_id = ingest_deaths(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)

@info "Ingesting COMSA MZ datasets"
flush(io)
@time ingest_data(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=true)

d = now() - t
@info "===== Ingesting COMSA MZ into sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)


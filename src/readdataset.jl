using ConfigEnv
using RDALake
using DBInterface
using Logging, LoggingExtras
#get environment variables
dotenv()

db, lake = opendatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], true, ENV["RDA_LAKE_PATH"], ENV["RDA_LAKE_DB"])
try
    global df = dataset_to_dataframe(db, 1, lake)
finally
    DBInterface.close!(db)
    if !isnothing(lake)
        DBInterface.close!(lake)
    end
end
using ConfigEnv
using AHRI_TRE
using DBInterface
using Logging, LoggingExtras
#get environment variables
dotenv()

db, lake = opendatabase(ENV["TRE_DATABASE_PATH"], ENV["TRE_DBNAME"], true, ENV["TRE_LAKE_PATH"], ENV["TRE_LAKE_DB"])
try
    global df = dataset_to_dataframe(db, 1, lake)
finally
    DBInterface.close!(db)
    if !isnothing(lake)
        DBInterface.close!(lake)
    end
end
using AHRI_TRE
using ConfigEnv
using Logging, LoggingExtras

using DBInterface
using DataFrames
using Dates


#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/new_test_store.log", "w")) do io, args
    # Write the module, level and message only
    println(io, args._module, " | ", "[", args.level, "] ", args.message)
end
minlogger = MinLevelLogger(logger, Logging.Info)
old_logger = global_logger(minlogger)

start_time = Dates.now()
# Test Datastore
datastore = AHRI_TRE.DataStore(
    server=ENV["TRE_SERVER"],
    user=ENV["TRE_USER"],
    password=ENV["TRE_PWD"],
    dbname=ENV["TRE_TEST_DBNAME"],
    lake_password=ENV["LAKE_PASSWORD"],
    lake_user=ENV["LAKE_USER"],
    lake_data=ENV["TRE_TEST_LAKE_PATH"],
    lake_db=ENV["TRE_TEST_LAKE_DB"]
)

@info "Execution started at: ", Dates.now()
AHRI_TRE.createdatastore(datastore; superuser=ENV["SUPER_USER"], superpwd=ENV["SUPER_PWD"])
@info "DataStore created or replaced at: $(datastore.server)"

elapsed = now() - start_time
@info "===== Completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(canonicalize(Dates.CompoundPeriod(elapsed)))"
global_logger(old_logger);  # Restore the old logger

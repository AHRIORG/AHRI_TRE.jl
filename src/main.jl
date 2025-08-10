using AHRI_TRE
using ConfigEnv
using Logging, LoggingExtras

using DBInterface
using DataFrames
using Dates


#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/create_store.log", "w")) do io, args
  # Write the module, level and message only
  println(io, args._module, " | ", "[", args.level, "] ", args.message)
end
minlogger = MinLevelLogger(logger, Logging.Info)
old_logger = global_logger(minlogger)

#Execution flags
do_createstore = false
do_createstudy = true
do_updatestudy = true

startime = Dates.now()
datastore = AHRI_TRE.DataStore(
  server=ENV["TRE_SERVER"],
  user=ENV["TRE_USER"],
  password=ENV["TRE_PWD"],
  dbname=ENV["TRE_DBNAME"],
  lake_password=ENV["LAKE_PASSWORD"],
  lake_user=ENV["LAKE_USER"]
)
if do_createstore
  AHRI_TRE.createdatastore(datastore; superuser=ENV["SUPER_USER"], superpwd=ENV["SUPER_PWD"])
  @info "DataStore created or replaced at: $(datastore.server)"
end
datastore = AHRI_TRE.opendatastore(datastore)
try
  println("Execution started at: ", Dates.now())
  if do_createstudy
    study = Study(
      name="APCC Update",
      description="Update APCC cohort data and contact information",
      external_id="APCC",
      study_type_id=3
    )
    study = upsert_study(study, datastore)
    @info "Study created or updated: $(study.name) with ID $(study.study_id)"
  end
  if do_updatestudy
    study = Study(
      study_id=1,  # Assuming study_id 1 exists
      name="APCC Update",
      description="Update APCC cohort data and contact information",
      external_id="APCC_Update",
      study_type_id=3
    )
    study = upsert_study(study, datastore)
    @info "Study updated: $(study.name) with ID $(study.study_id)"
  end
finally
  closedatastore(datastore)
  global_logger(old_logger)  # Restore the old logger
end
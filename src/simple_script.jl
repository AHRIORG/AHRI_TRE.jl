using AHRI_TRE
using ConfigEnv
using Logging, LoggingExtras

using DBInterface
using DataFrames
using Dates


#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/simple.log", "w")) do io, args
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
  studies = AHRI_TRE.get_studies(datastore)
  println("Studies available in the TRE:")
  for study in studies
    println("$(study.name) (ID: $(study.study_id))")
      datasets = AHRI_TRE.get_study_datasets(datastore, study)
      for dataset in datasets
        println(" - $(dataset.version.asset.name) v$(VersionNumber(dataset.version.major, dataset.version.minor, dataset.version.patch)) ")
      end
  end
  test_variables = AHRI_TRE.get_study_variables(datastore, get_study(datastore, "Test"))
  println("\nVariables in 'Test' study:")
  for variable in test_variables
    println("$(variable.name), $(variable.description), $(variable.value_type_id)")
  end
finally
  AHRI_TRE.closedatastore(datastore)
end


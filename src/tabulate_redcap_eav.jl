using AHRI_TRE
using ConfigEnv
using Logging, LoggingExtras
using Dates
using URIs
#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/tabulate.log", "w")) do io, args
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
  studies = AHRI_TRE.list_studies(datastore)
  if isempty(studies)
    @error "No studies found in the DataStore. Please create a study first."
  else
    @info "Found $(length(studies)) studies in the DataStore."
    for i in eachindex(studies)
      study = studies[i]
      @info "Study $i: '$(study.name)' ID: $(study.study_id)"
    end
    assets = AHRI_TRE.list_assets(datastore, studies[1]; include_versions=true) #Index verified from the list
    if isempty(assets)
      @error "No assets found in the study. Please create an asset first."
    else
      @info "Found $(length(assets)) assets in the study."
      for i in eachindex(assets)
        asset = assets[i]
        @info "Asset $i: '$(asset.name)' ID: $(asset.asset_id)"
      end
      version = AHRI_TRE.get_latest_version(assets[1]) #Index verified from the list
      if isnothing(version)
        @error "No versions found for the asset. Please create a version first."
      end
      datafile = AHRI_TRE.get_datafile_meta(datastore,version)
      if isnothing(datafile)
        @error "No data file found for the asset version. Please create a data file first"
      end
      @info "Data file path: '$(AHRI_TRE.file_uri_to_path(datafile.storage_uri))'" #unescapeuri(str) AHRI_TRE.file_uri_to_path
      variables = AHRI_TRE.get_eav_variables(datastore, datafile)
    end
  end
finally
  closedatastore(datastore)
  elapsed = now() - start_time
  @info "===== Completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(canonicalize(Dates.CompoundPeriod(elapsed)))"
  global_logger(old_logger)  # Restore the old logger
end

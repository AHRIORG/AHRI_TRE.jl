using AHRI_TRE
using ConfigEnv
using Logging, LoggingExtras

using DBInterface
using DataFrames
using Dates


#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/new_project.log", "w")) do io, args
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
    study = nothing
    domain = nothing
    # First create or retrieve a domain that the study can use for its data
    # Here we assume the domain does not exist, so we create it
    domain = Domain(
        name="APCC",
        uri="https://apcc.africa",
        description="African Population Cohorts Consortium"
    )
    domain = upsert_domain!(datastore, domain)
    @info "Domain inserted: $(domain.name) with ID $(domain.domain_id)"
    # Now create a study, or retrieve the study the REDCap project should be associated with
    # Here we assume the study does not exist, so we create it
    study = Study(
        name="APCC",
        description="Update APCC cohort data and contact information",
        external_id="APCC",
        study_type_id=3
    )
    study = upsert_study!(datastore, study)
    @info "Study created or updated: $(study.name) with ID $(study.study_id)"
    add_study_domain!(datastore, study, domain)
    # Now we can ingest the REDCap project data
    @info "Ingesting REDCap project data into the datastore"
    datafile = ingest_redcap_project(datastore, ENV["REDCAP_API_URL"], ENV["REDCAP_API_TOKEN"], study, domain)
    @info "REDCap project data ingested successfully into '$(AHRI_TRE.file_uri_to_path(datafile.storage_uri))'"
    # Note: The REDCap project data will be placed in a datafile as a records export in csv eav format
    # it will still need to be transformed into a dataset
    @info "Transforming REDCap project data into a dataset"
    dataset = AHRI_TRE.transform_eav_to_dataset(datastore, datafile)
    @info "Transformed EAV data to dataset $(dataset.version.asset.name)."
    # Read back the dataset as a DataFrame
    df = AHRI_TRE.read_dataset(datastore, dataset)
    @info "Dataset read back as DataFrame with $(nrow(df)) rows and $(ncol(df)) columns."
finally
    closedatastore(datastore)
    elapsed = now() - start_time
    @info "===== Completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(canonicalize(Dates.CompoundPeriod(elapsed)))"
    global_logger(old_logger)  # Restore the old logger
end

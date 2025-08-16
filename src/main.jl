using AHRI_TRE
using ConfigEnv
using Logging, LoggingExtras

using DBInterface
using DataFrames
using Dates


#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/redcap.log", "w")) do io, args
  # Write the module, level and message only
  println(io, args._module, " | ", "[", args.level, "] ", args.message)
end
minlogger = MinLevelLogger(logger, Logging.Info)
old_logger = global_logger(minlogger)

start_time = Dates.now()

#region Execution flags
do_createstore = false
do_createstudy = false
do_updatestudy = false
do_insertdomain = false
do_entities = false
do_variables = false
do_redcap = true
#endregion

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

if do_createstore
  AHRI_TRE.createdatastore(datastore; superuser=ENV["SUPER_USER"], superpwd=ENV["SUPER_PWD"])
  @info "DataStore created or replaced at: $(datastore.server)"
end
datastore = AHRI_TRE.opendatastore(datastore)
try
  study = nothing
  domain = nothing
  if !do_createstudy
    study = get_study(datastore, "APCC")
    if isnothing(study)
      @info "Study 'APCC' not found, creating a new one."
      global do_createstudy = true
    else
      @info "Found existing study: $(study.name) with ID $(study.study_id)"
    end
  else
    @info "Creating a new study."
  end
  if !do_insertdomain
    domain = get_domain(datastore, "APCC")
    if isnothing(domain)
      @info "Domain 'APCC' not found, inserting a new one."
      global do_insertdomain = true
    else
      @info "Found existing domain: $(domain.name) with ID $(domain.domain_id)"
    end
  else
    @info "Inserting a new domain."
  end
  if do_createstudy
    study = Study(
      name="APCC Update",
      description="Update APCC cohort data and contact information",
      external_id="APCC",
      study_type_id=3
    )
    study = upsert_study!(study, datastore)
    @info "Study created or updated: $(study.name) with ID $(study.study_id)"
  end
  if do_updatestudy
    study.description = "Updated description for APCC cohort"
    study = upsert_study!(study, datastore)
    @info "Study updated: $(study.name) with ID $(study.study_id)"
  end
  if do_insertdomain
    domain = Domain(
      name="APCC",
      uri="https://apcc.africa",
      description="African Population Cohorts Consortium"
    )
    domain = upsert_domain!(domain, datastore)
    @info "Domain inserted: $(domain.name) with ID $(domain.domain_id)"
  end
  if do_entities
    entity = Entity(
      name="APCC Cohort",
      description="African Population Cohorts Consortium Cohort",
      domain_id=get_domain(datastore, "APCC").domain_id,
      ontology_namespace="apcc",
      ontology_class="http://purl.obolibrary.org/obo/NCIT_C61512"
    )
    entity = upsert_entity!(entity, datastore)
    @info "Entity inserted: $(entity.name) with ID $(entity.entity_id)"
    entity2 = Entity(
      name="Person",
      description="A person that can be contacted at/represent a cohort",
      domain_id=entity.domain_id,
      ontology_namespace="foaf",
      ontology_class="http://xmlns.com/foaf/0.1/Person"
    )
    entity2 = upsert_entity!(entity2, datastore)
    @info "Entity inserted: $(entity2.name) with ID $(entity2.entity_id)"
    relation = EntityRelation(
      name="hasContact",
      description="A contact person for a cohort",
      domain_id=entity.domain_id,
      entity_id_1=entity.entity_id,
      entity_id_2=entity2.entity_id,
      ontology_namespace="apcc",
      ontology_class="http://purl.obolibrary.org/obo/RO_0000052",
    )
    relation = upsert_entityrelation!(relation, datastore)
    @info "Entity relation inserted: $(relation.name) with ID $(relation.entityrelation_id)"
    # read back the entity
    entity = get_entity(datastore, get_domain(datastore, "APCC").domain_id, "APCC Cohort")
    @info "Retrieved entity: $(entity.name) with ID $(entity.entity_id)"
    entities = list_domainentities(datastore, get_domain(datastore, "APCC").domain_id)
    entity_names = collect(skipmissing(entities.name))
    @info "Entities in domain 'APCC': $(join(entity_names, ", "))"
    relations = list_domainrelations(datastore, get_domain(datastore, "APCC").domain_id)
    relation_names = collect(skipmissing(relations.name))
    @info "Relations in domain 'APCC': $(join(relation_names, ", "))"
  end
  if do_variables
    vars_map = register_redcap_datadictionary(datastore;
      domain_id=get_domain(datastore, "APCC").domain_id,
      redcap_url=ENV["REDCAP_API_URL"],
      redcap_token=ENV["REDCAP_API_TOKEN"],
      dataset_id=nothing,                 # or dataset UUID if you want to fill dataset_variables
      forms=nothing,                      # or ["enrolment_form","visit_form"]
      vocabulary_prefix="apcc"
    )
  end
  if do_redcap
    @info "Ingesting REDCap project data into the datastore"
    ingest_redcap_project(datastore, ENV["REDCAP_API_URL"], ENV["REDCAP_API_TOKEN"], study, domain)
    @info "REDCap project data ingested successfully"
  end
finally
  closedatastore(datastore)
  elapsed = now() - start_time
  @info "===== Completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(canonicalize(Dates.CompoundPeriod(elapsed)))"
  global_logger(old_logger)  # Restore the old logger
end

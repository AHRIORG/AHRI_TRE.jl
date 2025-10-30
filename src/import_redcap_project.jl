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
    # Link the study to a domain
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
    t = list_study_transformations(datastore, study)
    @info "List of transformations for study $(study.name):"
    show(t)
    # Show assets in the study
    t = AHRI_TRE.list_assets_df(datastore, study)
    show(t)
    # Create entities
    cohort_entity = Entity(
        name="APCC Cohort",
        description="Entity representing an APCC cohort",
        domain=domain,
        ontology_namespace="http://purl.obolibrary.org/obo/",
        ontology_class="STATO_0000203"
    )
    cohort_entity = create_entity!(datastore, cohort_entity, domain)
    @info "Created entity: $(cohort_entity.name) with ID $(cohort_entity.entity_id)"
    institution_entity = Entity(
        name="Institution",
        description="An established society, corporation, foundation or other organization founded and united for a specific purpose, e.g. for health-related research; also used to refer to a building or buildings occupied or used by such organization.",
        domain=domain,
        ontology_namespace="http://purl.obolibrary.org/obo/",
        ontology_class="NCIT_C41206"
    )
    institution_entity = create_entity!(datastore, institution_entity, domain)
    @info "Created entity: $(cohort_entity.name) with ID $(cohort_entity.entity_id)"
    contact_entity = Entity(
        name="Contact",
        description="A person who acts as a point of contact for the cohort.",
        domain=domain,
        ontology_namespace="http://purl.obolibrary.org/obo/",
        ontology_class="OPMI_0000383"
    )
    contact_entity = create_entity!(datastore, contact_entity, domain)
    @info "Created entity: $(cohort_entity.name) with ID $(cohort_entity.entity_id)"
    country_entity = Entity(
        name="Country",
        description="A Geopolitical Entity that delimits a Government with effective internal and external sovereignty over the region and its population, and which is not dependent on or subject to any other power or Geopolitical Entity.",
        domain=domain,
        ontology_namespace="http://www.ontologyrepository.com/CommonCoreOntologies/",
        ontology_class="Country"
    )
    country_entity = create_entity!(datastore, country_entity, domain)
    @info "Created entity: $(cohort_entity.name) with ID $(cohort_entity.entity_id)"
    operates_relation = EntityRelation(
        subject_entity=institution_entity,
        object_entity=cohort_entity,
        domain=domain,
        name="operates",
        description="Indicates that the institution operates the cohort.",
        ontology_namespace="http://purl.obolibrary.org/obo/",
        ontology_class="SLSO_0000001"
    )
    operates_relation = create_entity_relation!(datastore, operates_relation, domain)
    @info "Created entity relation: $(operates_relation.name) with ID $(operates_relation.entityrelation_id)"
    located_in_relation = EntityRelation(
        subject_entity=cohort_entity,
        object_entity=country_entity,
        domain=domain,
        name="located_in",
        description="A relation between two independent continuants, the target and the location, in which the target is entirely within the location.",
        ontology_namespace="http://purl.obolibrary.org/obo/",
        ontology_class="CLAO_0001460"
    )
    located_in_relation = create_entity_relation!(datastore, located_in_relation, domain)
    @info "Created entity relation: $(located_in_relation.name) with ID $(located_in_relation.entityrelation_id)"
    contact_relation = EntityRelation(
        subject_entity=cohort_entity,
        object_entity=contact_entity,
        domain=domain,
        name="hasContact",
        description="Indicates that the cohort has this contact person.",
        ontology_namespace="http://purl.obolibrary.org/obo/MF#",
        ontology_class="hasAgent")
    contact_relation = create_entity_relation!(datastore, contact_relation, domain)
    @info "Created entity relation: $(contact_relation.name) with ID $(contact_relation.entityrelation_id)"
finally
    closedatastore(datastore)
    elapsed = now() - start_time
    @info "===== Completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(canonicalize(Dates.CompoundPeriod(elapsed)))"
    global_logger(old_logger)  # Restore the old logger
end

module AHRI_TRE

using DataFrames
using LibPQ
using DBInterface
using DuckDB
using ConfigEnv
using Dates
using Arrow
using DataStructures
using SHA
using CSV
using FileIO
using Base64
using URIs
using UUIDs
using HTTP
using JSON3
using Downloads
using StringEncodings
using OrderedCollections
using TranscodingStreams
using CodecZstd
using Git

export
    DataStore, Vocabulary, VocabularyItem, AbstractStudy, Study, Domain, Entity, EntityRelation,
    AbstractAsset, Asset, AbstractAssetVersion, AssetVersion, DataFile, Transformation,
    createdatastore, opendatastore, closedatastore,
    upsert_study!, upsert_domain!, get_domain, get_study, list_studies, add_study_domain!, create_study!,
    upsert_entity!, get_entity, upsert_entityrelation!, get_entityrelation, list_domainentities, list_domainrelations,
    git_commit_info,
    lookup_variables, get_eav_variables,
    get_namedkey, get_variable_id, get_variable, get_datasetname, updatevalues, insertdata, insertwithidentity,
    get_table, selectdataframe, prepareselectstatement, dataset_to_dataframe, dataset_to_arrow, dataset_to_csv,
    dataset_variables, dataset_column, savedataframe, read_dataset,
    ingest_redcap_project, register_redcap_datadictionary, transform_eav_to_dataset, list_study_transformations,
    create_entity!, create_entity_relation!

#region Structure
Base.@kwdef mutable struct DataStore
    server::String = "localhost"
    user::String = "postgres"
    password::String = ""
    dbname::String = "AHRI_TRE"
    lake_data::String = "/data/datalake" # Path to DuckDB data lake
    lake_db::String = "ducklake_catalog" # Database name for ducklake metadata database
    lake_user::String = "ducklake_user" # User for DuckDB data lake
    lake_password::String = "" # Password for DuckDB data lake
    store::Union{DBInterface.Connection,Nothing} = nothing # Connection to the TRE datastore
    lake::Union{DBInterface.Connection,Nothing} = nothing # Connection to the DuckDB data lake
end

abstract type AbstractStudy end

Base.@kwdef mutable struct Domain
    domain_id::Union{Int,Nothing} = nothing
    name::String
    uri::Union{Missing,String} = missing
    description::Union{Missing,String} = missing
end

Base.@kwdef mutable struct Study <: AbstractStudy
    study_id::Union{UUID,Nothing} = nothing
    name::String = "study_name"
    description::String = "study description"
    external_id::String = "external_id"
    study_type_id::Int = 1
    domains::Vector{Domain} = Domain[] # List of domains associated with the study
end

Base.@kwdef mutable struct Entity
    entity_id::Union{Int,Nothing} = nothing
    domain::Domain
    uuid::Union{UUID,Nothing} = nothing
    name::String
    description::Union{Missing,String} = missing
    ontology_namespace::Union{Missing,String} = missing
    ontology_class::Union{Missing,String} = missing
end

Base.@kwdef mutable struct EntityRelation
    entityrelation_id::Union{Int,Nothing} = nothing
    subject_entity::Entity #The entity being described
    object_entity::Entity #The entity that is related to the subject entity
    domain::Domain
    uuid::Union{UUID,Nothing} = nothing
    name::String
    description::Union{Missing,String} = missing
    ontology_namespace::Union{Missing,String} = missing
    ontology_class::Union{Missing,String} = missing
end
# Define supertypes to break circular concrete references
abstract type AbstractAsset end
abstract type AbstractAssetVersion end

Base.@kwdef mutable struct Asset <: AbstractAsset
    asset_id::Union{UUID,Nothing} = nothing
    study::Study
    name::String
    description::Union{Missing,String} = missing
    asset_type::String = "dataset" # "dataset" or "file"
    # Use abstract element type; avoid AssetVersion[] default (would reference before defined)
    versions::Vector{AbstractAssetVersion} = AbstractAssetVersion[]
end

Base.@kwdef mutable struct AssetVersion <: AbstractAssetVersion
    version_id::Union{UUID,Nothing} = nothing
    # Reference abstract to avoid circular concrete dependency
    asset::AbstractAsset
    major::Int32 = 1
    minor::Int32 = 0
    patch::Int32 = 0
    note::Union{Missing,String} = missing
    doi::Union{Missing,String} = missing
    is_latest::Bool = true
end

Base.@kwdef mutable struct DataFile
    version::Union{AssetVersion,Nothing} = nothing
    compressed::Bool = false
    encrypted::Bool = false
    compression_algorithm::Union{String,Missing} = missing
    encryption_algorithm::Union{String,Missing} = missing
    salt::Union{Missing,String} = missing
    storage_uri::String = ""
    edam_format::Union{Missing,String} = missing
    digest::Union{Missing,String} = missing
end

abstract type AbstractVocabulary end
abstract type AbstractVocabularyItem end

Base.@kwdef mutable struct VocabularyItem <: AbstractVocabularyItem
    vocabulary_item_id::Union{Int,Nothing} = nothing
    vocabulary_id::Int
    value::Int64
    code::String
    description::Union{String,Missing}
end

Base.@kwdef mutable struct Vocabulary <: AbstractVocabulary
    vocabulary_id::Union{Int,Nothing} = nothing
    name::String
    description::Union{String,Missing} = missing
    items::Vector{AbstractVocabularyItem} = AbstractVocabularyItem[]
end

Base.@kwdef mutable struct Variable
    variable_id::Union{Int,Nothing} = nothing
    domain_id::Int
    name::String
    value_type_id::Int
    value_format::Union{Missing,String} = missing # Used for data and time formats
    vocabulary_id::Union{Missing,Int} = missing # Reference to a vocabulary if applicable
    keyrole::String = "none" # "none", "record", "external"
    description::Union{Missing,String} = missing
    ontology_namespace::Union{Missing,String} = missing
    ontology_class::Union{Missing,String} = missing
    vocabulary::Union{Missing,Vocabulary} = missing
end

Base.@kwdef mutable struct DataSet
    version::Union{AssetVersion,Nothing} = nothing
    variables::Vector{Variable} = Variable[]
end

Base.@kwdef mutable struct Transformation
    transformation_id::Union{Int,Nothing} = nothing
    transformation_type::String = "transform" # "ingest", "transform", "entity", "export"
    description::String
    repository_url::Union{String,Nothing} = nothing
    commit_hash::Union{String,Nothing} = nothing
    file_path::String # Path to the script or notebook in the repository
    inputs::Vector{AssetVersion} = AssetVersion[] # Input asset versions
    outputs::Vector{AssetVersion} = AssetVersion[] # Output asset versions
end
#endregion
"""
    createdatastore(store::DataStore; superuser::String="postgres", superpwd::String="", port::Int=5432)

Create or replace a PostgreSQL database for the TRE datastore, including the datalake if specified.
This function creates a PostgreSQL database with the specified name and user credentials, and optionally creates a data lake using the DuckDb extension ducklake.
    store::DataStore: The DataStore object containing connection details for the datastore and datalake databases.
    superuser::String: The superuser name for PostgreSQL (default is "postgres").
    superpwd::String: The superuser password for PostgreSQL (default is empty).
    port::Int: The port number for the PostgreSQL server (default is 5432
NB: ONLY USE THIS FUNCTION IN DEVELOPMENT OR TESTING ENVIRONMENTS,
    as it will drop the existing database, lake and all its contents.
"""
function createdatastore(store::DataStore; superuser::String="postgres", superpwd::String="", port::Int=5432)
    store_conn = DBInterface.connect(LibPQ.Connection, "host=$(store.server) port=$(port) dbname=postgres user=$(superuser) password=$(superpwd)")
    try
        replace_database(store_conn, store.dbname, store.user, store.password)
    finally
        DBInterface.close!(store_conn)
    end

    if !isnothing(store.lake_db) && !isempty(store.lake_db)
        lake_conn = DBInterface.connect(LibPQ.Connection, "host=$(store.server) port=$(port) dbname=postgres user=$(superuser) password=$(superpwd)")
        try
            replace_database(lake_conn, store.lake_db, store.lake_user, store.lake_password)
        finally
            DBInterface.close!(lake_conn)
        end
    end
    emptydir(store.lake_data) # Remove existing lake data directory
    # Open connection then build schema via createdatabase(conn)
    conn = nothing
    try
        conn = DBInterface.connect(LibPQ.Connection, "host=$(store.server) port=$(port) dbname=$(store.dbname) user=$(store.user) password=$(store.password)")
        createdatabase(conn)
    finally
        # Reconnect as application user if different credentials supplied
        DBInterface.close!(conn)
    end
end

"""
    get_studyid(db::DBInterface.Connection, name)

Return the `source_id` of source `name`, returns `missing` if source doesn't exist
"""
function get_studyid(store::DataStore, name::AbstractString)::Union{UUID,Nothing}
    study_id = get_namedkey(store.store, "studies", name, :study_id)
    @info "Study ID for $(name): $(study_id)"
    if ismissing(study_id)
        return nothing
    end
    return UUID(study_id)
end
"""
    lookup_variables(db, variable_names, domain)

Returns a DataFrame with dataset variable names and ids
"""
function lookup_variables(db, variable_names, domain)
    names = DataFrame(:name => variable_names)
    variables = selectdataframe(db, "variables", ["name", "variable_id", "value_type_id"], ["domain_id"], [domain]) |> DataFrame
    return innerjoin(variables, names, on=:name) #just the variables in this dataset
end
"""
    savedataframetolake(lake::DBInterface.Connection, df::AbstractDataFrame, name::String, description::String)

Save dataframe to data lake, convert columns of type Missing to Union{String, Missing} for DuckDB compatibility
NOTE: This function assumes that the ducklake metadatabase is attached as LAKE_ALIAS
DEPPRECATED: DataFrames should be saved with a valid DataSet object
"""
function savedataframetolake(store::DataStore, df::AbstractDataFrame, name::String, description::String)
    convert_missing_to_string!(df) # Convert columns of type Missing to Union{String, Missing} for DuckDB compatibility
    DuckDB.register_table(store.lake, df, "__DF")
    sql = "CREATE OR REPLACE TABLE $LAKE_ALIAS.$(name) AS SELECT * FROM __DF"
    DBInterface.execute(store.lake, sql)
    sql = "COMMENT ON TABLE $LAKE_ALIAS.$(name) IS '$(description)'"
    DBInterface.execute(store.lake, sql)
    DuckDB.unregister_table(store.lake, "__DF")
    return nothing
end

"""
Supporting fuctions
"""

"""
    get_namedkey(db::DBInterface.Connection, table, key, keycol)

 Return the integer key from table `table` in column `keycol` (`keycol` must be a `Symbol`) for key with name `key`
"""
function get_namedkey(db::DBInterface.Connection, table, key, keycol)
    stmt = prepareselectstatement(db, table, [String(keycol)], ["name"])
    df = DBInterface.execute(stmt, [key]) |> DataFrame
    if nrow(df) == 0
        return missing
    else
        return df[1, keycol]
    end
end

"""
    get_variable_id(db::DBInterface.Connection, domain, name)

    Returns the `variable_id` of variable named `name` in domain with id `domain`
"""
function get_variable_id(db::DBInterface.Connection, domain, name)
    stmt = prepareselectstatement(db, "variables", ["variable_id"], ["domain_id", "name"])
    result = DBInterface.execute(stmt, [domain, name]; iterate_rows=true) |> DataFrame
    if nrow(result) == 0
        return missing
    else
        return result[1, :variable_id]
    end
end
"""
    get_variable(db::DBInterface.Connection, variable_id::Int)

Returns the entry of variable with `variable_id`
"""
function get_variable(db::DBInterface.Connection, variable_id::Int)
    stmt = prepareselectstatement(db, "variables", ["*"], ["variable_id"])
    result = DBInterface.execute(stmt, [variable_id]) |> DataFrame
    if nrow(result) == 0
        return missing
    else
        return result
    end
end

"""
    lines(str)

Returns an array of lines in `str` 
"""
lines(str) = split(str, '\n')
"""
    dataset_variables(db::DBInterface.Connection, dataset)::AbstractDataFrame

Return the list of variables in a dataset
"""
function dataset_variables(db::DBInterface.Connection, dataset::DataSet)::AbstractDataFrame
    sql = """
    SELECT
        v.*
    FROM dataset_variables dv
      JOIN variables v ON dv.variable_id = v.variable_id
    WHERE dv.dataset_id = @dataset;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, (dataset = dataset)) |> DataFrame
end
"""
    get_datasetname(dataset::DataSet; include_schema::Bool=false)::String

Return a valid dataset name for the dataset, optionally including the schema (study) name.
- `dataset`: The DataSet object for which to generate the name.
- `include_schema`: If true, includes the schema (study) name as a prefix to the dataset name (default is false).
"""
function get_datasetname(dataset::DataSet; include_schema::Bool=false)::String
    if isnothing(dataset.version) || isnothing(dataset.version.asset) || isnothing(dataset.version.asset.name)
        error("Dataset or its asset name is not defined.")
    end
    schema = to_ncname(dataset.version.asset.study.name, strict=true)
    base_name = to_ncname(dataset.version.asset.name, strict=true)
    version = to_ncname(string(VersionNumber(dataset.version.major, dataset.version.minor, dataset.version.patch)), strict=true)
    if include_schema
        return schema * "." * base_name * version
    else
        return base_name * version
    end
end
"""
    get_datafilename(datafile::DataFile)::String

Get a valid filename for a datafile based on its asset name and version
 - `datafile`: The DataFile object for which to generate the filename.
"""
function get_datafilename(datafile::DataFile)::String
    if isnothing(datafile.version) || isnothing(datafile.version.asset) || isnothing(datafile.version.asset.name)
        error("Dataset or its asset name is not defined.")
    end
    base_name = to_ncname(datafile.version.asset.name, strict=true)
    version = to_ncname(string(VersionNumber(datafile.version.major, datafile.version.minor, datafile.version.patch)), strict=true)
    return base_name * version
end
"""
    dataset_to_arrow(db, dataset, datapath)

Save a dataset in the arrow format
"""
function dataset_to_arrow(store::DataStore, dataset::DataSet, outputdir::String; replace::Bool=false)::String
    file_name = get_datasetname(dataset) * ".arrow"
    if !replace && isfile(joinpath(outputdir, file_name))
        error("File already exists: $(joinpath(outputdir, file_name)). Use `replace=true` to overwrite.")
    end
    df = dataset_to_dataframe(store, dataset)
    if !isdir(outputdir)
        mkpath(outputdir)
    end
    Arrow.write(joinpath(outputdir, filename), df, compress=:zstd)
end
"""
    dataset_to_csv(store::DataStore, dataset::DataSet, outputdir::String; replace::Bool=false, compress=false)

Save a dataset in compressed csv format
- `store`: The DataStore object containing the datastore and datalake connections.
- `dataset`: The DataSet object to be saved as CSV.
- `outputdir`: The directory where the CSV file will be saved.
- `replace`: If true, will overwrite existing files (default is false).
- `compress`: If true, will save the CSV file in compressed .zst format (default is false).
"""
function dataset_to_csv(store::DataStore, dataset::DataSet, outputdir::String; replace::Bool=false, compress=false)
    file_name = get_datasetname(dataset) * (compress ? ".zst" : ".csv")
    if !replace && isfile(joinpath(outputdir, file_name))
        error("File already exists: $(joinpath(outputdir, file_name)). Use `replace=true` to overwrite.")
    end
    if !isdir(outputdir)
        mkpath(outputdir)
    end
    df = dataset_to_dataframe(store, dataset)
    if (compress)
        open(ZstdCompressorStream, joinpath(outputdir, file_name), "w") do stream
            CSV.write(stream, df)
        end
    else
        CSV.write(joinpath(outputdir, file_name), df)
    end
end
"""
    ingest_redcap_project(api_url::AbstractString, api_token::AbstractString, study::Study, domain::Domain)

Retrieves the REDCap project metadata and add the project variables to the TRE datastore.
Downloads the REDCap project records in EAV format to a csv file and saves it to the data lake and creates an ingest transformation.
Transforms the csv file from EAV (long) format to wide format dataset and registers the dataset in the TRE datastore.
- `api_url`: The URL of the REDCap API endpoint.
- `api_token`: The API token for the REDCap project.
- `study`: The Study object to associate with the REDCap project. 
- `domain`: The Domain object to associate with the REDCap project.
- `vocabulary_prefix`: The prefix for the vocabulary used in the REDCap project (default is "REDCap").
- `forms`: A vector of form names to include in the REDCap project (default is empty, meaning all forms).
- `fields`: A vector of field names to include in the REDCap project (default is empty, meaning all fields).
Returns the DataFile object representing the ingested REDCap project data.
"""
function ingest_redcap_project(store::DataStore, api_url::AbstractString, api_token::AbstractString, study::Study, domain::Domain;
    vocabulary_prefix::String="REDCap", forms::Vector{String}=String[], fields::Vector{String}=String[])::DataFile
    conn = store.store
    if isnothing(conn)
        error("No datastore connection available. Please open datastore connection in DataStore.store before calling ingest_redcap_project.")
    end
    datafile = nothing
    # Start transaction on the datastore
    transaction_begin(conn)
    try
        register_redcap_datadictionary(store, domain.domain_id, api_url, api_token;
            vocabulary_prefix=vocabulary_prefix, use_transaction=false)
        @info "Registered REDCap datadictionary for study: $(study.name) in domain: $(domain.name)"
        redcap_info = redcap_project_info(api_url, api_token)
        # Download REDCap records in EAV format
        path = redcap_export_eav(api_url, api_token, forms=forms, fields=fields, decode=true, lake_root=joinpath(store.lake_data, study.name))
        @info "Downloaded REDCap EAV export to: $path"
        datafile = attach_datafile(store, study, "redcap_$(redcap_info.project_id)_eav", path,
            "http://edamontology.org/format_3752"; description="REDCap project $(redcap_info.project_id) EAV Export for $(redcap_info.project_title)", compress=true)
        @info "Attached data file: $(datafile.storage_uri) with digest $(datafile.digest)"
        #Create an ingest transformation to record this ingestion
        commit = git_commit_info(; script_path=caller_file_runtime(1))
        transformation = Transformation(
            transformation_type="ingest",
            description="Ingested REDCap project $(redcap_info.project_id) records for project: $(redcap_info.project_title) using AHRI_TRE ingest_redcap_project function",
            repository_url=commit.repo_url,
            commit_hash=commit.commit,
            file_path=commit.script_relpath
        )
        # Save the transformation to the datastore
        save_transformation!(store, transformation)
        @info "Created transformation with ID: $(transformation.transformation_id)"
        # Add the data file as an output to the transformation
        add_transformation_output(store, transformation, datafile.version)
        # Commit transaction
        transaction_commit(conn)
    catch e
        # Attempt rollback, then rethrow original error
        transaction_rollback(conn)
        rethrow(e)
    end
    return datafile
end
"""
    prepare_datafile(file_path::AbstractString, edam_format::String; compress::Bool=false, encrypt::Bool=false) -> DataFile

Prepare a DataFile for registration: validate path, optionally compress, compute digest and populate DataFile fields.
"""
function prepare_datafile(file_path::AbstractString, edam_format::String; compress::Bool=false, encrypt::Bool=false)::DataFile
    if encrypt
        throw(ErrorException("Encryption is not currently implemented."))
    end
    if !isfile(file_path)
        throw(ArgumentError("File does not exist: $file_path"))
    end

    datafile = DataFile(
        storage_uri=path_to_file_uri(file_path),
        edam_format=edam_format,
        encrypted=encrypt,
        compressed=compress
    )

    if compress
        compressed_path = string(file_path, ".zst")
        if !isfile(compressed_path)
            open(file_path, "r") do infile
                open(ZstdCompressorStream, compressed_path, "w") do outfile
                    write(outfile, read(infile))
                end
            end
            datafile.storage_uri = path_to_file_uri(compressed_path)
            datafile.compression_algorithm = "zstd"
            @info "Compressed file created: $compressed_path"
            rm(file_path; force=true)
            file_path = compressed_path
        else
            throw(ArgumentError("Compressed file already exists: $compressed_path"))
        end
    end

    datafile.digest = sha256_digest_hex(file_path)
    @info "File digest: $(datafile.digest)"

    return datafile
end

"""
    attach_datafile(store::DataStore, study::Study, asset_name::String,
    file_path::AbstractString, edam_format::String; description::Union{String,Missing}=missing, compress::Bool=false, encrypt::Bool=false)::DataFile

Attach a data file that is already in the data lake to the TRE datastore.
- `store`: The DataStore object containing connection details for the datastore.
- `study`: The Study object to associate with the data file.
- `asset_name`: The name of the asset to which the data file will be attached. Must comply with xsd:NCName restrictions.
- `file_path`: The full path including the file name to the file.
- `edam_format`: The EDAM format of the data file (e.g., "http://edamontology.org/format_3752" for a csv file).
- `description`: A description of the data file (default is missing).
- `compress`: Whether the file should be compressed (default is false). 
   If true, the file will be compressed using zstd, and the existing file will be replaced with the compressed version.
- `encrypt`: Whether the file should be encrypted (default is false). **NOT currently implemented**
This function does not copy the file, it only registers it in the TRE datastore.
It assumes the file is already in the data lake and creates an Asset object with a base version
"""
function attach_datafile(store::DataStore, study::Study, asset_name::String,
    file_path::AbstractString, edam_format::String; description::Union{String,Missing}=missing, compress::Bool=false, encrypt::Bool=false)::DataFile
    datafile = prepare_datafile(file_path, edam_format; compress=compress, encrypt=encrypt)
    # Create the Asset object
    asset = create_asset(store, study, asset_name, "file", description)
    datafile.version = asset.versions[1]  # Use the base version of the asset
    # Add the datafile to the datastore
    register_datafile(store, datafile)
    @info "Registered data file for asset: $(asset_name) with version ID: $(datafile.version.version_id)"
    # Return the DataFile object
    return datafile
end
"""
    attach_datafile(store::DataStore, assetversion::AssetVersion, file_path::AbstractString, edam_format::String;
    compress::Bool=false, encrypt::Bool=false)::DataFile

Attach a data file to an existing asset version in the TRE datastore.
- `store`: The DataStore object containing connection details for the datastore.
- `assetversion`: The AssetVersion object to which the data file will be attached.
- `file_path`: The full path including the file name to the file.
- `edam_format`: The EDAM format of the data file (e.g., "http://edamontology.org/format_3752" for a csv file).
- `compress`: Whether the file should be compressed (default is false). 
   If true, the file will be compressed using zstd, and the existing file will be replaced with the compressed version.
- `encrypt`: Whether the file should be encrypted (default is false). **NOT currently implemented**
This function does not copy the file, it only registers it in the TRE datastore.
It assumes the file is already in the data lake and creates a DataFile object associated with the given asset version.
"""
function attach_datafile(store::DataStore, assetversion::AssetVersion, file_path::AbstractString, edam_format::String;
    compress::Bool=false, encrypt::Bool=false)::DataFile
    datafile = prepare_datafile(file_path, edam_format; compress=compress, encrypt=encrypt)
    datafile.version = assetversion
    # Add the datafile to the datastore
    register_datafile(store.store, datafile)
    @info "Registered data file for asset version: $(assetversion.version_id)"
    # Return the DataFile object
    return datafile
end
"""
    attach_new_datafile_version(store::DataStore, assetversion::AssetVersion, version_note::String,
    file_path::AbstractString, edam_format::String, bumpmajor::Bool, bumpminor::Bool;
    compress::Bool=false, encrypt::Bool=false)::DataFile

Attach a new data file as a new version to an existing asset in the TRE datastore.
- `store`: The DataStore object containing connection details for the datastore.
- `assetversion`: The existing AssetVersion object to which the new data file version will be attached.
- `version_note`: A note describing the changes in this new version.
- `file_path`: The full path including the file name to the new file.
- `edam_format`: The EDAM format of the new data file (e.g., "http://edamontology.org/format_3752" for a csv file).
- `bumpmajor`: If true, increments the major version number and resets minor and patch to 0.
- `bumpminor`: If true, increments the minor version number and resets patch to 0.
- `compress`: Whether the file should be compressed (default is false). 
   If true, the file will be compressed using zstd, and the existing file will be replaced with the compressed version.
- `encrypt`: Whether the file should be encrypted (default is false). **NOT currently implemented**
This function does not copy the file, it only registers it in the TRE datastore.
It assumes the file is already in the data lake and creates a new DataFile object associated with a new version of the given asset.
"""
function attach_new_datafile_version(store::DataStore, assetversion::AssetVersion, version_note::String,
    file_path::AbstractString, edam_format::String, bumpmajor::Bool, bumpminor::Bool;
    compress::Bool=false, encrypt::Bool=false)::DataFile
    # Prepare the new data file
    datafile = prepare_datafile(file_path, edam_format; compress=compress, encrypt=encrypt)
    if bumpmajor
        assetversion.major += 1
        assetversion.minor = 0
        assetversion.patch = 0
    elseif bumpminor
        assetversion.minor += 1
        assetversion.patch = 0
    else # assume patch bump
        assetversion.patch += 1
    end
    assetversion.note = version_note
    assetversion.is_latest = true # Set the new version as the latest
    assetversion.version_id = nothing # Reset version_id to be generated by save_version!
    save_version!(store, assetversion) # Save the updated version
    datafile.version = assetversion # Associate the data file with the new version
    # Register the data file in the datastore
    @info "Registering new data file version"
    register_datafile(store, datafile)
    @info "Registered new data file version for asset version: $(assetversion.version_id)"
    # Return the DataFile object
    return datafile
end

"""
    transform_eav_to_dataset(store::DataStore, datafile::DataFile)::DataSet

Transform an EAV (Entity-Attribute-Value) data file into a dataset.
- 'store' is the DataStore object containing the database connection.
- 'datafile' is the DataFile object representing the EAV data file.
- 'convert' indicates whether to convert data types based on variable definitions (default is true).
This function creates a new dataset in the database by pivoting the EAV data into a wide format.
It aggregates multiple values for the same field per record into a single column.
The dataset name is derived from the datafile's asset name, dropping the "_eav" suffix if present.
Returns a DataSet object representing the transformed data.
This function assumes the EAV data is stored in a csv table with columns: record, field_name, and value.
"""
function transform_eav_to_dataset(store::DataStore, datafile::DataFile; convert=true)::DataSet
    #set dataset name to the datafile asset name, drop "_eav" suffix if present
    asset = datafile.version.asset
    if isnothing(asset)
        @error "DataFile must have a valid asset version with an associated asset"
        return nothing
    end
    study = asset.study
    if isnothing(study)
        @error "DataFile must have a valid asset version with an associated study"
        return nothing
    end
    dataset_name = replace(asset.name, r"_eav$" => "")
    dataset_name = to_ncname(dataset_name)
    if dataset_name == ""
        @error "Dataset name derived from asset name is empty after removing '_eav' suffix"
        return nothing
    end
    try
        transaction_begin(store)
        dataset = create_dataset_meta(store, study, dataset_name, "Dataset from eav file $(asset.name)", datafile)
        transform_eav_to_table!(store, datafile, dataset; convert=convert)
        #Create a transformation to record this ingestion
        commit = git_commit_info(; script_path=caller_file_runtime(1))
        transformation = Transformation(
            transformation_type="transform",
            description="Transformed eav $(asset.name) to dataset $(dataset_name)",
            repository_url=commit.repo_url,
            commit_hash=commit.commit,
            file_path=commit.script_relpath
        )
        # Save the transformation to the datastore
        save_transformation!(store, transformation)
        @info "Created transformation with ID: $(transformation.transformation_id)"
        # Add the data file as an input to the transformation
        add_transformation_input(store, transformation, datafile.version)
        # Add the data set as an output to the transformation
        add_transformation_output(store, transformation, dataset.version)
        transaction_commit(store)
        return dataset
    catch e
        transaction_rollback(store)
        @error "Error transforming EAV to dataset: $(e)"
        return nothing
    end
end
"""
    read_dataset(store::DataStore, dataset::DataSet)::AbstractDataFrame

Read a dataset from the TRE datastore and return it as an AbstractDataFrame.
- `store`: The DataStore object containing the datastore connection.
- `dataset`: The DataSet object representing the dataset to be read.
This function retrieves the dataset from the datastore and converts it to an AbstractDataFrame.
"""
function read_dataset(store::DataStore, dataset::DataSet)::AbstractDataFrame
    return dataset_to_dataframe(store, dataset)
end
"""
    read_dataset(store::DataStore, study_name::String, dataset_name::String)::AbstractDataFrame

Read a dataset from the TRE datastore by study name and dataset name.
- `store`: The DataStore object containing the datastore connection.
- `study_name`: The name of the study containing the dataset.
- `dataset_name`: The name of the dataset to be read.
This function retrieves the study and dataset asset from the datastore, gets the latest version of the dataset,
and converts it to an AbstractDataFrame.
"""
function read_dataset(store::DataStore, study_name::String, dataset_name::String)::AbstractDataFrame
    study = get_study(store, study_name)
    if isnothing(study)
        error("Study not found: $study_name")
    end
    asset = get_asset(store, study, dataset_name; include_versions=true, asset_type="dataset")
    if isnothing(asset)
        error("Dataset asset not found: $dataset_name in study $study_name")
    end
    version = get_latest_version(asset)
    if isnothing(version)
        error("No versions found for dataset asset: $dataset_name in study $study_name")
    end
    return dataset_to_dataframe(store, DataSet(version=version))
end
"""
    create_study!(store::DataStore, study::Study, domain::Domain)::Study

Create a new study in the TRE datastore and associate it with a domain.
- `store`: The DataStore object containing the datastore connection.
- `study`: The Study object representing the study to be created.
- `domain`: The Domain object representing the domain to associate with the study.
This function inserts or updates the study in the datastore and links it to the specified domain.
"""
function create_study!(store::DataStore, study::Study, domain::Domain)::Study
    if isnothing(store)
        throw(ArgumentError("DataStore cannot be nothing"))
    end
    if isnothing(domain) || isnothing(domain.domain_id)
        throw(ArgumentError("A valid domain with domain_id must be provided"))
    end
    upsert_study!(store, study)
    add_study_domain!(store, study, domain)
    return study
end
"""
    create_entity!(store::DataStore, entity::Entity, domain::Domain)::Entity

Create a new entity in the TRE datastore and associate it with a domain.
- `store`: The DataStore object containing the datastore connection.
- `entity`: The Entity object representing the entity to be created.
- `domain`: The Domain object representing the domain to associate with the entity.
This function inserts or updates the entity in the datastore and links it to the specified domain.
"""
function create_entity!(store::DataStore, entity::Entity, domain::Domain)::Entity
    if isnothing(store)
        throw(ArgumentError("DataStore cannot be nothing"))
    end
    if isnothing(entity) || isnothing(entity.name)
        throw(ArgumentError("A valid entity with a name must be provided"))
    end
    # Verify domain
    if isnothing(domain) || isnothing(domain.domain_id)
        throw(ArgumentError("A valid domain with domain_id must be provided"))
    end
    entity.domain = domain
    upsert_entity!(store, entity)
    return entity
end
"""
    create_entity_relation!(store::DataStore, entityrelation::EntityRelation, domain::Domain)::EntityRelation

Create a new entity relation in the TRE datastore and associate it with a domain.
- `store`: The DataStore object containing the datastore connection.
- `entityrelation`: The EntityRelation object representing the entity relation to be created.
- `domain`: The Domain object representing the domain to associate with the entity relation.
This function inserts or updates the entity relation in the datastore and links it to the specified domain.
"""
function create_entity_relation!(store::DataStore, entityrelation::EntityRelation, domain::Domain)::EntityRelation
    if isnothing(store)
        throw(ArgumentError("DataStore cannot be nothing"))
    end
    if isnothing(entityrelation) || isnothing(entityrelation.name)
        throw(ArgumentError("A valid entity relation with a name must be provided"))
    end
    # Verify domain
    if isnothing(domain) || isnothing(domain.domain_id)
        throw(ArgumentError("A valid domain with domain_id must be provided"))
    end
    # Verify subject and object entities
    if isnothing(entityrelation.subject_entity) || isnothing(entityrelation.subject_entity.entity_id)
        throw(ArgumentError("A valid subject entity with entity_id must be provided"))
    end
    if isnothing(entityrelation.object_entity) || isnothing(entityrelation.object_entity.entity_id)
        throw(ArgumentError("A valid object entity with entity_id must be provided"))
    end
    entityrelation.domain = domain
    upsert_entityrelation!(store, entityrelation)
    return entityrelation
end
"""
    create_entity_relation!(store::DataStore, subject_name::String, object_name::String, relation_name::String, 
                                 domain_name::String, description::Union{Missing,String} = missing)::EntityRelation

Create a new entity relation in the TRE datastore by specifying subject and object entity names, relation name, and domain name.
- `store`: The DataStore object containing the datastore connection.
- `subject_name`: The name of the subject entity in the relation.
- `object_name`: The name of the object entity in the relation.
- `relation_name`: The name of the relation.
- `domain_name`: The name of the domain to associate with the entity relation.
- `description`: An optional description of the entity relation (default is missing).
This function looks up the subject and object entities by name within the specified domain,
creates the entity relation, and inserts or updates it in the datastore.
"""
function create_entity_relation!(store::DataStore, subject_name::String, object_name::String, relation_name::String,
    domain_name::String, description::Union{Missing,String}=missing)::EntityRelation
    if isnothing(store)
        throw(ArgumentError("DataStore cannot be nothing"))
    end
    if isnothing(subject_name) || isnothing(object_name) || isnothing(relation_name) || isnothing(domain_name)
        throw(ArgumentError("Subject name, object name, relation name, and domain name must be provided"))
    end
    domain = get_domain(store, domain_name)
    if isnothing(domain)
        throw(ArgumentError("Domain not found: $domain_name"))
    end
    subject_entity = get_entity(store, domain.domain_id, subject_name)
    if isnothing(subject_entity)
        throw(ArgumentError("Subject entity not found: $subject_name in domain $domain_name"))
    end
    object_entity = get_entity(store, domain.domain_id, object_name)
    if isnothing(object_entity)
        throw(ArgumentError("Object entity not found: $object_name in domain $domain_name"))
    end
    entityrelation = EntityRelation(
        subject_entity=subject_entity,
        object_entity=object_entity,
        domain=domain,
        name=relation_name,
        description=description
    )
    upsert_entityrelation!(store, entityrelation)
    return entityrelation
end
"""
    ingest_file(store::DataStore, study::Study, asset_name::String, file_path::AbstractString, edam_format::String;
    description::Union{String,Missing}=missing, compress::Bool=false, encrypt::Bool=false, new_version::Bool=false)::DataFile

Ingest a file into the TRE data lake and register it in the TRE datastore.
- `store`: The DataStore object containing connection details for the datastore and data lake.
- `study`: The Study object to associate with the ingested file.
- `asset_name`: The name of the asset to which the file will be attached. Must comply with xsd:NCName restrictions.
- `file_path`: The full path including the file name to the file to be ingested.
- `edam_format`: The EDAM format of the file (e.g., "http://edamontology.org/format_3752" for a csv file).
- `description`: A description of the data file (default is missing).
- `compress`: Whether the file should be compressed (default is false). 
   If true, the file will be compressed using zstd before being copied to the data lake.
- `encrypt`: Whether the file should be encrypted (default is false). **NOT currently implemented**
- `new_version`: If true, and an asset with the same name already exists in the study, a new version will be created (default is false).
This function copies the file to the data lake directory, optionally compresses it,
and registers it in the TRE datastore as a new asset or a new version of an existing asset.
It returns the DataFile object representing the ingested file
"""
function ingest_file(store::DataStore, study::Study, asset_name::String, file_path::AbstractString, edam_format::String;
    description::Union{String,Missing}=missing, compress::Bool=false, encrypt::Bool=false, new_version::Bool=false)::Union{DataFile,Nothing}
    # Check if file exists
    if !isfile(file_path)
        throw(ArgumentError("File does not exist: $file_path"))
    end
    # Check if asset name is valid NCName
    if to_ncname(asset_name, strict=true) != asset_name
        throw(ArgumentError("Asset name must comply with xsd:NCName restrictions: $asset_name"))
    end
    # Check if an asset by this name already exists in the study
    existing_asset = get_asset(store, study, asset_name; include_versions=true, asset_type="file")
    if !isnothing(existing_asset) && !new_version
        throw(ArgumentError("Asset with name $asset_name already exists in study $(study.name). Use `new_version=true` to add a new version."))
    end
    # if there is an existing asset get the latest version
    base_name, ext = splitext(basename(file_path))
    base_name = to_ncname(asset_name, strict=true) # use the asset name instead of the original filename
    version = to_ncname(string(VersionNumber(1, 0, 0)), strict=true)
    latest_version = nothing
    if !isnothing(existing_asset)
        latest_version = get_latest_version(existing_asset)
        if isnothing(latest_version)
            throw(ArgumentError("Existing asset $asset_name has no versions. Cannot add new version."))
        end
        version = to_ncname(string(VersionNumber(latest_version.major + 1, latest_version.minor, latest_version.patch)), strict=true)
    end
    # Copy the file to the data lake directory
    dest_path = joinpath(store.lake_data, study.name, base_name * version * ext)
    mkpath(dirname(dest_path)) # Create directory if it doesn't exist
    cp(file_path, dest_path, force=true)
    @info "Copied file to data lake: $dest_path"
    try
        transaction_begin(store)
        if !isnothing(latest_version)
            datafile = attach_new_datafile_version(store, latest_version, "New version from ingest_datafile", dest_path, edam_format, true, false; compress=compress, encrypt=encrypt)
        else
            datafile = attach_datafile(store, study, asset_name, dest_path, edam_format; description=description, compress=compress, encrypt=encrypt)
        end
        @info "Ingested data file for asset: $(asset_name) with version ID: $(datafile.version.version_id)"
        commit = git_commit_info(; script_path=caller_file_runtime(1))
        transformation = Transformation(
            transformation_type="ingest",
            description="Ingesting datafile $(file_path) to $(dest_path) in study $(study.name)",
            repository_url=commit.repo_url,
            commit_hash=commit.commit,
            file_path=commit.script_relpath
        )
        # Save the transformation to the datastore
        save_transformation!(store, transformation)
        @info "Created transformation with ID: $(transformation.transformation_id)"
        # Add the data set as an output to the transformation
        add_transformation_output(store, transformation, datafile.version)
        transaction_commit(store)
        return datafile
    catch e
        @error "Error ingesting file: $(e)"
        transaction_rollback(store)
        if isfile(dest_path)
            rm(dest_path; force=true)
            @info "Removed copied file from data lake: $dest_path"
        end
        return nothing
    end
end
function sql_to_dataset(store::DataStore, study::Study, dataset_name::String, conn::DBInterface.Connection, sql::String; description::Union{String,Missing}=missing, replace::Bool=false)::DataSet
    if isnothing(store)
        throw(ArgumentError("DataStore cannot be nothing"))
    end
    if isnothing(study) || isnothing(study.study_id)
        throw(ArgumentError("A valid study with study_id must be provided"))
    end
    if to_ncname(dataset_name, strict=true) != dataset_name
        throw(ArgumentError("Dataset name must comply with xsd:NCName restrictions: $dataset_name"))
    end
    existing_asset = get_asset(store, study, dataset_name; include_versions=true, asset_type="dataset")
    if !isnothing(existing_asset) && !replace
        throw(ArgumentError("Dataset asset with name $dataset_name already exists in study $(study.name). Use `replace=true` to replace it with a new version."))
    end
    try
        transaction_begin(store)
        # Create a temporary table to hold the SQL query results
        temp_table = "__DF"
        DBInterface.execute(conn, "DROP TABLE IF EXISTS $temp_table;")
        DBInterface.execute(conn, "CREATE TABLE $temp_table AS $sql;")
        # Load the data from the temporary table into the dataset
        load_table_to_dataset!(store, conn, temp_table, dataset; convert=true)
        # Drop the temporary table
        DBInterface.execute(conn, "DROP TABLE IF EXISTS $temp_table;")
        #Create a transformation to record this ingestion
        commit = git_commit_info(; script_path=caller_file_runtime(1))
        transformation = Transformation(
            transformation_type="transform",
            description="Transformed SQL query to dataset $(dataset_name)",
            repository_url=commit.repo_url,
            commit_hash=commit.commit,
            file_path=commit.script_relpath
        )
        # Save the transformation to the datastore
        save_transformation!(store, transformation)
        @info "Created transformation with ID: $(transformation.transformation_id)"
        # Add the data set as an output to the transformation
        add_transformation_output(store, transformation, dataset.version)
        transaction_commit(store)
        return dataset
    catch e
        transaction_rollback(store)
        @error "Error transforming SQL to dataset: $(e)"
        return nothing
    end
end
include("constants.jl")
include("utils.jl")
include("tredatabase.jl")
include("redcap.jl")

end #module

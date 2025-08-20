module AHRI_TRE

using DataFrames
using LibPQ
using DBInterface
using DuckDB
using ConfigEnv
using Dates
using Arrow
using DataStructures
using ODBC
using SHA
using CSV
using XLSX
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
    upsert_study!, upsert_domain!, get_domain, get_study, list_studies, add_study_domain!,
    upsert_entity!, get_entity, upsert_entityrelation!, get_entityrelation, list_domainentities, list_domainrelations,
    datasetlakename, git_commit_info,
    lookup_variables, get_eav_variables,
    get_namedkey, get_variable_id, get_variable, get_datasetname, updatevalues, insertdata, insertwithidentity,
    get_table, selectdataframe, prepareselectstatement, dataset_to_dataframe, dataset_to_arrow, dataset_to_csv,
    dataset_variables, dataset_column, savedataframe,
    ingest_redcap_project, register_redcap_datadictionary

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
    domain_id::Int
    uuid::Union{UUID,Nothing} = nothing
    name::String
    description::Union{Missing,String} = missing
    ontology_namespace::Union{Missing,String} = missing
    ontology_class::Union{Missing,String} = missing
end

Base.@kwdef mutable struct EntityRelation
    entityrelation_id::Union{Int,Nothing} = nothing
    entity_id_1::Int
    entity_id_2::Int
    domain_id::Int
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
    assetversion::Union{AssetVersion,Nothing} = nothing
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
    vocabulary::Union{Missing,Vocabulary} = missing
end

Base.@kwdef mutable struct DataSet
    assetversion::Union{AssetVersion,Nothing} = nothing
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
    maint = DBInterface.connect(LibPQ.Connection, "host=$(store.server) port=$(port) dbname=postgres user=$(superuser) password=$(superpwd)")
    try
        replace_database(maint, store.dbname, store.user, store.password)
    finally
        DBInterface.close!(maint)
    end

    if !isnothing(store.lake_db) && !isempty(store.lake_db)
        maint2 = DBInterface.connect(LibPQ.Connection, "host=$(store.server) port=$(port) dbname=postgres user=$(superuser) password=$(superpwd)")
        try
            replace_database(maint2, store.lake_db, store.lake_user, store.lake_password)
        finally
            DBInterface.close!(maint2)
        end
    end

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
NOTE: This function assumes that the ducklake metadatabase is attached as "tre_lake"
"""
function savedataframetolake(store::DataStore, df::AbstractDataFrame, name::String, description::String)
    # Save dataframe to data lake
    # @info df
    # @info describe
    convert_missing_to_string!(df) # Convert columns of type Missing to Union{String, Missing} for DuckDB compatibility
    DuckDB.register_table(lake, df, "__DF")
    sql = "CREATE OR REPLACE TABLE tre_lake.$(name) AS SELECT * FROM __DF"
    DBInterface.execute(lake, sql)
    sql = "COMMENT ON TABLE tre_lake.$(name) IS '$(description)'"
    DBInterface.execute(lake, sql)
    DuckDB.unregister_table(lake, "__DF")
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
    get_datasetmetadata(store::DataStore, asset_id::UUID, version_id::UUID = nothing)::DataSet

Get the metadata of a dataset from the datastore. 
If `version_id` is provided, it retrieves the metadata for that specific version, otherwise it retrieves the latest version.
- `store`: The DataStore object containing connection details for the datastore.
- `asset_id`: The UUID of the asset representing the dataset.
- `version_id`: The UUID of the specific version of the dataset (optional). If not provided, it retrieves the latest version.
"""
function get_datasetmetadata(store::DataStore, asset_id::UUID, version_id::UUID=nothing)::DataSet
    db = store.store
    if isnothing(db)
        error("No datastore connection available. Please provide a valid PostgreSQL connection for the DataStore.")
    end

    # Prepare SQL query to get dataset metadata
    sql = """
    SELECT
        av.asset_id,
        av.version_id,
        v.variable_id,
        v.name AS variable_name,
        v.value_type_id
    FROM asset_versions av
      JOIN assets a ON av.asset_id = a.asset_id
      JOIN variables v ON a.asset_id = v.asset_id
    WHERE a.asset_id = @asset_id
    """

    if !isnothing(version_id)
        sql *= " AND av.version_id = @version_id"
    else
        sql *= " AND av.is_latest = true"
    end

    stmt = DBInterface.prepare(db, sql)
    result = DBInterface.execute(stmt, (asset_id=asset_id, version_id=version_id)) |> DataFrame
    # Set version_id to the first version_id in the result if not provided
    if isnothing(version_id) && nrow(result) > 0
        version_id = result[1, :version_id]
    end
    # Create DataSet object and populate it with variables
    dataset = DataSet()
    dataset.assetversion = AssetVersion(asset=Asset(asset_id=asset_id), version_id=version_id)

    for row in eachrow(result)
        variable = Variable(variable_id=row.variable_id, name=row.variable_name, value_type_id=row.value_type_id)
        push!(dataset.variables, variable)
    end

    return dataset
end
"""
    dataset_variables(db::DBInterface.Connection, dataset)::AbstractDataFrame

Return the list of variables in a dataset
"""
function dataset_variables(db::DBInterface.Connection, dataset::DataSet)::AbstractDataFrame
    sql = """
    SELECT
        v.variable_id,
        v.name variable,
        v.value_type_id
    FROM dataset_variables dv
      JOIN variables v ON dv.variable_id = v.variable_id
    WHERE dv.dataset_id = @dataset;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, (dataset = dataset)) |> DataFrame
end
"""
    dataset_to_arrow(db, dataset, datapath)

Save a dataset in the arrow format
"""
function dataset_to_arrow(db, dataset, datapath, lake::DuckDB.Connection=nothing)
    outputdir = joinpath(datapath, "arrowfiles")
    if !isdir(outputdir)
        mkpath(outputdir)
    end
    df = dataset_to_dataframe(db, dataset, lake)
    Arrow.write(joinpath(outputdir, "$(get_datasetname(db,dataset)).arrow"), df, compress=:zstd)
end


"""
    dataset_to_csv(db, dataset_id, datapath, compress)

Save a dataset in compressed csv format
"""
function dataset_to_csv(db, dataset_id, datapath, compress=false, lake::DuckDB.Connection=nothing)
    outputdir = joinpath(datapath, "csvfiles")
    if !isdir(outputdir)
        mkpath(outputdir)
    end
    df = dataset_to_dataframe(db, dataset_id, lake)
    if (compress)
        CSV.write(joinpath(outputdir, "$(get_datasetname(db,dataset_id)).gz"), df, compress=true) #have trouble opening on MacOS
    else
        CSV.write(joinpath(outputdir, "$(get_datasetname(db,dataset_id)).csv"), df)
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
Returns nothing.
"""
function ingest_redcap_project(store::DataStore, api_url::AbstractString, api_token::AbstractString, study::Study, domain::Domain;
    vocabulary_prefix::String="REDCap", forms::Vector{String}=String[], fields::Vector{String}=String[])
    conn = store.store
    if isnothing(conn)
        error("No datastore connection available. Please open datastore connection in DataStore.store before calling ingest_redcap_project.")
    end

    # Start transaction on the datastore
    transaction_begin(conn)
    try
        register_redcap_datadictionary(store, domain.domain_id, api_url, api_token;
        vocabulary_prefix=vocabulary_prefix, use_transaction=false)
        @info "Registered REDCap datadictionary for study: $(study.name) in domain: $(domain.name)"
        redcap_info = redcap_project_info(api_url, api_token)
        # Download REDCap records in EAV format
        path = redcap_export_eav(api_url, api_token, forms=forms, fields=fields, decode=true)
        @info "Downloaded REDCap EAV export to: $path"
        datafile = attach_datafile(store, study, "redcap_$(redcap_info.project_id)_eav", path, 
                    "http://edamontology.org/format_3752"; description = "REDCap project $(redcap_info.project_id) EAV Export for $(redcap_info.project_title)",compress=true)
        @info "Attached data file: $(datafile.storage_uri) with digest $(datafile.digest)"
        #Create an ingest transformation to record this ingestion
        commit = git_commit_info(; script_path=@__FILE__)
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
        add_transformation_output(store, transformation.transformation_id, datafile.assetversion.version_id)
        @info "Added transformation output"

        # Commit transaction
        transaction_commit(conn)
    catch e
        # Attempt rollback, then rethrow original error
        transaction_rollback(conn)
        rethrow(e)
    end
    return nothing
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
    datafile.assetversion = asset.versions[1]  # Use the base version of the asset
    # Add the datafile to the datastore
    register_datafile(store, datafile)
    @info "Registered data file for asset: $(asset_name) with version ID: $(datafile.assetversion.version_id)"
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
    datafile.assetversion = assetversion
    # Add the datafile to the datastore
    register_datafile(store.store, datafile)
    @info "Registered data file for asset version: $(assetversion.version_id)"
    # Return the DataFile object
    return datafile
end
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
    datafile.assetversion = assetversion # Associate the data file with the new version
    # Register the data file in the datastore
    register_datafile(store.store, datafile)
    @info "Registered new data file version for asset version: $(assetversion.version_id)"
    # Return the DataFile object
    return datafile
end
include("constants.jl")
include("utils.jl")
include("tredatabase.jl")
include("redcap.jl")

end #module
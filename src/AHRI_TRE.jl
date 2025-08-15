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
using Blake3Hash
using CSV
using XLSX
using FileIO
using Base64
using URIs
using UUIDs
using HTTP
using JSON3
using Downloads

export
    DataStore,
    Vocabulary, VocabularyItem,
    AbstractStudy, Study, Domain, Entity, EntityRelation,
    createdatastore, opendatastore, closedatastore,
    upsert_study!, upsert_domain!, get_domain, get_study,
    upsert_entity!, get_entity, upsert_entityrelation!, get_entityrelation, list_domainentities, list_domainrelations,
    datasetlakename,
    lookup_variables,
    get_namedkey, get_variable_id, get_variable, get_datasetname, updatevalues, insertdata, insertwithidentity,
    get_table, selectdataframe, prepareselectstatement, dataset_to_dataframe, dataset_to_arrow, dataset_to_csv,
    dataset_variables, dataset_column, savedataframe,
    register_redcap_datadictionary

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
struct VocabularyItem
    value::Int64
    code::String
    description::Union{String,Missing}
end
struct Vocabulary
    name::String
    description::String
    items::Vector{VocabularyItem}
end

abstract type AbstractStudy end

Base.@kwdef mutable struct Study <: AbstractStudy
    study_id::Union{UUID,Nothing} = nothing
    name::String = "study_name"
    description::String = "study description"
    external_id::String = "external_id"
    study_type_id::Integer = 1
end

Base.@kwdef mutable struct Domain
    domain_id::Union{Int,Nothing} = nothing
    name::String
    uri::Union{Missing,String} = missing
    description::Union{Missing,String} = missing
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

#endregion
"""
    createdatastore(store::DataStore; superuser::String="postgres", superpwd::String="", port::Integer=5432)

Create or replace a PostgreSQL database for the TRE datastore, including the datalake if specified.
This function creates a PostgreSQL database with the specified name and user credentials, and optionally creates a data lake using the DuckDb extension ducklake.
    store::DataStore: The DataStore object containing connection details for the datastore and datalake databases.
    superuser::String: The superuser name for PostgreSQL (default is "postgres").
    superpwd::String: The superuser password for PostgreSQL (default is empty).
    port::Integer: The port number for the PostgreSQL server (default is 5432
NB: ONLY USE THIS FUNCTION IN DEVELOPMENT OR TESTING ENVIRONMENTS,
    as it will drop the existing database, lake and all its contents.
"""
function createdatastore(store::DataStore; superuser::String="postgres", superpwd::String="", port::Integer=5432)
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
    upsert_study!(study::Study, store::DataStore)::Study

Create or update a study record. If a study with the same name already exists, it updates and returns the study.
Otherwise, it inserts a new row and returns the new study.
If `study.study_id` is `nothing`, it inserts a new study and lets PostgreSQL assign
"""
function upsert_study!(study::Study, store::DataStore)::Study
    db = store.store
    if study.study_id === nothing
        # Insert letting PostgreSQL assign uuidv7() default
        @info "Inserting new study: $(study.name)"
        sql = raw"""
        INSERT INTO studies (name, description, external_id, study_type_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (name) DO UPDATE
        SET description   = EXCLUDED.description,
            external_id   = EXCLUDED.external_id,
            study_type_id = EXCLUDED.study_type_id
        RETURNING study_id;
        """
        stmt = DBInterface.prepare(db, sql)
        df = DBInterface.execute(stmt, (study.name, study.description, study.external_id, study.study_type_id)) |> DataFrame
        study.study_id = UUID(df[1, :study_id])
    else
        @info "Updating existing study: $(study.name) with ID $(study.study_id)"
        updatevalues(db, "studies", "study_id", study.study_id, ["name", "description", "external_id", "study_type_id"],
            [study.name, study.description, study.external_id, study.study_type_id])
    end
    return study
end

"""
    get_study(db::DBInterface.Connection, name)

Return the `source_id` of source `name`, returns `missing` if source doesn't exist
"""
function get_study(store::DataStore, name::AbstractString)::Union{UUID,Nothing}
    study_id = get_namedkey(store.store, "studies", name, :study_id)
    @info "Study ID for $(name): $(study_id)"
    if ismissing(study_id)
        return nothing
    end
    return UUID(study_id)
end

"""
    upsert_domain!(domain::Domain, store::DataStore)::Domain

Create or update a domain record. If a domain with the same (name, uri) already
exists (treating NULL uri correctly), it updates and returns its domain_id.
Otherwise, it inserts a new row and returns the new domain_id.
"""
function upsert_domain!(domain::Domain, store::DataStore)::Domain
    db = store.store

    # 1) Does a matching domain already exist?
    sql_get = raw"""
        SELECT domain_id
        FROM domains
        WHERE name = $1
          AND ( ($2::text IS NULL AND uri IS NULL) OR uri = $2 )
        LIMIT 1;
    """
    stmt_get = DBInterface.prepare(db, sql_get)
    df = DBInterface.execute(stmt_get, (domain.name, domain.uri)) |> DataFrame

    if nrow(df) == 0
        @info "Insert new domain: $(domain.name) with URI '$(domain.uri)'"
        sql_ins = raw"""
            INSERT INTO domains (name, uri, description)
            VALUES ($1, $2, $3)
            RETURNING domain_id;
        """
        stmt_ins = DBInterface.prepare(db, sql_ins)
        ins = DBInterface.execute(stmt_ins, (domain.name, domain.uri, domain.description)) |> DataFrame
        domain.domain_id = ins[1, :domain_id]
    else
        @info "Update existing domain: $(domain.name) with URI '$(domain.uri)'"
        domain.domain_id = df[1, :domain_id]
        sql_upd = raw"""
            UPDATE domains
               SET description = $3
             WHERE domain_id   = $4
             RETURNING domain_id;
        """
        stmt_upd = DBInterface.prepare(db, sql_upd)
        DBInterface.execute(stmt_upd, (domain.name, domain.uri, domain.description, domain.domain_id))
    end

    return domain
end
# Get a domain by name (and optional URI), returns Domain or nothing
"""
    get_domain(store::DataStore, name::AbstractString; uri::Union{Nothing,String}=nothing)::Union{Domain,Nothing}

Return a Domain object by its name (and optional URI) in the specified DataStore.
"""
function get_domain(store::DataStore, name::AbstractString; uri::Union{Nothing,String}=nothing)::Union{Domain,Nothing}
    db = store.store
    if isnothing(uri)
        sql = raw"""
            SELECT domain_id, name, uri, description
              FROM domains
             WHERE name = $1
             LIMIT 1;
            """
    else
        sql = raw"""
            SELECT domain_id, name, uri, description
              FROM domains
             WHERE name = $1
               AND uri = $2 
             LIMIT 1;
            """
    end
    stmt = DBInterface.prepare(db, sql)
    if isnothing(uri)
        df = DBInterface.execute(stmt, (name,)) |> DataFrame
    else
        df = DBInterface.execute(stmt, (name, uri)) |> DataFrame
    end
    if nrow(df) == 0
        @info "No domain found with name: $(name) and URI: $(uri)"
        return nothing
    end
    row = df[1, :]
    return Domain(
        domain_id=row.domain_id,
        name=row.name,
        uri=coalesce(row.uri, missing),
        description=coalesce(row.description, missing)
    )
end

"""
    upsert_entity!(store::DataStore, e::Entity)::Entity

Create or update an entity record. If an entity with the same (domain_id, name) already exists, it updates and returns its entity_id.
"""
function upsert_entity!(e::Entity, store::DataStore)::Entity
    conn = store.store
    if e.entity_id === nothing
        @info "Inserting new entity: $(e.name) in domain ID $(e.domain_id)"
        sql = raw"""
            INSERT INTO entities (domain_id, name, description, ontology_namespace, ontology_class)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (domain_id, name) DO UPDATE
              SET description = EXCLUDED.description,
                  ontology_namespace = EXCLUDED.ontology_namespace,
                  ontology_class = EXCLUDED.ontology_class
            RETURNING entity_id, uuid;
        """
        df = DBInterface.execute(DBInterface.prepare(conn, sql),
            (e.domain_id, e.name, e.description, e.ontology_namespace, e.ontology_class)) |> DataFrame
        @info "Rows affected: $(nrow(df))"
        e.entity_id = df[1, :entity_id]
        e.uuid = UUID(df[1, :uuid])
    else
        sql = raw"""
            UPDATE entities
               SET domain_id = $2,
                   name = $3,
                   description = $4,
                   ontology_namespace = $5,
                   ontology_class = $6
             WHERE entity_id = $1
             RETURNING uuid;
        """
        df = DBInterface.execute(DBInterface.prepare(conn, sql),
            (e.entity_id, e.domain_id, e.name, e.description, e.ontology_namespace, e.ontology_class)) |> DataFrame
        e.uuid = UUID(df[1, :uuid])
    end
    return e
end

"""
    upsert_entityrelation!(store::DataStore, r::EntityRelation)::EntityRelation

Create or update an entity relation record. If a relation with the same (domain_id, name) already exists, it updates and returns its entityrelation_id.
"""
function upsert_entityrelation!(r::EntityRelation, store::DataStore)::EntityRelation
    conn = store.store
    if r.entityrelation_id === nothing
        sql = raw"""
            INSERT INTO entityrelations
              (entity_id_1, entity_id_2, domain_id, name, description, ontology_namespace, ontology_class)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (domain_id, name) DO UPDATE
              SET description = EXCLUDED.description,
                  ontology_namespace = EXCLUDED.ontology_namespace,
                  ontology_class = EXCLUDED.ontology_class
            RETURNING entityrelation_id, uuid;
        """
        df = DBInterface.execute(DBInterface.prepare(conn, sql),
            (r.entity_id_1, r.entity_id_2, r.domain_id, r.name, r.description, r.ontology_namespace, r.ontology_class)) |> DataFrame
        r.entityrelation_id = df[1, :entityrelation_id]
        r.uuid = UUID(df[1, :uuid])
    else
        sql = raw"""
            UPDATE entityrelations
               SET entity_id_1 = $2,
                   entity_id_2 = $3,
                   domain_id = $4,
                   name = $5,
                   description = $6,
                   ontology_namespace = $7,
                   ontology_class = $8
             WHERE entityrelation_id = $1
             RETURNING uuid;
        """
        df = DBInterface.execute(DBInterface.prepare(conn, sql),
            (r.entityrelation_id, r.entity_id_1, r.entity_id_2, r.domain_id, r.name, r.description, r.ontology_namespace, r.ontology_class)) |> DataFrame
        r.uuid = UUID(df[1, :uuid])
    end
    return r
end
"""
    get_entity(store::DataStore, domain_id::Int, name::String)::Union{Entity,Nothing}

Return an Entity object by its name in the specified domain.
"""
function get_entity(store::DataStore, domain_id::Int, name::String)::Union{Entity,Nothing}
    conn = store.store
    sql = raw"""
        SELECT entity_id, domain_id, uuid, name, description, ontology_namespace, ontology_class
          FROM entities
         WHERE domain_id = $1 AND name = $2
         LIMIT 1;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (domain_id, name)) |> DataFrame
    if nrow(df) == 0
        return nothing
    end
    row = df[1, :]
    return Entity(entity_id=row.entity_id,
        domain_id=row.domain_id,
        uuid=UUID(row.uuid),
        name=row.name,
        description=coalesce(row.description, missing),
        ontology_namespace=coalesce(row.ontology_namespace, missing),
        ontology_class=coalesce(row.ontology_class, missing))
end

"""
    get_entityrelation!(store::DataStore, domain_id::Int, name::String)::Union{EntityRelation,Nothing}

Return an EntityRelation object by its name in the specified domain.
"""
function get_entityrelation!(store::DataStore, domain_id::Int, name::String)::Union{EntityRelation,Nothing}
    conn = store.store
    sql = raw"""
        SELECT entityrelation_id, entity_id_1, entity_id_2, domain_id, uuid, name,
               description, ontology_namespace, ontology_class
          FROM entityrelations
         WHERE domain_id = $1 AND name = $2
         LIMIT 1;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (domain_id, name)) |> DataFrame
    if nrow(df) == 0
        return nothing
    end
    row = df[1, :]
    return EntityRelation(
        entityrelation_id=row.entityrelation_id,
        entity_id_1=row.entity_id_1,
        entity_id_2=row.entity_id_2,
        domain_id=row.domain_id,
        uuid=UUID(row.uuid),
        name=row.name,
        description=coalesce(row.description, missing),
        ontology_namespace=coalesce(row.ontology_namespace, missing),
        ontology_class=coalesce(row.ontology_class, missing)
    )
end
function list_domainentities(store::DataStore, domain_id::Int)::DataFrame
    conn = store.store
    sql = raw"""
        SELECT * FROM entities
        WHERE domain_id = $1
        ORDER BY name;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (domain_id,)) |> DataFrame
    return df
end
function list_domainrelations(store::DataStore, domain_id::Int)::DataFrame
    conn = store.store
    sql = raw"""
        SELECT * FROM entityrelations
        WHERE domain_id = $1
        ORDER BY name;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (domain_id,)) |> DataFrame
    return df
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
    datasetlakename(dataset_id::Integer):: String

Return the name of the dataset in the data lake, based on dataset_id
This is used to store the dataset in the data lake.
"""
datasetlakename(dataset_id::Integer)::String = "dataset_$dataset_id"
"""
    convert_missing_to_string!(df::DataFrame)

If the column type is Missing, convert the column eltype to Union{String, Missing}.
"""
function convert_missing_to_string!(df::DataFrame)
    for name in names(df)
        if eltype(df[!, name]) == Missing
            df[!, name] = convert(Vector{Union{String,Missing}}, df[!, name])
        end
    end
    return nothing
end
"""
    savedataframetolake(lake::DBInterface.Connection, df::AbstractDataFrame, name::String, description::String)

Save dataframe to data lake, convert columns of type Missing to Union{String, Missing} for DuckDB compatibility
NOTE: This function assumes that the ducklake metadatabase is attached as "tre_lake"
"""
function savedataframetolake(lake::DBInterface.Connection, df::AbstractDataFrame, name::String, description::String)
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
    get_variable(db::DBInterface.Connection, variable_id::Integer)

Returns the entry of variable with `variable_id`
"""
function get_variable(db::DBInterface.Connection, variable_id::Integer)
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
Export dataset 
"""

"""
    dataset_to_dataframe(db::DBInterface.Connection, dataset::Integer, lake::DBInterface.Connection = nothing)::AbstractDataFrame

Return a dataset with id `dataset` as a DataFrame in the wide format,
if lake is not specified, the data is read from the `data` table, otherwise from the data lake.
"""
function dataset_to_dataframe(db::DBInterface.Connection, dataset::Integer, lake::DBInterface.Connection=nothing)::AbstractDataFrame
    # Check if dataset is in the data lake
    inlake = !isnothing(lake)
    if inlake
        ds = selectdataframe(db, "datasets", ["dataset_id", "name", "in_lake"], ["dataset_id"], [dataset]) |> DataFrame
        @info ds
        if nrow(ds) == 0
            error("Dataset with id $dataset not found in database.")
        end
        inlake = ds[1, :in_lake] == 1
        if inlake
            @info "Dataset $dataset is in the data lake."
            sql = "SELECT * FROM tre_lake.$(datasetlakename(dataset));"
            df = DBInterface.execute(lake, sql) |> DataFrame
            return df
        end
    end
    sql = """
    SELECT
        d.row_id,
        v.name variable,
        d.value
    FROM data d
      JOIN datarows r ON d.row_id = r.row_id
      JOIN variables v ON d.variable_id = v.variable_id
    WHERE r.dataset_id = @dataset;
    """
    stmt = DBInterface.prepare(db, sql)
    long = DBInterface.execute(stmt, (dataset = dataset)) |> DataFrame
    return unstack(long, :row_id, :variable, :value)
end
"""
    dataset_variables(db::DBInterface.Connection, dataset)::AbstractDataFrame

Return the list of variables in a dataset
"""
function dataset_variables(db::DBInterface.Connection, dataset)::AbstractDataFrame
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
    dataset_column(db::DBInterface.Connection, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame

Return one column of data in a dataset (representing a variable)
"""
function dataset_column(db::DBInterface.Connection, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame
    sql = """
    SELECT
        d.row_id,
        d.value as $variable_name
    FROM data d
      JOIN datarows r ON d.row_id = r.row_id
    WHERE r.dataset_id = @dataset_id
      AND d.variable_id = @variable_id;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, (dataset_id=dataset_id, variable_id=variable_id)) |> DataFrame
end

"""
    get_datasetname(db::DBInterface.Connection, dataset)

Return dataset name, given the `dataset_id`
"""
function get_datasetname(db::DBInterface.Connection, dataset)
    sql = """
    SELECT
      name
    FROM datasets
    WHERE dataset_id = @dataset
    """
    stmt = DBInterface.prepare(db, sql)
    result = DBInterface.execute(stmt, (dataset = dataset))
    if isempty(result)
        return missing
    else
        df = DataFrame(result)
        name, ext = splitext(df[1, :name])
        return name # Return name without extension
    end
end
"""
    path_to_file_uri(path::AbstractString) -> String

Converts a local file path to a properly encoded file:// URI.
"""
function path_to_file_uri(path::AbstractString)
    # Normalize and get absolute path
    abs_path = string(normpath(abspath(path)))

    # Handle Windows paths (convert backslashes and add slash before drive letter)
    if Sys.iswindows()
        abs_path = replace(abs_path, "\\" => "/")
        if occursin(r"^[A-Za-z]:", abs_path)
            abs_path = "/" * abs_path  # e.g. /C:/Users/...
        end
    end

    # Percent-encode using URI constructor
    uri = URI("file://" * abs_path)
    return string(uri)
end

"""
    blake3_digest_hex(path::AbstractString) -> String

Computes the BLAKE3 digest of a file and returns it as a hexadecimal string.
"""
function blake3_digest_hex(path::AbstractString)
    open(path, "r") do io
        digest_bytes = blake3sum(io)
        return lowercase(bytes2hex(digest_bytes))
    end
end
"""
    verify_blake3_digest(path::AbstractString, expected_hex::AbstractString) -> Bool

Checks whether the BLAKE3 digest of the file matches the expected hex digest.
"""
function verify_blake3_digest(path::AbstractString, expected_hex::AbstractString)
    digest = blake3_digest_hex(path)
    return lowercase(digest) == lowercase(expected_hex)
end
"""
    ingest_redcap_project(api_url::AbstractString, api_token::AbstractString, study::Study, domain::Domain)

Retrieves the REDCap project metadata and add the project variables to the TRE datastore.
Downloads the REDCap project records in EAV format to a csv file and saves it to the data lake and creates an ingest transformation.
Transforms the csv file from EAV (long) format to wide format dataset and registers the dataset in the TRE datastore.
- `api_url`: The URL of the REDCap API endpoint.
- `api_token`: The API token for the REDCap project.
- `study`: The Study object to associate with the REDCap project. If `study.study_id` is `nothing`, it will be created.
- `domain`: The Domain object to associate with the REDCap project. If `domain.domain_id` is `nothing`, it will be created.
Returns nothing.
"""
function ingest_redcap_project(store::DataStore, api_url::AbstractString, api_token::AbstractString, study::Study, domain::Domain, vocabulary_prefix::String)
    # Ensure study and domain are set up
    study = upsert_study!(study, store)
    domain = upsert_domain!(domain, store)
    register_redcap_datadictionary(store; domain_id=domain.domain_id, redcap_url=api_url, redcap_token=api_token, vocabulary_prefix=vocabulary_prefix)
    # Download REDCap records in EAV format
    path = redcap_export_eav(api_url, api_token, fields=fields)

    # Save to data lake and register dataset
    savedataframetolake(DataStore().lake, dataset_to_dataframe(DataStore().store, 1), "redcap_project_$(study.name)", "REDCap project $(study.name)")

    return nothing
end

include("constants.jl")
include("tredatabase.jl")
include("redcap.jl")

end #module
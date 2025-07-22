module AHRI_TRE

using DataFrames
using SQLite
using DBInterface
using DuckDB
using ConfigEnv
using Dates
using Arrow
using DataStructures
using ODBC

using CSV
using XLSX
using FileIO
using Base64
using URIs

export
    Vocabulary, VocabularyItem,
    DataDocument, DocCSV, DocXLSX, DocPDF, read_data,
    AbstractStudy, rawSource, CHAMPSSource, COMSAMZSource, HEALSLSource, #Source structs
    AbstractIngest, sourceIngest, CHAMPSIngest, COMSAMZIngest, HEALSLIngest, userIngest, #Ingest structs
    ingest_study, datasetlakename,
    add_sites, add_instruments, add_protocols, add_ethics, add_study, add_domain,
    ingest_dictionary, ingest_deaths, ingest_data, save_dataset,
    read_variables, get_vocabulary, add_variables, add_vocabulary, lookup_variables,
    add_datarows, add_data_column, death_in_ingest, get_last_deathingest, link_instruments, link_deathrows,
    get_namedkey, get_variable_id, get_variable, get_valuetype, get_datasetname, updatevalue, insertdata, insertwithidentity,
    get_table, selectdataframe, prepareselectstatement, selectsourcesites, dataset_to_dataframe, dataset_to_arrow, dataset_to_csv,
    dataset_variables, dataset_column, savedataframe, createdatabase, opendatabase

#ODBC.bindtypes(x::Vector{UInt8}) = ODBC.API.SQL_C_BINARY, ODBC.API.SQL_LONGVARBINARY

"""
Structs for vocabulary
"""

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

"""
Struct for study-related information
"""
abstract type AbstractStudy end

Base.@kwdef mutable struct Study <: AbstractStudy

    name::String = "study_name"
    description::String = "study description"
    # Study type
    study_type_id::Integer = 1

end

abstract type AbstractIngest end

Base.@kwdef mutable struct StudyIngest <: AbstractIngest
    study::AbstractStudy = Study() # Study information
    ingested::DateTime = now() # Date of ingestion
    responsible::String = "0000-0000-0000-0000" # ORCID of the person responsible for ingestion
end

"""
    ingest_study(study::AbstractStudy)

TBW
"""
function ingest_study(study::AbstractStudy)
    db, lake = opendatabase(dbpath, dbname, sqlite)
    try
        DBInterface.transaction(db) do

            source_id = add_study(sudy.name, db)

        end

        return nothing
    finally
        DBInterface.close!(db)
        if !isnothing(lake)
            DBInterface.close!(lake)
        end
    end
end

"""
    add_study(study_name, db::DBInterface.Connection)

Add source `name` to the sources table, and returns the `source_id`
"""
function add_study(study_name::String, db::DBInterface.Connection)
    id = get_source(db, study_name)
    if ismissing(id)  # insert source
        id = insertwithidentity(db, "studies", ["name"], [study_name], "study_id")
    end
    return id
end

"""
    get_source(db::DBInterface.Connection, name)

Return the `source_id` of source `name`, returns `missing` if source doesn't exist
"""
function get_source(db::DBInterface.Connection, name)
    return get_namedkey(db, "studies", name, :study_id)
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
    save_dataset(db::DBInterface.Connection, dataset::AbstractDataFrame, name::String, description::String, unit_of_analysis_id::Integer,
    domain_id::Integer, transformation_id::Integer, ingestion_id::Integer, lake::DBInterface.Connection=nothing)::Integer

Insert dataframe containing dataset into TRE database and returns the dataset_id
if lake is not nothing, the dataset data is stored in the data lake.
"""
function save_dataset(db::DBInterface.Connection, dataset::AbstractDataFrame, name::String, description::String, unit_of_analysis_id::Integer,
    domain_id::Integer, transformation_id::Integer, ingestion_id::Integer, lake::DBInterface.Connection=nothing)::Integer

    variables = lookup_variables(db, names(dataset), domain_id)
    transform!(variables, [:variable_id, :value_type_id] => ByRow((x, y) -> Tuple([x, y])) => :variable_id_type)

    var_lookup = Dict{String,Tuple{Integer,Integer}}(zip(variables.name, variables.variable_id_type))

    # Add dataset entry to datasets table
    dataset_id = insertwithidentity(db, "datasets", ["name", "date_created", "description", "unit_of_analysis_id", "in_lake"],
        [name, isa(db, SQLite.DB) ? Dates.format(today(), "yyyy-mm-dd") : today(), description, unit_of_analysis_id, isnothing(lake) ? 0 : 1], "dataset_id")

    insertdata(db, "ingest_datasets", ["data_ingestion_id", "transformation_id", "dataset_id"],
        [ingestion_id, transformation_id, dataset_id])
    insertdata(db, "transformation_outputs", ["transformation_id", "dataset_id"],
        [transformation_id, dataset_id])

    savedataframe(db, select(variables, [] => Returns(dataset_id) => :dataset_id, :variable_id), "dataset_variables")

    # RDALake: row_id is still used to link data rows to the death table
    # Store datarows in datarows table and get row_ids 
    datarows = add_datarows(db, nrow(dataset), dataset_id)

    #prepare data for storage
    d = hcat(datarows, dataset; makeunique=true, copycols=false) #add the row_id to each row of data

    #RDALake: Complete dataset stored in data lake now
    if !isnothing(lake)
        @info "Saving dataset $name to data lake as '$(datasetlakename(dataset_id))'."
        savedataframetolake(lake, d, datasetlakename(dataset_id), name * ": " * description)
        @info "Dataset '$(datasetlakename(dataset_id))' saved to datalake."
    else
        #store whole column at a time
        for col in propertynames(dataset)
            variable_id, value_type = var_lookup[string(col)]
            coldata = select(d, :row_id, col => :value; copycols=false)
            add_data_column(db, variable_id, value_type, coldata)
        end
        @info "Dataset $name saved to database."
    end
    @info "Dataset $name ingested."
    return dataset_id
end
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
    add_datarows(db::SQLite.DB, nrow::Integer, dataset_id::Integer)

    Define data rows in the datarows table
"""
function add_datarows(db::DBInterface.Connection, nrow::Integer, dataset_id::Integer)
    stmt = prepareinsertstatement(db, "datarows", ["dataset_id"])
    #Create a row_id for every row in the dataset
    for i = 1:nrow
        DBInterface.execute(stmt, [dataset_id])
    end
    return selectdataframe(db, "datarows", ["row_id"], ["dataset_id"], [dataset_id]) |> DataFrame
end


"""
    add_data_column(db::SQLite.DB, variable_id, coldata)

Insert data for a column of the source dataset
"""
function add_data_column(db::SQLite.DB, variable_id, value_type, coldata)
    stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value"])
    if eltype(coldata.value) <: TimeType
        if value_type == TRE_TYPE_DATE
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x, "yyyy-mm-dd") : x) => :value)
        elseif value_type == TRE_TYPE_TIME
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x, "HH:MM:SS.sss") : x) => :value)
        elseif value_type == TRE_TYPE_DATETIME
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x, "yyyy-mm-ddTHH:MM:SS.sss") : x) => :value)
        else
            error("Variable $variable_id is not a date/time type. value_type = $value_type, eltype = $(eltype(coldata.value))")
        end
    end
    for row in eachrow(coldata)
        DBInterface.execute(stmt, [row.row_id, variable_id, row.value])
    end
    return nothing
end
"""
    add_data_column(db::ODBC.Connection, variable_id, value_type, coldata)

Insert data for a column of the source dataset
"""
function add_data_column(db::ODBC.Connection, variable_id, value_type, coldata)
    #println("Add data column variable_id = $variable_id, value_type = $value_type, eltype = $(eltype(coldata.value))")
    if value_type == TRE_TYPE_INTEGER
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_integer"])
    elseif value_type == TRE_TYPE_FLOAT
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_float"])
    elseif value_type == TRE_TYPE_STRING
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_string"])
        if eltype(coldata.value) <: Union{Missing,Number}
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? string(x) : x) => :value)
        elseif eltype(coldata.value) <: Union{Missing,TimeType}
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x, "yyyy-mm-dd") : x) => :value)
        else
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? String(x) : x) => :value)
        end
    elseif value_type == TRE_TYPE_DATE || value_type == TRE_TYPE_TIME || value_type == TRE_TYPE_DATETIME
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_datetime"])
    elseif value_type == TRE_TYPE_CATEGORY && eltype(coldata.value) <: Union{Missing,Integer}
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_integer"])
    elseif value_type == TRE_TYPE_CATEGORY && eltype(coldata.value) <: Union{Missing,AbstractString}
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_string"])
        transform!(coldata, :value => ByRow(x -> !ismissing(x) ? String(x) : x) => :value)
    else
        error("Variable $variable_id is not a valid type. value_type = $value_type, eltype = $(eltype(coldata.value))")
    end
    for row in eachrow(coldata)
        DBInterface.execute(stmt, [row.row_id, variable_id, row.value])
    end
    return nothing
end

"""
    link_deathrows(db::SQLite.DB, ingestion_id, dataset_id, death_identifier)

Insert records into `deathrows` table to link dataset `dataset_id` to `deaths` table. Limited to a specific ingest.
`death_identifier` is the variable in the dataset that corresponds to the `external_id` of the death.
"""
function link_entity_rows(db::SQLite.DB, ingestion_id, dataset_id, entity_identifier)

    sql = """
    INSERT OR IGNORE INTO death_rows (entity_id, row_id)
    SELECT
        d.death_id,
        data.row_id
    FROM deaths d
        JOIN data ON d.external_id = data.value
        JOIN datarows r ON data.row_id = r.row_id
    WHERE d.data_ingestion_id = @ingestion_id
    AND data.variable_id = @death_identifier
    AND r.dataset_id = @dataset_id
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (ingestion_id=ingestion_id, death_identifier=death_identifier, dataset_id=dataset_id))

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
    stmt = prepareselectstatement(db, table, ["*"], ["name"])
    df = DBInterface.execute(stmt, [key]) |> DataFrame
    if nrow(df) == 0
        return missing
    else
        return df[1, keycol]
    end
end



"""
    get_variable_id(db::SQLite.DB, domain, name)

Returns the `variable_id` of variable named `name` in domain with id `domain`
"""
function get_variable_id(db::SQLite.DB, domain, name)
    stmt = prepareselectstatement(db, "variables", ["variable_id"], ["domain_id", "name"])
    result = DBInterface.execute(stmt, [domain, name]) |> DataFrame
    if nrow(result) == 0
        return missing
    else
        return result[1, :variable_id]
    end
end


"""
    get_variable_id(db::DBInterface.Connection, domain, name)

    Returns the `variable_id` of variable named `name` in domain with id `domain`
"""
function get_variable_id(db::ODBC.Connection, domain, name)
    stmt = prepareselectstatement(db, "variables", ["variable_id"], ["domain_id", "name"])
    result = DBInterface.execute(stmt, [domain, name]; iterate_rows=true) |> DataFrame
    if nrow(result) == 0
        return missing
    else
        return result[1, :variable_id]
    end
end


"""
    get_variable(db::SQLite.DB, variable_id::Integer)

Returns the entry of variable with `variable_id`
"""
function get_variable(db::SQLite.DB, variable_id::Integer)
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
    dataset_to_dataframe(db::SQLite.DB, dataset::Integer, lake::DBInterface.Connection = nothing)::AbstractDataFrame

Return a dataset with id `dataset` as a DataFrame in the wide format,
if lake is not specified, the data is read from the `data` table, otherwise from the data lake.
"""
function dataset_to_dataframe(db::SQLite.DB, dataset::Integer, lake::DBInterface.Connection=nothing)::AbstractDataFrame
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
    dataset_variables(db::SQLite.DB, dataset)::AbstractDataFrame

Return the list of variables in a dataset
"""
function dataset_variables(db::SQLite.DB, dataset)::AbstractDataFrame
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
    dataset_column(db::SQLite.DB, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame

Return one column of data in a dataset (representing a variable)
"""
function dataset_column(db::SQLite.DB, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame
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
    get_datasetname(db::SQLite.DB, dataset)

Return dataset name, given the `dataset_id`
"""
function get_datasetname(db::SQLite.DB, dataset)
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

include("constants.jl")
include("tredatabase.jl")

end #module
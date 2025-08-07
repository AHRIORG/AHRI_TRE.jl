module AHRI_TRE

using DataFrames
using MySQL
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

export
    Vocabulary, VocabularyItem,
    DataDocument, DocCSV, DocXLSX, DocPDF, read_data,
    AbstractStudy, 
    datasetlakename,
    lookup_variables,
    get_namedkey, get_variable_id, get_variable, get_datasetname, updatevalue, insertdata, insertwithidentity,
    get_table, selectdataframe, prepareselectstatement, dataset_to_dataframe, dataset_to_arrow, dataset_to_csv,
    dataset_variables, dataset_column, savedataframe, createdatabase, opendatastore

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

"""
    get_study(db::DBInterface.Connection, name)

Return the `source_id` of source `name`, returns `missing` if source doesn't exist
"""
function get_study(db::DBInterface.Connection, name)
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
    stmt = prepareselectstatement(db, table, ["*"], ["name"])
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
include("constants.jl")
include("tredatabase.jl")

end #module
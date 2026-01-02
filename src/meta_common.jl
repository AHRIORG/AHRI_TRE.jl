# meta_common.jl - common SQL metadata extraction logic
# This file contains generic implementations and default fallbacks.

#region Database Flavor Types
"""
Abstract type for database flavors to enable multiple dispatch.
"""
abstract type DatabaseFlavour end

struct MSSQLFlavour <: DatabaseFlavour end
struct DuckDBFlavour <: DatabaseFlavour end
struct PostgreSQLFlavour <: DatabaseFlavour end
struct SQLiteFlavour <: DatabaseFlavour end
struct MySQLFlavour <: DatabaseFlavour end

"""
    parse_flavour(flavour::AbstractString) -> DatabaseFlavour

Convert a string database flavour name to the corresponding type for dispatch.
"""
function parse_flavour(flavour::AbstractString)::DatabaseFlavour
    flavour_upper = uppercase(flavour)
    if flavour_upper == "MSSQL"
        return MSSQLFlavour()
    elseif flavour_upper == "DUCKDB"
        return DuckDBFlavour()
    elseif flavour_upper == "POSTGRESQL"
        return PostgreSQLFlavour()
    elseif flavour_upper == "SQLITE"
        return SQLiteFlavour()
    elseif flavour_upper == "MYSQL"
        return MySQLFlavour()
    else
        error("Unsupported database flavour: $flavour. Supported: MSSQL, DuckDB, PostgreSQL, SQLite, MySQL")
    end
end
#endregion

#region Type Mapping
"""
    map_sql_type_to_tre(sql_type::AbstractString, flavour::DatabaseFlavour) -> Int

Map a SQL data type string to the corresponding TRE_TYPE_* constant.
Returns the TRE type ID.
"""
function map_sql_type_to_tre(sql_type::AbstractString, ::DatabaseFlavour)::Int
    type_lower = lowercase(sql_type)

    # Integer types
    if occursin(r"^(int|integer|smallint|bigint|tinyint|mediumint|int2|int4|int8|serial|bigserial|smallserial)$"i, type_lower)
        return TRE_TYPE_INTEGER
    end

    # Float types
    if occursin(r"^(float|double|real|decimal|numeric|money|smallmoney|float4|float8|double precision)$"i, type_lower) ||
       occursin(r"^(decimal|numeric)\s*\(\s*\d+\s*,\s*\d+\s*\)$"i, type_lower)
        return TRE_TYPE_FLOAT
    end

    # DateTime types (check before date to avoid substring match)
    if occursin(r"^(datetime|datetime2|datetimeoffset|timestamp|timestamptz|timestamp with(out)? time zone)$"i, type_lower)
        return TRE_TYPE_DATETIME
    end

    # Date types
    if occursin(r"^date$"i, type_lower)
        return TRE_TYPE_DATE
    end

    # Time types
    if occursin(r"^(time|timetz|time with(out)? time zone)$"i, type_lower)
        return TRE_TYPE_TIME
    end

    # String types (default)
    return TRE_TYPE_STRING
end
#endregion

#region Column Metadata Extraction
"""
    ColumnInfo

Internal structure to hold column metadata extracted from query results.
"""
struct ColumnInfo
    name::String
    data_type::String
    is_nullable::Bool
    table_name::Union{String,Nothing}
    schema_name::Union{String,Nothing}
end

"""
    get_query_columns(conn, sql::AbstractString, flavour::DatabaseFlavour) -> Vector{ColumnInfo}

Execute a query with LIMIT 0 or equivalent to get column metadata without fetching data.
"""
function get_query_columns(conn, sql::AbstractString, flavour::DatabaseFlavour)::Vector{ColumnInfo}
    # Wrap query with limit to avoid fetching actual data
    wrapped_sql = wrap_query_for_metadata(sql, flavour)
    result = DBInterface.execute(conn, wrapped_sql)
    df = DataFrame(result)

    columns = ColumnInfo[]
    for col_name in names(df)
        col_type = eltype(df[!, col_name])
        # Convert Julia type to SQL type string for mapping
        sql_type = julia_type_to_sql_string(col_type)
        push!(columns, ColumnInfo(String(col_name), sql_type, col_type >: Missing, nothing, nothing))
    end
    return columns
end

"""
    wrap_query_for_metadata(sql::AbstractString, flavour::DatabaseFlavour) -> String

Wrap the query to limit results for metadata extraction.
"""
function wrap_query_for_metadata(sql::AbstractString, ::DatabaseFlavour)::String
    # Generic approach: wrap with LIMIT 0
    return "SELECT * FROM ($sql) AS _meta_query LIMIT 0"
end

"""
    julia_type_to_sql_string(T::Type) -> String

Convert a Julia type to a SQL type string for type mapping.
"""
function julia_type_to_sql_string(T::Type)::String
    # Unwrap Union types (handle Missing)
    if T isa Union
        # Get the non-Missing type
        types = Base.uniontypes(T)
        for t in types
            if t !== Missing
                return julia_type_to_sql_string(t)
            end
        end
    end

    if T <: Integer
        return "INTEGER"
    elseif T <: AbstractFloat
        return "FLOAT"
    elseif T <: Dates.Date
        return "DATE"
    elseif T <: Dates.DateTime
        return "DATETIME"
    elseif T <: Dates.Time
        return "TIME"
    elseif T <: AbstractString
        return "VARCHAR"
    else
        return "VARCHAR"
    end
end
#endregion

#region Column Comments
"""
    get_column_comment(conn, table_name::AbstractString, column_name::AbstractString,
                       schema_name::Union{AbstractString,Nothing}, flavour::DatabaseFlavour) -> Union{String,Missing}

Get the comment/description for a column from the database metadata.
"""
function get_column_comment(conn, table_name::AbstractString, column_name::AbstractString,
    schema_name::Union{AbstractString,Nothing}, ::DatabaseFlavour)::Union{String,Missing}
    # Default: no comment support
    return missing
end
#endregion

#region Foreign Key Inspection
"""
    get_foreign_key_reference(conn, table_name::AbstractString, column_name::AbstractString,
                              flavour::DatabaseFlavour) -> Union{Nothing,Tuple{String,String}}

Return the referenced table/column for a foreign key on `table_name.column_name`,
or `nothing` if no foreign key relationship can be determined.

Database-specific overrides implement this for each flavour.
"""
function get_foreign_key_reference(conn, table_name::AbstractString, column_name::AbstractString,
    ::DatabaseFlavour)::Union{Nothing,Tuple{String,String}}
    return nothing
end
#endregion

#region Code Table Detection
"""
    is_code_table(conn, table_name::AbstractString, pk_column::AbstractString, flavour::DatabaseFlavour) -> Bool

Check if a table qualifies as a code/lookup table:
- Has less than 250 records
- Has a primary key column matching the specified column name
"""
function is_code_table(conn, table_name::AbstractString, pk_column::AbstractString, flavour::DatabaseFlavour)::Bool
    # Check if table exists and has the column as primary key
    if !table_has_primary_key(conn, table_name, pk_column, flavour)
        return false
    end

    # Check record count
    count_sql = "SELECT COUNT(*) AS cnt FROM $(quote_identifier(table_name, flavour))"
    try
        result = DBInterface.execute(conn, count_sql) |> DataFrame
        return result[1, :cnt] < 250
    catch
        return false
    end
end

"""
    table_has_primary_key(conn, table_name::AbstractString, column_name::AbstractString, flavour::DatabaseFlavour) -> Bool

Check if the specified column is the primary key of the table.
"""
function table_has_primary_key(conn, table_name::AbstractString, column_name::AbstractString, ::DatabaseFlavour)::Bool
    return false
end

"""
    quote_identifier(name::AbstractString, flavour::DatabaseFlavour) -> String

Quote an identifier appropriately for the database flavour.
"""
function quote_identifier(name::AbstractString, ::DatabaseFlavour)::String
    return "\"$name\""
end

"""
    get_code_table_vocabulary(conn, table_name::AbstractString, pk_column::AbstractString,
                               flavour::DatabaseFlavour) -> Vector{VocabularyItem}

Extract vocabulary items from a code/lookup table.
Uses the primary key as value, first string column as code, and description column if present.
"""
function get_code_table_vocabulary(conn, table_name::AbstractString, pk_column::AbstractString,
    flavour::DatabaseFlavour)::Vector{VocabularyItem}
    items = VocabularyItem[]

    # Get table columns to find first string column and description column
    columns = get_table_columns(conn, table_name, flavour)

    string_col = nothing
    desc_col = nothing

    for col in columns
        if col.name == pk_column
            continue
        end
        if map_sql_type_to_tre(col.data_type, flavour) == TRE_TYPE_STRING
            if isnothing(string_col)
                string_col = col.name
            end
            if lowercase(col.name) in ["description", "desc", "label", "name", "text"]
                desc_col = col.name
            end
        end
    end

    # Avoid selecting the same column twice (e.g., when the only string column is `name`).
    if !isnothing(desc_col) && desc_col == string_col
        desc_col = nothing
    end

    if isnothing(string_col)
        return items
    end

    # If no explicit description column found, check if there's a second string column
    if isnothing(desc_col)
        for col in columns
            if col.name == pk_column || col.name == string_col
                continue
            end
            if map_sql_type_to_tre(col.data_type, flavour) == TRE_TYPE_STRING
                desc_col = col.name
                break
            end
        end
    end

    # Query the code table
    pk_quoted = quote_identifier(pk_column, flavour)
    str_quoted = quote_identifier(string_col, flavour)
    table_quoted = quote_identifier(table_name, flavour)

    if !isnothing(desc_col)
        desc_quoted = quote_identifier(desc_col, flavour)
        sql = "SELECT $pk_quoted, $str_quoted, $desc_quoted FROM $table_quoted ORDER BY $pk_quoted"
    else
        sql = "SELECT $pk_quoted, $str_quoted FROM $table_quoted ORDER BY $pk_quoted"
    end

    try
        result = DBInterface.execute(conn, sql) |> DataFrame
        for row in eachrow(result)
            value = Int64(row[Symbol(pk_column)])
            code = String(row[Symbol(string_col)])
            desc = if !isnothing(desc_col) && hasproperty(row, Symbol(desc_col)) && !ismissing(row[Symbol(desc_col)])
                String(row[Symbol(desc_col)])
            else
                missing
            end
            push!(items, VocabularyItem(
                vocabulary_item_id=nothing,
                vocabulary_id=0,  # Will be set when vocabulary is saved
                value=value,
                code=code,
                description=desc
            ))
        end
    catch e
        @warn "Failed to get code table vocabulary from $table_name: $e"
    end

    return items
end

function build_code_table_vocabulary_for_column(conn, column_name::AbstractString, ref_table::AbstractString,
    ref_column::AbstractString, flavour::DatabaseFlavour)::Union{Vocabulary,Nothing}
    if !is_code_table(conn, ref_table, ref_column, flavour)
        return nothing
    end
    code_items = get_code_table_vocabulary(conn, ref_table, ref_column, flavour)
    if isempty(code_items)
        return nothing
    end
    return Vocabulary(
        vocabulary_id=nothing,
        name="$(column_name)_codes",
        description="Code table values from $ref_table",
        items=code_items
    )
end

function find_code_table_by_column_name(conn, column_name::AbstractString,
    ::DatabaseFlavour, source_table::Union{Nothing,String})::Union{Nothing,Tuple{String,String}}
    return nothing
end

"""
    get_table_columns(conn, table_name::AbstractString, flavour::DatabaseFlavour) -> Vector{ColumnInfo}

Get column information for a table.
"""
function get_table_columns(conn, table_name::AbstractString, ::DatabaseFlavour)::Vector{ColumnInfo}
    return ColumnInfo[]
end
#endregion

#region Enum and Constraint Detection
"""
    get_enum_values(conn, type_name::AbstractString, flavour::DatabaseFlavour) -> Vector{VocabularyItem}

Get the values for an enum type.
"""
function get_enum_values(conn, type_name::AbstractString, ::DatabaseFlavour)::Vector{VocabularyItem}
    return VocabularyItem[]
end

"""
    is_enum_type(conn, type_name::AbstractString, flavour::DatabaseFlavour) -> Bool

Check if a type is an enum type.
"""
function is_enum_type(conn, type_name::AbstractString, ::DatabaseFlavour)::Bool
    return false
end

"""
    get_check_constraint_values(conn, table_name::AbstractString, column_name::AbstractString,
                                 flavour::DatabaseFlavour) -> Vector{VocabularyItem}

Extract allowed values from CHECK constraints on a column.
"""
function get_check_constraint_values(conn, table_name::AbstractString, column_name::AbstractString,
    ::DatabaseFlavour)::Vector{VocabularyItem}
    return VocabularyItem[]
end

"""
    parse_check_constraint_values(constraint_def::AbstractString, column_name::AbstractString) -> Vector{VocabularyItem}

Parse a CHECK constraint definition to extract allowed string values.
"""
function parse_check_constraint_values(constraint_def::AbstractString, column_name::AbstractString)::Vector{VocabularyItem}
    items = VocabularyItem[]

    # Pattern 1: column IN ('val1', 'val2', ...)
    pattern = Regex("$column_name\\s*(?:=\\s*ANY\\s*\\(ARRAY\\[|IN\\s*\\()([^)\\]]+)[)\\]]", "i")
    m = match(pattern, constraint_def)
    if !isnothing(m)
        return parse_in_list_values(m.captures[1])
    end

    # Pattern 2: column = 'val1' OR column = 'val2' ...
    values = String[]
    for m in eachmatch(Regex("$column_name\\s*=\\s*'([^']*)'", "i"), constraint_def)
        push!(values, String(m.captures[1]))
    end
    if !isempty(values)
        for (i, val) in enumerate(unique(values))
            push!(items, VocabularyItem(
                vocabulary_item_id=nothing,
                vocabulary_id=0,
                value=Int64(i),
                code=val,
                description=missing
            ))
        end
    end

    return items
end

"""
    parse_in_list_values(values_str::AbstractString) -> Vector{VocabularyItem}

Parse a comma-separated list of quoted values into VocabularyItems.
"""
function parse_in_list_values(values_str::AbstractString)::Vector{VocabularyItem}
    items = VocabularyItem[]
    # Match quoted strings
    for (i, m) in enumerate(eachmatch(r"'([^']*)'", values_str))
        push!(items, VocabularyItem(
            vocabulary_item_id=nothing,
            vocabulary_id=0,
            value=Int64(i),
            code=String(m.captures[1]),
            description=missing
        ))
    end
    return items
end
#endregion

#region Table and Type Helpers
"""
    table_exists(conn, table_name::AbstractString, flavour::DatabaseFlavour) -> Bool

Check if a table exists in the database.
"""
function table_exists(conn, table_name::AbstractString, ::DatabaseFlavour)::Bool
    return false
end

#region Column Type Information from Database
"""
    get_column_type_info(conn, sql::AbstractString, flavour::DatabaseFlavour) -> Vector{Tuple{String,String,Union{String,Nothing}}}

Get detailed column type information from the database for a query.
Returns vector of (column_name, data_type, table_name) tuples.
"""
function get_column_type_info(conn, sql::AbstractString, flavour::DatabaseFlavour)
    columns = get_query_columns(conn, sql, flavour)
    return [(c.name, c.data_type, c.table_name) for c in columns]
end

"""
    get_original_column_type(conn, table_name::AbstractString, column_name::AbstractString,
                              flavour::DatabaseFlavour) -> Union{String,Nothing}

Get the original column type from the database schema.
"""
function get_original_column_type(conn, table_name::AbstractString, column_name::AbstractString,
    ::DatabaseFlavour)::Union{String,Nothing}
    return nothing
end
#endregion

#region Main Function
"""
    sql_meta(conn, sql::AbstractString, domain_id::Int, flavour::AbstractString) -> Vector{Variable}

Extract variable metadata from the columns of an SQL SELECT statement.

# Arguments
- `conn`: Database connection (DBInterface.Connection)
- `sql`: SQL SELECT statement
- `domain_id`: Domain ID to assign to variables
- `flavour`: Database flavour string ("MSSQL", "DuckDB", "PostgreSQL", "SQLite", or "MySQL")

# Returns
Vector of Variable structures, one for each column in the query result.

The function:
1. Executes the query with LIMIT 0 to get column metadata
2. Maps SQL types to TRE_TYPE_* constants
3. Detects CATEGORY types for:
   - ENUM columns
   - Integer columns referencing code tables (< 250 records)
   - String columns with CHECK constraints listing allowed values
4. Populates Vocabulary for CATEGORY types
5. Retrieves column comments as descriptions where available
"""
function sql_meta(conn, sql::AbstractString, domain_id::Int, flavour::AbstractString)::Vector{Variable}
    db_flavour = parse_flavour(flavour)
    return sql_meta(conn, sql, domain_id, db_flavour)
end

"""
    sql_meta(conn, sql::AbstractString, domain_id::Int, flavour::DatabaseFlavour) -> Vector{Variable}

Internal implementation using typed database flavour for dispatch.
"""
function sql_meta(conn, sql::AbstractString, domain_id::Int, flavour::DatabaseFlavour)::Vector{Variable}
    variables = Variable[]

    # Get column information from query
    columns = get_query_columns(conn, sql, flavour)

    # Try to extract table information from SQL for column comments and constraints
    # This is a best-effort extraction
    table_name = extract_table_from_sql(sql)

    for col in columns
        column_name = col.name
        sql_type = col.data_type
        col_table = something(col.table_name, table_name)

        # Get original type from database if we have a table reference
        original_type = if !isnothing(col_table)
            get_original_column_type(conn, col_table, column_name, flavour)
        else
            nothing
        end
        actual_type = something(original_type, sql_type)

        # Determine value type
        value_type_id = map_sql_type_to_tre(actual_type, flavour)

        # Check for category types
        vocabulary = missing

        # 1. Check for ENUM types
        if is_enum_type(conn, actual_type, flavour)
            value_type_id = TRE_TYPE_CATEGORY
            enum_items = get_enum_values(conn, actual_type, flavour)
            if !isempty(enum_items)
                vocabulary = Vocabulary(
                    vocabulary_id=nothing,
                    name="$(column_name)_enum",
                    description="Enum values for $column_name",
                    items=enum_items
                )
            end
        end

        # 2. Check for code table reference via FOREIGN KEY (integer FK -> small PK code table)
        if value_type_id == TRE_TYPE_INTEGER && !isnothing(col_table)
            fk_ref = get_foreign_key_reference(conn, col_table, column_name, flavour)
            if !isnothing(fk_ref)
                ref_table, ref_column = fk_ref
                code_vocab = build_code_table_vocabulary_for_column(conn, column_name, ref_table, ref_column, flavour)
                if !isnothing(code_vocab)
                    value_type_id = TRE_TYPE_CATEGORY
                    vocabulary = code_vocab
                end
            end
        end

        if value_type_id == TRE_TYPE_INTEGER && vocabulary === missing
            direct_ref = find_code_table_by_column_name(conn, column_name, flavour, col_table)
            if !isnothing(direct_ref)
                ref_table, ref_column = direct_ref
                code_vocab = build_code_table_vocabulary_for_column(conn, column_name, ref_table, ref_column, flavour)
                if !isnothing(code_vocab)
                    value_type_id = TRE_TYPE_CATEGORY
                    vocabulary = code_vocab
                end
            end
        end

        # 3. Check for CHECK constraints on string columns
        if value_type_id == TRE_TYPE_STRING && !isnothing(col_table)
            check_items = get_check_constraint_values(conn, col_table, column_name, flavour)
            if !isempty(check_items)
                value_type_id = TRE_TYPE_CATEGORY
                vocabulary = Vocabulary(
                    vocabulary_id=nothing,
                    name="$(column_name)_allowed",
                    description="Allowed values for $column_name",
                    items=check_items
                )
            end
        end

        # Get column description from comment
        description = if !isnothing(col_table)
            get_column_comment(conn, col_table, column_name, col.schema_name, flavour)
        else
            missing
        end

        # Create Variable
        var = Variable(
            variable_id=nothing,
            domain_id=domain_id,
            name=column_name,
            value_type_id=value_type_id,
            value_format=missing,
            vocabulary_id=missing,
            keyrole="none",
            description=description,
            ontology_namespace=missing,
            ontology_class=missing,
            vocabulary=vocabulary
        )

        push!(variables, var)
    end

    return variables
end

"""
    extract_table_from_sql(sql::AbstractString) -> Union{String, Nothing}

Attempt to extract the main table name from a simple SQL SELECT statement.
This is a best-effort extraction and may not work for complex queries.
"""
function extract_table_from_sql(sql::AbstractString)::Union{String,Nothing}
    # Simple pattern: SELECT ... FROM table_name ...
    # Match the first table after FROM
    m = match(r"FROM\s+([\"'`\[]?\w+[\"'`\]]?)(?:\s|$|,|;)"i, sql)
    if !isnothing(m)
        table = m.captures[1]
        # Remove quotes/brackets
        return strip(table, ['\"', '\'', '`', '[', ']'])
    end
    return nothing
end
#endregion
#region sql to dataset
"""
    sql_to_dataset(store::DataStore, study::Study, domain::Domain, dataset_name::String, conn, db_flavour::AbstractString, sql::String;
    description::String, replace::Bool=false, new_version::Union{VersionNumber,Nothing}=nothing)::DataSet

Transform the result of an SQL query into a DataSet stored in the TRE DataStore.
# Arguments
- `store`: DataStore object
- `study`: Study object to associate the dataset with
- `domain`: Domain object for the dataset
- `dataset_name`: Name of the dataset to create
- `conn`: Database connection to execute the SQL query
- `db_flavour`: Database flavour type for metadata extraction
- `sql`: SQL query string to execute
- `description`: Description for the dataset asset/version
- `replace`: If true, replace existing dataset with the same name by creating a new version
- `new_version`: Optional Version object for the new dataset version
# Returns
The created DataSet object, or `nothing` on failure.
"""
function sql_to_dataset(store::DataStore, study::Study, domain::Domain, dataset_name::String, conn::DBInterface.Connection, db_flavour::AbstractString, sql::String;
    description::String, replace::Bool=false, new_version::Union{VersionNumber,Nothing}=nothing)::DataSet
    return sql_to_dataset(store, study, domain, dataset_name, conn, parse_flavour(db_flavour), sql;
        description=description, replace=replace, new_version=new_version)
end

function sql_to_dataset(store::DataStore, study::Study, domain::Domain, dataset_name::String, conn::DBInterface.Connection, db_flavour::DatabaseFlavour, sql::String;
    description::String, replace::Bool=false, new_version::Union{VersionNumber,Nothing}=nothing)::DataSet
    @info "Saving sql query to datastore"
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
        dataset_meta = sql_meta(conn, sql, domain.domain_id, db_flavour)  # domain_id=0 for now
        @info "Extracted $(length(dataset_meta)) variables from SQL query"
        transaction_begin(store)
        if isnothing(existing_asset)
            # Create a new dataset asset & assetversion
            existing_asset = create_asset(store, study, dataset_name, "dataset", description)
        else
            # Create a new version of the existing dataset asset
            save_asset_version!(store, existing_asset, description, new_version)
        end
        dataset = DataSet(version=get_latest_version(existing_asset))
        register_dataset(store, dataset)
        @info "Registered dataset with name: $(dataset.version.asset.name)"
        dataset.variables = dataset_meta
        save_dataset_variables!(store, dataset)
        @info "Saved $(length(dataset.variables)) variables to dataset version $(dataset.version.version_id)"
        # Execute the SQL and save data in the this dataset in the datasore (using the ducklake)
        load_query(store, dataset, conn, sql)
        @info "Loaded data into dataset $(dataset.version.asset.name) version $(dataset.version.version_id)"
        #Create a transformation to record this ingestion. Use the caller's script
        #location as the git root hint so we pick up the user's repo instead of the
        #installed package directory.
        caller_path = caller_file_runtime(1)
        commit = if isnothing(caller_path)
            git_commit_info()
        else
            git_commit_info(dirname(caller_path); script_path=caller_path)
        end
        transformation = create_transformation("ingest","Ingested sql \n$sql\n to dataset $(dataset.version.asset.name) version $(dataset.version.version_id)";
            repository_url=commit.repo_url,
            commit_hash=commit.commit,
            file_path=commit.script_relpath
        )
        # Save the transformation to the datastore
        add_transformation!(store, transformation)
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

#endregion
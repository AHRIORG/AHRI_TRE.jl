# sql_meta.jl - Extract variable metadata from SQL query results
# This file provides the sql_meta function and database-flavor-specific implementations

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

# PostgreSQL-specific type mapping
function map_sql_type_to_tre(sql_type::AbstractString, ::PostgreSQLFlavour)::Int
    type_lower = lowercase(sql_type)
    
    # Check for enum (user-defined types in PostgreSQL are often enums)
    # Enums will be handled separately, but we check for common patterns
    
    # Integer types
    if occursin(r"^(int|integer|smallint|bigint|int2|int4|int8|serial|bigserial|smallserial|oid)$"i, type_lower)
        return TRE_TYPE_INTEGER
    end
    
    # Float types
    if occursin(r"^(float4|float8|real|double precision|numeric|decimal|money)$"i, type_lower) ||
       occursin(r"^(decimal|numeric)\s*\(\s*\d+\s*,\s*\d+\s*\)$"i, type_lower)
        return TRE_TYPE_FLOAT
    end
    
    # DateTime types (check before date to avoid substring match)
    if occursin(r"^(timestamp|timestamptz|timestamp with(out)? time zone)$"i, type_lower)
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

# MSSQL-specific type mapping
# Handles system_type_name format from dm_exec_describe_first_result_set 
# which includes precision/scale like "varchar(50)", "decimal(18,2)", "datetime2(7)"
function map_sql_type_to_tre(sql_type::AbstractString, ::MSSQLFlavour)::Int
    type_lower = lowercase(sql_type)
    
    # Strip parenthetical precision/scale for matching: "varchar(50)" -> "varchar"
    base_type = replace(type_lower, r"\s*\([^)]*\)\s*$" => "")
    
    # Integer types
    if base_type in ["int", "integer", "smallint", "bigint", "tinyint", "bit"]
        return TRE_TYPE_INTEGER
    end
    
    # Float types
    if base_type in ["float", "real", "decimal", "numeric", "money", "smallmoney"]
        return TRE_TYPE_FLOAT
    end
    
    # DateTime types (check before date to avoid substring match)
    if base_type in ["datetime", "datetime2", "datetimeoffset", "smalldatetime"]
        return TRE_TYPE_DATETIME
    end
    
    # Date types
    if base_type == "date"
        return TRE_TYPE_DATE
    end
    
    # Time types
    if base_type == "time"
        return TRE_TYPE_TIME
    end
    
    # String types (default)
    return TRE_TYPE_STRING
end

# MySQL-specific type mapping
function map_sql_type_to_tre(sql_type::AbstractString, ::MySQLFlavour)::Int
    type_lower = lowercase(sql_type)
    
    # Check for ENUM type
    if startswith(type_lower, "enum")
        return TRE_TYPE_CATEGORY
    end
    
    # Integer types
    if occursin(r"^(int|integer|smallint|bigint|tinyint|mediumint)$"i, type_lower)
        return TRE_TYPE_INTEGER
    end
    
    # Float types
    if occursin(r"^(float|double|real|decimal|numeric)$"i, type_lower) ||
       occursin(r"^(decimal|numeric)\s*\(\s*\d+\s*,\s*\d+\s*\)$"i, type_lower)
        return TRE_TYPE_FLOAT
    end
    
    # DateTime types (check before date to avoid substring match)
    if occursin(r"^(datetime|timestamp)$"i, type_lower)
        return TRE_TYPE_DATETIME
    end
    
    # Date types
    if occursin(r"^date$"i, type_lower)
        return TRE_TYPE_DATE
    end
    
    # Time types
    if occursin(r"^time$"i, type_lower)
        return TRE_TYPE_TIME
    end
    
    # String types (default)
    return TRE_TYPE_STRING
end

# DuckDB-specific type mapping
function map_sql_type_to_tre(sql_type::AbstractString, ::DuckDBFlavour)::Int
    type_lower = lowercase(sql_type)
    
    # Check for ENUM type
    if startswith(type_lower, "enum")
        return TRE_TYPE_CATEGORY
    end
    
    # Integer types
    if occursin(r"^(integer|int|int4|int8|bigint|smallint|tinyint|hugeint|ubigint|uinteger|usmallint|utinyint|int2)$"i, type_lower)
        return TRE_TYPE_INTEGER
    end
    
    # Float types
    if occursin(r"^(float|double|real|decimal|numeric|float4|float8)$"i, type_lower) ||
       occursin(r"^(decimal|numeric)\s*\(\s*\d+\s*,\s*\d+\s*\)$"i, type_lower)
        return TRE_TYPE_FLOAT
    end
    
    # DateTime types (check before date to avoid substring match)
    if occursin(r"^(timestamp|timestamptz|timestamp with(out)? time zone)$"i, type_lower)
        return TRE_TYPE_DATETIME
    end
    
    # Date types
    if occursin(r"^date$"i, type_lower)
        return TRE_TYPE_DATE
    end
    
    # Time types
    if occursin(r"^time$"i, type_lower)
        return TRE_TYPE_TIME
    end
    
    # String types (default)
    return TRE_TYPE_STRING
end

# SQLite-specific type mapping
function map_sql_type_to_tre(sql_type::AbstractString, ::SQLiteFlavour)::Int
    type_lower = lowercase(sql_type)
    
    # SQLite has dynamic typing with type affinity
    # INTEGER affinity
    if occursin(r"int"i, type_lower)
        return TRE_TYPE_INTEGER
    end
    
    # REAL affinity
    if occursin(r"(real|floa|doub)"i, type_lower)
        return TRE_TYPE_FLOAT
    end
    
    # Date/Time (SQLite stores as TEXT, INTEGER, or REAL)
    # Check datetime/timestamp before date to avoid substring match
    if occursin(r"(datetime|timestamp)"i, type_lower)
        return TRE_TYPE_DATETIME
    end
    if occursin(r"^date$"i, type_lower)
        return TRE_TYPE_DATE
    end
    if occursin(r"^time$"i, type_lower)
        return TRE_TYPE_TIME
    end
    
    # TEXT/BLOB affinity (default to string)
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
    for (i, col_name) in enumerate(names(df))
        col_type = eltype(df[!, col_name])
        # Convert Julia type to SQL type string for mapping
        sql_type = julia_type_to_sql_string(col_type)
        push!(columns, ColumnInfo(col_name, sql_type, col_type >: Missing, nothing, nothing))
    end
    return columns
end

"""
    get_query_columns(conn, sql::AbstractString, ::MSSQLFlavour) -> Vector{ColumnInfo}

MSSQL-specific implementation using sys.dm_exec_describe_first_result_set to get
rich column metadata including source table and schema information.
"""
function get_query_columns(conn, sql::AbstractString, ::MSSQLFlavour)::Vector{ColumnInfo}
    # Use sys.dm_exec_describe_first_result_set to get detailed column metadata
    # This function returns: name, system_type_name, source_table, source_column, 
    # source_schema, is_nullable, and more
    meta_sql = """
    SELECT 
        name,
        system_type_name,
        is_nullable,
        source_table,
        source_schema,
        source_column
    FROM sys.dm_exec_describe_first_result_set(?, NULL, 1)
    WHERE is_hidden = 0
    ORDER BY column_ordinal
    """
    
    columns = ColumnInfo[]
    try
        result = DBInterface.execute(conn, meta_sql, [sql]) |> DataFrame
        for row in eachrow(result)
            col_name = String(row[:name])
            # system_type_name includes precision/scale like "varchar(50)" or "decimal(18,2)"
            data_type = String(row[:system_type_name])
            is_nullable = row[:is_nullable] == true || row[:is_nullable] == 1
            
            # Source table and schema may be NULL if column is computed/derived
            table_name = if !ismissing(row[:source_table]) && !isnothing(row[:source_table])
                String(row[:source_table])
            else
                nothing
            end
            schema_name = if !ismissing(row[:source_schema]) && !isnothing(row[:source_schema])
                String(row[:source_schema])
            else
                nothing
            end
            
            push!(columns, ColumnInfo(col_name, data_type, is_nullable, table_name, schema_name))
        end
    catch e
        # Fallback to generic approach if dm_exec_describe_first_result_set fails
        @warn "MSSQL dm_exec_describe_first_result_set failed, falling back to generic approach: $e"
        wrapped_sql = "SELECT TOP 0 * FROM ($sql) AS _meta_query"
        result = DBInterface.execute(conn, wrapped_sql)
        df = DataFrame(result)
        
        for (i, col_name) in enumerate(names(df))
            col_type = eltype(df[!, col_name])
            sql_type = julia_type_to_sql_string(col_type)
            push!(columns, ColumnInfo(col_name, sql_type, col_type >: Missing, nothing, nothing))
        end
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

function wrap_query_for_metadata(sql::AbstractString, ::MSSQLFlavour)::String
    # MSSQL uses TOP instead of LIMIT
    return "SELECT TOP 0 * FROM ($sql) AS _meta_query"
end

function wrap_query_for_metadata(sql::AbstractString, ::SQLiteFlavour)::String
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

function get_column_comment(conn, table_name::AbstractString, column_name::AbstractString,
                           schema_name::Union{AbstractString,Nothing}, ::PostgreSQLFlavour)::Union{String,Missing}
    schema = isnothing(schema_name) ? "public" : schema_name
    sql = """
    SELECT col_description(
        (SELECT oid FROM pg_class WHERE relname = \$1 AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = \$2)),
        (SELECT ordinal_position FROM information_schema.columns WHERE table_schema = \$2 AND table_name = \$1 AND column_name = \$3)
    ) AS comment
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, schema, column_name]) |> DataFrame
        if nrow(result) > 0 && !ismissing(result[1, :comment]) && !isnothing(result[1, :comment])
            return String(result[1, :comment])
        end
    catch
        # Column comment not available
    end
    return missing
end

function get_column_comment(conn, table_name::AbstractString, column_name::AbstractString,
                           schema_name::Union{AbstractString,Nothing}, ::MSSQLFlavour)::Union{String,Missing}
    schema = isnothing(schema_name) ? "dbo" : schema_name
    sql = """
    SELECT CAST(ep.value AS VARCHAR(MAX)) AS comment
    FROM sys.extended_properties ep
    JOIN sys.columns c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = ? AND c.name = ? AND s.name = ? AND ep.name = 'MS_Description'
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name, schema]) |> DataFrame
        if nrow(result) > 0 && !ismissing(result[1, :comment])
            return String(result[1, :comment])
        end
    catch
        # Column comment not available
    end
    return missing
end

function get_column_comment(conn, table_name::AbstractString, column_name::AbstractString,
                           schema_name::Union{AbstractString,Nothing}, ::MySQLFlavour)::Union{String,Missing}
    sql = """
    SELECT COLUMN_COMMENT AS comment
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ? AND COLUMN_NAME = ?
    """
    params = [table_name, column_name]
    if !isnothing(schema_name)
        sql *= " AND TABLE_SCHEMA = ?"
        push!(params, schema_name)
    end
    try
        result = DBInterface.execute(conn, sql, params) |> DataFrame
        if nrow(result) > 0 && !ismissing(result[1, :comment]) && !isempty(result[1, :comment])
            return String(result[1, :comment])
        end
    catch
        # Column comment not available
    end
    return missing
end

function get_column_comment(conn, table_name::AbstractString, column_name::AbstractString,
                           schema_name::Union{AbstractString,Nothing}, ::DuckDBFlavour)::Union{String,Missing}
    # DuckDB supports comments via COMMENT ON syntax and can be queried
    sql = """
    SELECT comment FROM duckdb_columns()
    WHERE table_name = ? AND column_name = ?
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        if nrow(result) > 0 && !ismissing(result[1, :comment]) && !isnothing(result[1, :comment])
            return String(result[1, :comment])
        end
    catch
        # Column comment not available
    end
    return missing
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

function table_has_primary_key(conn, table_name::AbstractString, column_name::AbstractString, ::PostgreSQLFlavour)::Bool
    sql = """
    SELECT COUNT(*) as cnt
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
      ON tc.constraint_name = kcu.constraint_name 
      AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_name = \$1
      AND kcu.column_name = \$2
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
end

function table_has_primary_key(conn, table_name::AbstractString, column_name::AbstractString, ::MSSQLFlavour)::Bool
    sql = """
    SELECT COUNT(*) as cnt
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
      ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
      AND tc.TABLE_NAME = ?
      AND kcu.COLUMN_NAME = ?
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
end

function table_has_primary_key(conn, table_name::AbstractString, column_name::AbstractString, ::MySQLFlavour)::Bool
    sql = """
    SELECT COUNT(*) as cnt
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
      ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
      AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
      AND tc.TABLE_NAME = ?
      AND kcu.COLUMN_NAME = ?
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
end

function table_has_primary_key(conn, table_name::AbstractString, column_name::AbstractString, ::DuckDBFlavour)::Bool
    # DuckDB: check via duckdb_constraints()
    sql = """
    SELECT COUNT(*) as cnt
    FROM duckdb_constraints()
    WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'
    """
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        if result[1, :cnt] > 0
            # Check if the column is part of the primary key
            sql2 = """
            SELECT constraint_column_names FROM duckdb_constraints()
            WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'
            """
            result2 = DBInterface.execute(conn, sql2, [table_name]) |> DataFrame
            if nrow(result2) > 0
                # constraint_column_names is an array
                col_names = result2[1, :constraint_column_names]
                return column_name in col_names
            end
        end
    catch
        return false
    end
    return false
end

function table_has_primary_key(conn, table_name::AbstractString, column_name::AbstractString, ::SQLiteFlavour)::Bool
    # SQLite: check pragma table_info
    sql = "PRAGMA table_info('$table_name')"
    try
        result = DBInterface.execute(conn, sql) |> DataFrame
        for row in eachrow(result)
            if row[:name] == column_name && row[:pk] > 0
                return true
            end
        end
    catch
        return false
    end
    return false
end

"""
    quote_identifier(name::AbstractString, flavour::DatabaseFlavour) -> String

Quote an identifier appropriately for the database flavour.
"""
function quote_identifier(name::AbstractString, ::DatabaseFlavour)::String
    return "\"$name\""
end

function quote_identifier(name::AbstractString, ::MSSQLFlavour)::String
    return "[$name]"
end

function quote_identifier(name::AbstractString, ::MySQLFlavour)::String
    return "`$name`"
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

"""
    get_table_columns(conn, table_name::AbstractString, flavour::DatabaseFlavour) -> Vector{ColumnInfo}

Get column information for a table.
"""
function get_table_columns(conn, table_name::AbstractString, ::DatabaseFlavour)::Vector{ColumnInfo}
    return ColumnInfo[]
end

function get_table_columns(conn, table_name::AbstractString, ::PostgreSQLFlavour)::Vector{ColumnInfo}
    sql = """
    SELECT column_name, data_type, is_nullable = 'YES' as is_nullable
    FROM information_schema.columns
    WHERE table_name = \$1
    ORDER BY ordinal_position
    """
    columns = ColumnInfo[]
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        for row in eachrow(result)
            push!(columns, ColumnInfo(
                String(row[:column_name]),
                String(row[:data_type]),
                row[:is_nullable],
                table_name,
                nothing
            ))
        end
    catch
    end
    return columns
end

function get_table_columns(conn, table_name::AbstractString, ::MSSQLFlavour)::Vector{ColumnInfo}
    sql = """
    SELECT COLUMN_NAME, DATA_TYPE, CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as is_nullable
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """
    columns = ColumnInfo[]
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        for row in eachrow(result)
            push!(columns, ColumnInfo(
                String(row[:COLUMN_NAME]),
                String(row[:DATA_TYPE]),
                row[:is_nullable] == 1,
                table_name,
                nothing
            ))
        end
    catch
    end
    return columns
end

function get_table_columns(conn, table_name::AbstractString, ::MySQLFlavour)::Vector{ColumnInfo}
    sql = """
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE = 'YES' as is_nullable
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """
    columns = ColumnInfo[]
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        for row in eachrow(result)
            push!(columns, ColumnInfo(
                String(row[:COLUMN_NAME]),
                String(row[:DATA_TYPE]),
                row[:is_nullable] == 1,
                table_name,
                nothing
            ))
        end
    catch
    end
    return columns
end

function get_table_columns(conn, table_name::AbstractString, ::DuckDBFlavour)::Vector{ColumnInfo}
    sql = """
    SELECT column_name, data_type, is_nullable
    FROM duckdb_columns()
    WHERE table_name = ?
    ORDER BY column_index
    """
    columns = ColumnInfo[]
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        for row in eachrow(result)
            push!(columns, ColumnInfo(
                String(row[:column_name]),
                String(row[:data_type]),
                row[:is_nullable],
                table_name,
                nothing
            ))
        end
    catch
    end
    return columns
end

function get_table_columns(conn, table_name::AbstractString, ::SQLiteFlavour)::Vector{ColumnInfo}
    sql = "PRAGMA table_info('$table_name')"
    columns = ColumnInfo[]
    try
        result = DBInterface.execute(conn, sql) |> DataFrame
        for row in eachrow(result)
            push!(columns, ColumnInfo(
                String(row[:name]),
                String(row[:type]),
                row[:notnull] == 0,
                table_name,
                nothing
            ))
        end
    catch
    end
    return columns
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

function get_enum_values(conn, type_name::AbstractString, ::PostgreSQLFlavour)::Vector{VocabularyItem}
    sql = """
    SELECT enumsortorder::integer as sort_order, enumlabel
    FROM pg_enum
    WHERE enumtypid = (SELECT oid FROM pg_type WHERE typname = \$1)
    ORDER BY enumsortorder
    """
    items = VocabularyItem[]
    try
        result = DBInterface.execute(conn, sql, [type_name]) |> DataFrame
        for (i, row) in enumerate(eachrow(result))
            push!(items, VocabularyItem(
                vocabulary_item_id=nothing,
                vocabulary_id=0,
                value=Int64(i),  # Use 1-based index as value
                code=String(row[:enumlabel]),
                description=missing
            ))
        end
    catch
    end
    return items
end

function get_enum_values(conn, type_name::AbstractString, ::MySQLFlavour)::Vector{VocabularyItem}
    # MySQL ENUM values are embedded in the column type definition
    # type_name will be like "enum('val1','val2','val3')"
    items = VocabularyItem[]
    m = match(r"enum\s*\((.*)\)"i, type_name)
    if !isnothing(m)
        values_str = m.captures[1]
        # Parse the quoted values
        for (i, m) in enumerate(eachmatch(r"'([^']*)'", values_str))
            push!(items, VocabularyItem(
                vocabulary_item_id=nothing,
                vocabulary_id=0,
                value=Int64(i),
                code=String(m.captures[1]),
                description=missing
            ))
        end
    end
    return items
end

function get_enum_values(conn, type_name::AbstractString, ::DuckDBFlavour)::Vector{VocabularyItem}
    # DuckDB ENUM values
    items = VocabularyItem[]
    sql = """
    SELECT unnest(enum_range(NULL::$type_name)) as enum_value
    """
    try
        result = DBInterface.execute(conn, sql) |> DataFrame
        for (i, row) in enumerate(eachrow(result))
            push!(items, VocabularyItem(
                vocabulary_item_id=nothing,
                vocabulary_id=0,
                value=Int64(i),
                code=String(row[:enum_value]),
                description=missing
            ))
        end
    catch
    end
    return items
end

"""
    is_enum_type(conn, type_name::AbstractString, flavour::DatabaseFlavour) -> Bool

Check if a type is an enum type.
"""
function is_enum_type(conn, type_name::AbstractString, ::DatabaseFlavour)::Bool
    return false
end

function is_enum_type(conn, type_name::AbstractString, ::PostgreSQLFlavour)::Bool
    sql = """
    SELECT EXISTS (
        SELECT 1 FROM pg_type WHERE typname = \$1 AND typtype = 'e'
    ) as is_enum
    """
    try
        result = DBInterface.execute(conn, sql, [type_name]) |> DataFrame
        return result[1, :is_enum]
    catch
        return false
    end
end

function is_enum_type(conn, type_name::AbstractString, ::MySQLFlavour)::Bool
    return startswith(lowercase(type_name), "enum")
end

function is_enum_type(conn, type_name::AbstractString, ::DuckDBFlavour)::Bool
    # Check if it's a user-defined enum
    sql = """
    SELECT COUNT(*) as cnt FROM duckdb_types()
    WHERE type_name = ? AND type_category = 'ENUM'
    """
    try
        result = DBInterface.execute(conn, sql, [type_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
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

function get_check_constraint_values(conn, table_name::AbstractString, column_name::AbstractString,
                                     ::PostgreSQLFlavour)::Vector{VocabularyItem}
    sql = """
    SELECT pg_get_constraintdef(c.oid) as constraint_def
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    WHERE t.relname = \$1 
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) LIKE '%' || \$2 || '%'
    """
    items = VocabularyItem[]
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        for row in eachrow(result)
            constraint_def = String(row[:constraint_def])
            items = vcat(items, parse_check_constraint_values(constraint_def, column_name))
        end
    catch
    end
    return items
end

function get_check_constraint_values(conn, table_name::AbstractString, column_name::AbstractString,
                                     ::MSSQLFlavour)::Vector{VocabularyItem}
    sql = """
    SELECT cc.definition as constraint_def
    FROM sys.check_constraints cc
    JOIN sys.tables t ON cc.parent_object_id = t.object_id
    JOIN sys.columns c ON cc.parent_object_id = c.object_id AND cc.parent_column_id = c.column_id
    WHERE t.name = ? AND c.name = ?
    """
    items = VocabularyItem[]
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        for row in eachrow(result)
            constraint_def = String(row[:constraint_def])
            items = vcat(items, parse_check_constraint_values(constraint_def, column_name))
        end
    catch
    end
    return items
end

function get_check_constraint_values(conn, table_name::AbstractString, column_name::AbstractString,
                                     ::MySQLFlavour)::Vector{VocabularyItem}
    # MySQL 8.0+ supports CHECK constraints
    sql = """
    SELECT CHECK_CLAUSE as constraint_def
    FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
    JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
      ON cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    WHERE tc.TABLE_NAME = ? AND cc.CHECK_CLAUSE LIKE CONCAT('%', ?, '%')
    """
    items = VocabularyItem[]
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        for row in eachrow(result)
            constraint_def = String(row[:constraint_def])
            items = vcat(items, parse_check_constraint_values(constraint_def, column_name))
        end
    catch
    end
    return items
end

function get_check_constraint_values(conn, table_name::AbstractString, column_name::AbstractString,
                                     ::SQLiteFlavour)::Vector{VocabularyItem}
    # SQLite: parse from CREATE TABLE statement
    sql = "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
    items = VocabularyItem[]
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        if nrow(result) > 0
            create_sql = String(result[1, :sql])
            # Look for CHECK constraints involving the column
            # Pattern: CHECK (column_name IN ('val1', 'val2', ...))
            pattern = Regex("CHECK\\s*\\(\\s*$column_name\\s+IN\\s*\\(([^)]+)\\)", "i")
            m = match(pattern, create_sql)
            if !isnothing(m)
                items = parse_in_list_values(m.captures[1])
            end
        end
    catch
    end
    return items
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
    pattern2 = Regex("$column_name\\s*=\\s*'([^']*)'", "gi")
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

"""
    table_exists(conn, table_name::AbstractString, flavour::DatabaseFlavour) -> Bool

Check if a table exists in the database.
"""
function table_exists(conn, table_name::AbstractString, ::DatabaseFlavour)::Bool
    return false
end

function table_exists(conn, table_name::AbstractString, ::PostgreSQLFlavour)::Bool
    sql = """
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = \$1
    ) as exists
    """
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        return result[1, :exists]
    catch
        return false
    end
end

function table_exists(conn, table_name::AbstractString, ::MSSQLFlavour)::Bool
    sql = "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?"
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
end

function table_exists(conn, table_name::AbstractString, ::MySQLFlavour)::Bool
    sql = "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?"
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
end

function table_exists(conn, table_name::AbstractString, ::DuckDBFlavour)::Bool
    sql = "SELECT COUNT(*) as cnt FROM duckdb_tables() WHERE table_name = ?"
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
end

function table_exists(conn, table_name::AbstractString, ::SQLiteFlavour)::Bool
    sql = "SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='table' AND name=?"
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
end
#endregion

#region Column Type Information from Database
"""
    get_column_type_info(conn, sql::AbstractString, flavour::DatabaseFlavour) -> Vector{Tuple{String,String,Union{String,Nothing}}}

Get detailed column type information from the database for a query.
Returns vector of (column_name, data_type, table_name) tuples.
"""
function get_column_type_info(conn, sql::AbstractString, flavour::DatabaseFlavour)
    # Execute with limit 0 to get metadata
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

function get_original_column_type(conn, table_name::AbstractString, column_name::AbstractString,
                                  ::PostgreSQLFlavour)::Union{String,Nothing}
    sql = """
    SELECT CASE 
        WHEN t.typtype = 'e' THEN t.typname
        ELSE c.data_type
    END as data_type,
    t.typtype
    FROM information_schema.columns c
    LEFT JOIN pg_type t ON c.udt_name = t.typname
    WHERE c.table_name = \$1 AND c.column_name = \$2
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        if nrow(result) > 0
            return String(result[1, :data_type])
        end
    catch
    end
    return nothing
end

function get_original_column_type(conn, table_name::AbstractString, column_name::AbstractString,
                                  ::MSSQLFlavour)::Union{String,Nothing}
    sql = """
    SELECT DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ? AND COLUMN_NAME = ?
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        if nrow(result) > 0
            return String(result[1, :DATA_TYPE])
        end
    catch
    end
    return nothing
end

function get_original_column_type(conn, table_name::AbstractString, column_name::AbstractString,
                                  ::MySQLFlavour)::Union{String,Nothing}
    sql = """
    SELECT COLUMN_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ? AND COLUMN_NAME = ?
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        if nrow(result) > 0
            return String(result[1, :COLUMN_TYPE])
        end
    catch
    end
    return nothing
end

function get_original_column_type(conn, table_name::AbstractString, column_name::AbstractString,
                                  ::DuckDBFlavour)::Union{String,Nothing}
    sql = """
    SELECT data_type
    FROM duckdb_columns()
    WHERE table_name = ? AND column_name = ?
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        if nrow(result) > 0
            return String(result[1, :data_type])
        end
    catch
    end
    return nothing
end

function get_original_column_type(conn, table_name::AbstractString, column_name::AbstractString,
                                  ::SQLiteFlavour)::Union{String,Nothing}
    sql = "PRAGMA table_info('$table_name')"
    try
        result = DBInterface.execute(conn, sql) |> DataFrame
        for row in eachrow(result)
            if row[:name] == column_name
                return String(row[:type])
            end
        end
    catch
    end
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
        
        # 2. Check for code table reference (integer column matching table primary key)
        if value_type_id == TRE_TYPE_INTEGER && table_exists(conn, column_name, flavour)
            if is_code_table(conn, column_name, column_name, flavour)
                value_type_id = TRE_TYPE_CATEGORY
                code_items = get_code_table_vocabulary(conn, column_name, column_name, flavour)
                if !isempty(code_items)
                    vocabulary = Vocabulary(
                        vocabulary_id=nothing,
                        name="$(column_name)_codes",
                        description="Code table values from $column_name",
                        items=code_items
                    )
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
        return strip(table, ['"', '\'', '`', '[', ']'])
    end
    return nothing
end
#endregion

# meta_msql.jl - MSSQL-specific SQL metadata extraction overrides

#region Type Mapping
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
#endregion

#region Column Metadata Extraction
"""
    get_query_columns(conn, sql::AbstractString, ::MSSQLFlavour) -> Vector{ColumnInfo}

MSSQL-specific implementation using sys.dm_exec_describe_first_result_set to get
rich column metadata including source table and schema information.
"""
function get_query_columns(conn, sql::AbstractString, ::MSSQLFlavour)::Vector{ColumnInfo}
    # Use sys.dm_exec_describe_first_result_set to get detailed column metadata
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

        for col_name in names(df)
            col_type = eltype(df[!, col_name])
            sql_type = julia_type_to_sql_string(col_type)
            push!(columns, ColumnInfo(String(col_name), sql_type, col_type >: Missing, nothing, nothing))
        end
    end
    return columns
end

function wrap_query_for_metadata(sql::AbstractString, ::MSSQLFlavour)::String
    # MSSQL uses TOP instead of LIMIT
    return "SELECT TOP 0 * FROM ($sql) AS _meta_query"
end
#endregion

#region Column Comments
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
#endregion

#region Constraints / Code Tables
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
#endregion

#region Quoting / Table Inspection
function quote_identifier(name::AbstractString, ::MSSQLFlavour)::String
    return "[$name]"
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
#endregion

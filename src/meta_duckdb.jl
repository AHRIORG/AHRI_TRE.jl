# meta_duckdb.jl - DuckDB-specific SQL metadata extraction overrides

#region Type Mapping
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

    # DateTime types
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
#endregion

#region Column Comments
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

#region Constraints / Enums
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
                col_names = result2[1, :constraint_column_names]
                return column_name in col_names
            end
        end
    catch
        return false
    end
    return false
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
#endregion

#region Table Inspection
function table_exists(conn, table_name::AbstractString, ::DuckDBFlavour)::Bool
    sql = "SELECT COUNT(*) as cnt FROM duckdb_tables() WHERE table_name = ?"
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
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
#endregion

# meta_mysql.jl - MySQL-specific SQL metadata extraction overrides

#region Type Mapping
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

    # DateTime types
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
#endregion

#region Column Comments
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
#endregion

#region Constraints / Enums
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

function is_enum_type(conn, type_name::AbstractString, ::MySQLFlavour)::Bool
    return startswith(lowercase(type_name), "enum")
end

function get_enum_values(conn, type_name::AbstractString, ::MySQLFlavour)::Vector{VocabularyItem}
    # MySQL ENUM values are embedded in the column type definition
    # type_name will be like "enum('val1','val2','val3')"
    items = VocabularyItem[]
    m = match(r"enum\s*\((.*)\)"i, type_name)
    if !isnothing(m)
        values_str = m.captures[1]
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
#endregion

#region Quoting / Table Inspection
function quote_identifier(name::AbstractString, ::MySQLFlavour)::String
    return "`$name`"
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
#endregion

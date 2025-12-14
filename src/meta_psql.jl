# meta_psql.jl - PostgreSQL-specific SQL metadata extraction overrides

#region Type Mapping
function map_sql_type_to_tre(sql_type::AbstractString, ::PostgreSQLFlavour)::Int
    type_lower = lowercase(sql_type)

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
#endregion

#region Column Comments
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
#endregion

#region Constraints / Enums
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
                value=Int64(i),
                code=String(row[:enumlabel]),
                description=missing
            ))
        end
    catch
    end
    return items
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
#endregion

#region Table Inspection
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
#endregion

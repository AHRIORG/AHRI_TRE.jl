# meta_sqlite.jl - SQLite-specific SQL metadata extraction overrides

#region Type Mapping
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

#region Constraints / Code Tables
function get_foreign_key_reference(conn, table_name::AbstractString, column_name::AbstractString,
                                   ::SQLiteFlavour)::Union{Nothing,Tuple{String,String}}
    sql = "PRAGMA foreign_key_list('$table_name')"
    try
        result = DBInterface.execute(conn, sql) |> DataFrame
        for row in eachrow(result)
            if String(row[:from]) == column_name
                return (String(row[:table]), String(row[:to]))
            end
        end
    catch
    end
    return nothing
end

function table_has_primary_key(conn, table_name::AbstractString, column_name::AbstractString, ::SQLiteFlavour)::Bool
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

function get_check_constraint_values(conn, table_name::AbstractString, column_name::AbstractString,
                                     ::SQLiteFlavour)::Vector{VocabularyItem}
    # SQLite: parse from CREATE TABLE statement
    sql = "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
    items = VocabularyItem[]
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        if nrow(result) > 0
            create_sql = String(result[1, :sql])
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
#endregion

#region Table Inspection
function table_exists(conn, table_name::AbstractString, ::SQLiteFlavour)::Bool
    sql = "SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='table' AND name=?"
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
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

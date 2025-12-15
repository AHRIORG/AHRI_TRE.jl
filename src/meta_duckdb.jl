# meta_duckdb.jl - DuckDB-specific SQL metadata extraction overrides

# NOTE: DuckDB.jl currently marks several legacy result-chunk APIs as deprecated.
# Under `--depwarn=yes` (as used by `Pkg.test()`), materializing results through
# Tables/DataFrames triggers those deprecated APIs. For DuckDB-specific metadata
# queries we therefore fetch result chunks via `duckdb_fetch_chunk` and convert
# columns directly.

function connect_duckdb(db_path::AbstractString)
    db = DuckDB.DB(db_path)
    conn = DBInterface.connect(db)
    df = DBInterface.execute(conn, "SELECT version() AS version;") |> DataFrame
    version = df[1, :version]
    @info "Connected to DuckDB version: $version"
    return db, conn
end
function close_duckdb(db::DuckDB.DB, conn)
    DBInterface.close!(conn)
    DuckDB.close(db)
end
function duckdb_query_columntable(conn, sql::AbstractString, params=nothing)
    q = isnothing(params) ? DBInterface.execute(conn, sql) : DBInterface.execute(conn, sql, params)
    column_count = DuckDB.duckdb_column_count(q.handle)
    chunks = DuckDB.DataChunk[]
    while true
        chunk_handle = DuckDB.duckdb_fetch_chunk(q.handle[])
        if chunk_handle == C_NULL
            break
        end
        chunk = DuckDB.DataChunk(chunk_handle, true)
        if DuckDB.get_size(chunk) == 0
            break
        end
        push!(chunks, chunk)
    end
    return DuckDB.convert_columns(q, chunks, column_count)
end

function duckdb_scalar(conn, sql::AbstractString, col::Symbol; params=nothing)
    tbl = duckdb_query_columntable(conn, sql, params)
    return tbl[col][1]
end

#region Column Metadata Extraction (DuckDB)
function get_query_columns(conn, sql::AbstractString, flavour::DuckDBFlavour)::Vector{ColumnInfo}
    wrapped_sql = wrap_query_for_metadata(sql, flavour)
    tbl = duckdb_query_columntable(conn, wrapped_sql)

    columns = ColumnInfo[]
    for (col_sym, col_vec) in pairs(tbl)
        col_type = eltype(col_vec)
        sql_type = julia_type_to_sql_string(col_type)
        push!(columns, ColumnInfo(String(col_sym), sql_type, col_type >: Missing, nothing, nothing))
    end
    return columns
end
#endregion

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
        tbl = duckdb_query_columntable(conn, sql, [table_name, column_name])
        if !isempty(tbl[:comment]) && !ismissing(tbl[:comment][1]) && !isnothing(tbl[:comment][1])
            return String(tbl[:comment][1])
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
        cnt = duckdb_scalar(conn, sql, :cnt; params=[table_name])
        if cnt > 0
            # Check if the column is part of the primary key
            sql2 = """
            SELECT constraint_column_names FROM duckdb_constraints()
            WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'
            """
            tbl2 = duckdb_query_columntable(conn, sql2, [table_name])
            if !isempty(tbl2[:constraint_column_names])
                col_names = tbl2[:constraint_column_names][1]
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
        cnt = duckdb_scalar(conn, sql, :cnt; params=[type_name])
        return cnt > 0
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
        tbl = duckdb_query_columntable(conn, sql)
        for (i, v) in enumerate(tbl[:enum_value])
            push!(items, VocabularyItem(
                vocabulary_item_id=nothing,
                vocabulary_id=0,
                value=Int64(i),
                code=String(v),
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
        cnt = duckdb_scalar(conn, sql, :cnt; params=[table_name])
        return cnt > 0
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
        tbl = duckdb_query_columntable(conn, sql, [table_name, column_name])
        if !isempty(tbl[:data_type])
            return String(tbl[:data_type][1])
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
        tbl = duckdb_query_columntable(conn, sql, [table_name])
        for i in eachindex(tbl[:column_name])
            push!(columns, ColumnInfo(
                String(tbl[:column_name][i]),
                String(tbl[:data_type][i]),
                tbl[:is_nullable][i],
                table_name,
                nothing
            ))
        end
    catch
    end
    return columns
end
#endregion

#region Foreign Key Inspection
function get_foreign_key_reference(conn, table_name::AbstractString, column_name::AbstractString,
                                   ::DuckDBFlavour)::Union{Nothing,Tuple{String,String}}
    sql = """
    SELECT *
    FROM duckdb_constraints()
    WHERE table_name = ?
      AND constraint_type = 'FOREIGN KEY'
    """
    try
        tbl = duckdb_query_columntable(conn, sql, [table_name])
        if !haskey(tbl, :constraint_column_names) || !haskey(tbl, :referenced_table) || !haskey(tbl, :referenced_column_names)
            return nothing
        end
        for i in eachindex(tbl[:constraint_column_names])
            src_cols = tbl[:constraint_column_names][i]
            if column_name in src_cols
                idx = findfirst(==(column_name), src_cols)
                ref_table = String(tbl[:referenced_table][i])
                ref_cols = tbl[:referenced_column_names][i]
                ref_col = isnothing(idx) ? nothing : ref_cols[idx]
                if !isnothing(ref_col)
                    return (ref_table, String(ref_col))
                end
            end
        end
    catch
    end
    return nothing
end
#endregion

#region Code Table Detection (DuckDB)
function is_code_table(conn, table_name::AbstractString, pk_column::AbstractString, flavour::DuckDBFlavour)::Bool
    if !table_has_primary_key(conn, table_name, pk_column, flavour)
        return false
    end

    count_sql = "SELECT COUNT(*) AS cnt FROM $(quote_identifier(table_name, flavour))"
    try
        cnt = duckdb_scalar(conn, count_sql, :cnt)
        return cnt < 250
    catch
        return false
    end
end

function get_code_table_vocabulary(conn, table_name::AbstractString, pk_column::AbstractString,
                                   flavour::DuckDBFlavour)::Vector{VocabularyItem}
    items = VocabularyItem[]
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

    if !isnothing(desc_col) && desc_col == string_col
        desc_col = nothing
    end

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

    pk_quoted = quote_identifier(pk_column, flavour)
    str_quoted = quote_identifier(string_col, flavour)
    table_quoted = quote_identifier(table_name, flavour)

    sql = if !isnothing(desc_col)
        desc_quoted = quote_identifier(desc_col, flavour)
        "SELECT $pk_quoted AS pk_value, $str_quoted AS code_value, $desc_quoted AS desc_value FROM $table_quoted ORDER BY $pk_quoted"
    else
        "SELECT $pk_quoted AS pk_value, $str_quoted AS code_value FROM $table_quoted ORDER BY $pk_quoted"
    end

    try
        tbl = duckdb_query_columntable(conn, sql)
        for i in eachindex(tbl[:pk_value])
            value = Int64(tbl[:pk_value][i])
            code = String(tbl[:code_value][i])
            desc = if haskey(tbl, :desc_value) && !ismissing(tbl[:desc_value][i])
                String(tbl[:desc_value][i])
            else
                missing
            end
            push!(items, VocabularyItem(
                vocabulary_item_id=nothing,
                vocabulary_id=0,
                value=value,
                code=code,
                description=desc
            ))
        end
    catch
    end

    return items
end
#endregion

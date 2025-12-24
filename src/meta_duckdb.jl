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

#region Identifier Helpers
function duckdb_split_table_identifier(name::AbstractString)::Tuple{Union{String,Nothing},String}
    stripped = strip(name)
    idx = findfirst('.', stripped)
    if isnothing(idx)
        return nothing, stripped
    end
    schema = strip(stripped[1:idx-1])
    table = strip(stripped[idx+1:end])
    schema = isempty(schema) ? nothing : schema
    return schema, table
end

function duckdb_system_filter(schema::Union{String,Nothing}, table::AbstractString)::Tuple{String,Vector{Any}}
    clause = "table_name = ?"
    params = Any[table]
    if !isnothing(schema)
        clause *= " AND schema_name = ?"
        push!(params, schema)
    end
    return clause, params
end

duckdb_quote_segment(segment::AbstractString) = "\"" * replace(segment, "\"" => "\"\"") * "\""

function duckdb_normalize_table_identifier(name::AbstractString)::String
    schema, table = duckdb_split_table_identifier(name)
    schema_lower = lowercase(something(schema, "main"))
    return string(schema_lower, ".", lowercase(table))
end

function quote_identifier(name::AbstractString, ::DuckDBFlavour)::String
    schema, table = duckdb_split_table_identifier(name)
    if isnothing(schema)
        return duckdb_quote_segment(table)
    end
    return string(duckdb_quote_segment(schema), ".", duckdb_quote_segment(table))
end
#endregion

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
    schema, bare_table = duckdb_split_table_identifier(table_name)
    clause, params = duckdb_system_filter(schema, bare_table)
    sql = """
    SELECT constraint_column_names
    FROM duckdb_constraints()
    WHERE $clause AND constraint_type = 'PRIMARY KEY'
    """
    try
        tbl = duckdb_query_columntable(conn, sql, params)
        if !haskey(tbl, :constraint_column_names)
            return false
        end
        target = lowercase(column_name)
        for cols in tbl[:constraint_column_names]
            if isnothing(cols)
                continue
            end
            for col in cols
                if lowercase(String(col)) == target
                    return true
                end
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
    schema, bare_table = duckdb_split_table_identifier(table_name)
    clause, params = duckdb_system_filter(schema, bare_table)
    sql = "SELECT COUNT(*) as cnt FROM duckdb_tables() WHERE $clause"
    try
        cnt = duckdb_scalar(conn, sql, :cnt; params=params)
        return cnt > 0
    catch
        return false
    end
end

function get_original_column_type(conn, table_name::AbstractString, column_name::AbstractString,
                                  ::DuckDBFlavour)::Union{String,Nothing}
    schema, bare_table = duckdb_split_table_identifier(table_name)
    clause, params = duckdb_system_filter(schema, bare_table)
    sql = """
    SELECT data_type
    FROM duckdb_columns()
    WHERE $clause AND column_name = ?
    """
    try
        query_params = copy(params)
        push!(query_params, column_name)
        tbl = duckdb_query_columntable(conn, sql, query_params)
        if !isempty(tbl[:data_type])
            return String(tbl[:data_type][1])
        end
    catch
    end
    return nothing
end

function get_table_columns(conn, table_name::AbstractString, ::DuckDBFlavour)::Vector{ColumnInfo}
    schema, bare_table = duckdb_split_table_identifier(table_name)
    clause, params = duckdb_system_filter(schema, bare_table)
    sql = """
    SELECT column_name, data_type, is_nullable
    FROM duckdb_columns()
    WHERE $clause
    ORDER BY column_index
    """
    columns = ColumnInfo[]
    try
        tbl = duckdb_query_columntable(conn, sql, params)
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
    schema, bare_table = duckdb_split_table_identifier(table_name)
    clause, params = duckdb_system_filter(schema, bare_table)
    sql = """
    SELECT constraint_column_names, referenced_table, referenced_column_names
    FROM duckdb_constraints()
    WHERE $clause AND constraint_type = 'FOREIGN KEY'
    """
    try
        tbl = duckdb_query_columntable(conn, sql, params)
        required_cols = [:constraint_column_names, :referenced_table, :referenced_column_names]
        if any(!haskey(tbl, col) for col in required_cols)
            return nothing
        end
        lower_target = lowercase(column_name)
        for i in eachindex(tbl[:constraint_column_names])
            src_cols = tbl[:constraint_column_names][i]
            if isnothing(src_cols)
                continue
            end
            idx = findfirst(col -> lowercase(String(col)) == lower_target, src_cols)
            if isnothing(idx)
                continue
            end
            ref_cols = tbl[:referenced_column_names][i]
            if isnothing(ref_cols) || idx > length(ref_cols)
                continue
            end
            ref_col = ref_cols[idx]
            if isnothing(ref_col)
                continue
            end
            ref_table = String(tbl[:referenced_table][i])
            return (ref_table, String(ref_col))
        end
    catch
    end
    return nothing
end
#endregion

#region Code Table Detection (DuckDB)
function find_code_table_by_column_name(conn, column_name::AbstractString,
                                                                                ::DuckDBFlavour,
                                                                                source_table::Union{Nothing,String})::Union{Nothing,Tuple{String,String}}
    sql = """
    SELECT schema_name, table_name, constraint_column_names
    FROM duckdb_constraints()
    WHERE constraint_type = 'PRIMARY KEY'
      AND lower(COALESCE(schema_name, 'main')) IN ('main', 'codes')
    ORDER BY CASE WHEN lower(COALESCE(schema_name, 'main')) = 'codes' THEN 0 ELSE 1 END,
             table_name
    """
    try
        tbl = duckdb_query_columntable(conn, sql)
        if !haskey(tbl, :constraint_column_names) || !haskey(tbl, :table_name)
            return nothing
        end
        schema_col = haskey(tbl, :schema_name) ? tbl[:schema_name] : nothing
        source_norm = isnothing(source_table) ? nothing : duckdb_normalize_table_identifier(source_table)
        target = lowercase(column_name)
        for i in eachindex(tbl[:constraint_column_names])
            cols = tbl[:constraint_column_names][i]
            if isnothing(cols) || length(cols) != 1
                continue
            end
            pk_name = cols[1]
            if lowercase(String(pk_name)) != target
                continue
            end
            table = String(tbl[:table_name][i])
            schema_val = isnothing(schema_col) ? nothing : schema_col[i]
            schema_str = if isnothing(schema_val) || ismissing(schema_val)
                nothing
            else
                str = String(schema_val)
                isempty(str) ? nothing : str
            end
            schema_lower = isnothing(schema_str) ? "main" : lowercase(schema_str)
            candidate_norm = string(schema_lower, ".", lowercase(table))
            if !isnothing(source_norm) && source_norm == candidate_norm
                continue
            end
            if schema_lower == "codes"
                return (string(schema_str, ".", table), column_name)
            elseif schema_lower == "main"
                return (table, column_name)
            end
        end
    catch
    end
    return nothing
end

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

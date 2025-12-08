########################
# meta_sqlite.jl
########################

struct SQLiteFlavor <: DBFlavor end
const SQLITE = SQLiteFlavor()

# ───────── SQLite Specific Configuration ─────────

# SQLite identifiers: "name" (ANSI SQL standard)
sql_ident(::SQLiteFlavor, s::AbstractString) = "\"" * replace(String(s), "\"" => "\"\"") * "\""

# ───────── Core Database Operations ─────────

"""Create temporary table for query analysis (instead of view for better type info)."""
function make_temp_view(::SQLiteFlavor, db::SQLite.DB, sql::AbstractString)
    table_name = "_tmp_meta_table_"
    
    # Clean up any existing table and create new one
    drop_sql = "DROP TABLE IF EXISTS $(sql_ident(SQLITE, table_name));"
    create_sql = "CREATE TEMP TABLE $(sql_ident(SQLITE, table_name)) AS $(String(sql));"
    
    DBInterface.execute(db, drop_sql)
    DBInterface.execute(db, create_sql)
    
    return table_name
end

"""Map Julia types to SQLite type names."""
function map_julia_type_to_sqlite(T::Type)
    if T <: AbstractString
        return "TEXT"
    elseif T <: Integer
        return "INTEGER"
    elseif T <: Real
        return "REAL"
    elseif T <: Bool
        return "BOOLEAN"
    elseif T <: Dates.Date
        return "DATE"
    elseif T <: Dates.DateTime
        return "TIMESTAMP"
    else
        return "BLOB"
    end
end

"""Get column information from temporary table using PRAGMA table_info."""
function describe_output(::SQLiteFlavor, db::SQLite.DB, tablename::AbstractString)
    # Use PRAGMA table_info which should work better with temporary tables
    pragma_result = try
        query = "PRAGMA table_info($(sql_ident(SQLITE, tablename)));"
        DBInterface.execute(db, query; iterate_rows=true) |> DataFrame
    catch e
        @warn "PRAGMA table_info failed: $e"
        DataFrame()
    end
    
    # If PRAGMA worked and has name column, use it
    if !isempty(pragma_result) && hascol(pragma_result, "name")
        name_col = colname(pragma_result, "name")
        type_col = hascol(pragma_result, "type") ? colname(pragma_result, "type") : nothing
        
        column_names = String.(pragma_result[!, name_col])
        
        if type_col !== nothing
            # Get types from PRAGMA, but enhance empty/missing types
            data_types = String[]
            for (i, col_name) in enumerate(column_names)
                pragma_type = String(coalesce(pragma_result[i, type_col], ""))
                
                if isempty(pragma_type) || pragma_type == ""
                    # Try to infer type from a sample value
                    inferred_type = try
                        sample_query = "SELECT $(sql_ident(SQLITE, col_name)) FROM $(sql_ident(SQLITE, tablename)) LIMIT 1;"
                        sample_result = DBInterface.execute(db, sample_query; iterate_rows=true) |> DataFrame
                        
                        if !isempty(sample_result) && !ismissing(sample_result[1, 1])
                            sample_value = sample_result[1, 1]
                            map_julia_type_to_sqlite(typeof(sample_value))
                        else
                            "UNKNOWN"
                        end
                    catch
                        "UNKNOWN"
                    end
                    push!(data_types, inferred_type)
                else
                    push!(data_types, pragma_type)
                end
            end
        else
            # No type column in PRAGMA result, try to infer all types
            data_types = String[]
            for col_name in column_names
                inferred_type = try
                    sample_query = "SELECT $(sql_ident(SQLITE, col_name)) FROM $(sql_ident(SQLITE, tablename)) LIMIT 1;"
                    sample_result = DBInterface.execute(db, sample_query; iterate_rows=true) |> DataFrame
                    
                    if !isempty(sample_result) && !ismissing(sample_result[1, 1])
                        sample_value = sample_result[1, 1]
                        map_julia_type_to_sqlite(typeof(sample_value))
                    else
                        "UNKNOWN"
                    end
                catch
                    "UNKNOWN"
                end
                push!(data_types, inferred_type)
            end
        end
        
        return DataFrame(column_name = column_names, data_type = data_types)
    end
    
    # Last resort fallback: Execute LIMIT 0 query and read schema
    try
        query = "SELECT * FROM $(sql_ident(SQLITE, tablename)) LIMIT 0;"
        cursor = DBInterface.execute(db, query; iterate_rows=true)
        schema = Tables.schema(cursor)
        column_names = String.(schema.names)
        
        # Try to infer types from schema or sample data
        data_types = String[]
        if schema.types !== nothing && length(schema.types) == length(column_names)
            for julia_type in schema.types
                sqlite_type = map_julia_type_to_sqlite(julia_type)
                push!(data_types, sqlite_type)
            end
        else
            # Sample each column individually
            for col_name in column_names
                inferred_type = try
                    sample_query = "SELECT $(sql_ident(SQLITE, col_name)) FROM $(sql_ident(SQLITE, tablename)) LIMIT 1;"
                    sample_result = DBInterface.execute(db, sample_query; iterate_rows=true) |> DataFrame
                    
                    if !isempty(sample_result) && !ismissing(sample_result[1, 1])
                        sample_value = sample_result[1, 1]
                        map_julia_type_to_sqlite(typeof(sample_value))
                    else
                        "UNKNOWN"
                    end
                catch
                    "UNKNOWN"
                end
                push!(data_types, inferred_type)
            end
        end
        
        return DataFrame(column_name = column_names, data_type = data_types)
    catch e
        error("Failed to describe output for table '$tablename': $e")
    end
end

# ───────── Relation Discovery ─────────

"""Parse referenced tables from final SELECT, excluding CTEs."""
function referenced_relations(::SQLiteFlavor, ::SQLite.DB, ::AbstractString, sql::AbstractString)
    cte_names = extract_cte_names(sql)
    final_select = final_select_segment(sql)
    
    # Pattern to match table references in FROM/JOIN clauses
    table_pattern = r"""
        \b(?:from|join)\s+
        (
          (?:"[^"]+"|\w+)               # table or schema name
          (?:\.(?:"[^"]+"|\w+))?        # optional .table_name
        )
    """ix
    
    relations = Tuple{Union{Missing,String},String}[]
    
    for match in eachmatch(table_pattern, final_select)
        relation_ref = match.captures[1]
        parts = split(relation_ref, '.')
        
        if length(parts) == 2
            # Schema-qualified: schema.table (rare in SQLite)
            schema = strip_identifier_quotes(parts[1])
            table = strip_identifier_quotes(parts[2])
            push!(relations, (schema, table))
        else
            # Bare table name - exclude if it's a CTE
            table_name = strip_identifier_quotes(parts[1])
            if !(lowercase(table_name) in cte_names)
                push!(relations, (missing, table_name))
            end
        end
    end
    
    return unique(relations)
end

# ───────── Metadata Loading Functions ─────────

"""Load column information from SQLite tables (no native column comments)."""
function load_all_column_descriptions(::SQLiteFlavor, db::SQLite.DB, rels)
    result_rows = DataFrame(
        schema_name=String[], table_name=String[], 
        column_name=String[], description=String[]
    )
    
    for (schema, table) in rels
        table_info = try
            query = "PRAGMA table_info($(sql_ident(SQLITE, table)));"
            DBInterface.execute(db, query; iterate_rows=true) |> DataFrame
        catch
            DataFrame()
        end
        
        for row in eachrow(table_info)
            column_name = haskey(row, :name) ? String(row[:name]) : ""
            if !isempty(column_name)
                push!(result_rows, (
                    "", # SQLite doesn't typically use schemas
                    String(table),
                    column_name,
                    "" # SQLite has no native column comments
                ))
            end
        end
    end
    
    return result_rows
end

"""Load foreign key relationships using PRAGMA foreign_key_list."""
function load_fk_edges(::SQLiteFlavor, db::SQLite.DB, rels)
    result_rows = DataFrame(
        rel_schema=String[], rel_table=String[], src_column=String[],
        ref_schema=String[], ref_table=String[], ref_column=String[]
    )
    
    for (schema, table) in rels
        fk_info = try
            query = "PRAGMA foreign_key_list($(sql_ident(SQLITE, table)));"
            DBInterface.execute(db, query; iterate_rows=true) |> DataFrame
        catch
            DataFrame()
        end
        
        for row in eachrow(fk_info)
            src_column = haskey(row, :from) ? String(row[:from]) : ""
            ref_table = haskey(row, :table) ? String(row[:table]) : ""
            ref_column = haskey(row, :to) ? String(row[:to]) : ""
            
            if !any(isempty, [src_column, ref_table, ref_column])
                push!(result_rows, (
                    "", String(table), src_column,
                    "", ref_table, ref_column
                ))
            end
        end
    end
    
    return result_rows
end

# ───────── Vocabulary and Sampling Functions ─────────

"""Check if a SQLite data type is text-based."""
is_text_type_sqlite(data_type::AbstractString) = 
    occursin(r"(?i)char|text|clob", String(data_type))

"""Find the best label column for vocabulary display."""
function pick_label_column(::SQLiteFlavor, db::SQLite.DB, schema::AbstractString, table::AbstractString)
    table_info = try
        query = "PRAGMA table_info($(sql_ident(SQLITE, table)));"
        DBInterface.execute(db, query; iterate_rows=true) |> DataFrame
    catch
        DataFrame()
    end
    
    isempty(table_info) && return missing
    
    # Prefer columns with label-like names
    preferred_patterns = r"(?i)^(label|name|title|desc|description)$"
    for row in eachrow(table_info)
        column_name = String(get(row, :name, ""))
        column_type = String(get(row, :type, ""))
        
        if is_text_type_sqlite(column_type) && occursin(preferred_patterns, column_name)
            return column_name
        end
    end
    
    # Fall back to any text column
    for row in eachrow(table_info)
        column_name = String(get(row, :name, ""))
        column_type = String(get(row, :type, ""))
        
        if is_text_type_sqlite(column_type)
            return column_name
        end
    end
    
    return missing
end

"""Sample vocabulary values from a table."""
function sample_vocab(::SQLiteFlavor, db::SQLite.DB, schema::AbstractString, table::AbstractString,
                     codecol::AbstractString, labelcol::Union{AbstractString,Missing}, max_rows::Int)
    
    qualified_table = qualify(SQLITE, missing, table)  # SQLite typically doesn't use schemas
    code_col_quoted = sql_ident(SQLITE, codecol)
    
    if labelcol === missing
        query = string(
            "SELECT ", code_col_quoted, " AS code ",
            "FROM ", qualified_table, " ",
            "ORDER BY 1 LIMIT ", string(max_rows), ";"
        )
    else
        label_col_quoted = sql_ident(SQLITE, labelcol)
        query = string(
            "SELECT ", code_col_quoted, " AS code, ", label_col_quoted, " AS label ",
            "FROM ", qualified_table, " ",
            "ORDER BY 1 LIMIT ", string(max_rows), ";"
        )
    end
    
    return DBInterface.execute(db, query; iterate_rows=true) |> DataFrame
end

"""Check if table exceeds row threshold."""
function table_exceeds_threshold(::SQLiteFlavor, db::SQLite.DB, schema::AbstractString, table::AbstractString; threshold::Int)
    qualified_table = qualify(SQLITE, missing, table)
    query = "SELECT 1 FROM $(qualified_table) LIMIT $(threshold + 1);"
    
    result = DBInterface.execute(db, query; iterate_rows=true) |> DataFrame
    return nrow(result) > threshold
end

# ───────── Advanced Vocabulary Processing ─────────

"""Extract CHECK constraint values from CREATE TABLE SQL."""
function extract_check_constraint_values(table_sql::AbstractString, column_name::AbstractString)::Vector{String}
    # Look for CHECK constraints on the specific column
    check_pattern = Regex("(?is)\\b" * column_name * "\\b\\s*[^,]*?\\bCHECK\\s*\\((.*?)\\)")
    check_match = match(check_pattern, table_sql)
    check_match === nothing && return String[]
    
    check_clause = check_match.captures[1]
    
    # Look for IN (...) patterns within the CHECK clause
    in_match = match(r"(?is)\bIN\s*\((.*?)\)", check_clause)
    in_match === nothing && return String[]
    
    inner_content = in_match.captures[1]
    
    # Extract quoted string literals
    values = String[]
    for string_match in eachmatch(r"'((?:''|[^'])*)'", inner_content)
        # Unescape doubled quotes
        unescaped_value = replace(string_match.captures[1], "''" => "'")
        push!(values, unescaped_value)
    end
    
    return unique(values)
end

"""Post-process vocabulary by examining CHECK constraints."""
function postprocess_vocab!(::SQLiteFlavor, db::SQLite.DB, cols::DataFrame; max_vocab::Int=200)
    for i in 1:nrow(cols)
        # Skip if already has vocabulary or missing source info
        if !ismissing(cols.vocabulary_relation[i]) || 
           ismissing(cols.source_relation[i]) || 
           ismissing(cols.base_column[i])
            continue
        end
        
        # Parse source relation (SQLite typically doesn't use schemas)
        _, table = _split_relkey(String(cols.source_relation[i]))
        column = String(cols.base_column[i])
        
        # Get the CREATE TABLE/VIEW SQL from sqlite_master
        master_query = "SELECT sql FROM sqlite_master WHERE type IN ('table','view') AND name = ? LIMIT 1;"
        table_sql_df = try
            DBInterface.execute(db, master_query, (table,); iterate_rows=true) |> DataFrame
        catch
            DataFrame()
        end
        
        isempty(table_sql_df) && continue
        table_sql = String(table_sql_df[1, :sql])
        
        # Extract CHECK constraint values
        constraint_values = extract_check_constraint_values(table_sql, column)
        isempty(constraint_values) && continue
        
        # Only proceed if values are within the limit
        if length(constraint_values) <= max_vocab
            cols.vocabulary_relation[i] = "check:$(table).$(column)"
            cols.code_column[i] = "literal"
            cols.label_column[i] = missing
            cols.vocab_sample[i] = DataFrame(code = constraint_values)
            cols.vocab_skipped[i] = false
        end
    end
    
    return cols
end

# ───────── Utility Functions ─────────

"""Get table column metadata for structure analysis."""
function get_table_columns_metadata(::SQLiteFlavor, db::SQLite.DB, schema::AbstractString, table::AbstractString)
    query = "PRAGMA table_info($(sql_ident(SQLITE, table)));"
    
    try
        result = DBInterface.execute(db, query; iterate_rows=true) |> DataFrame
        # Standardize column names
        if hascol(result, "name")
            rename!(result, "name" => "column_name")
        end
        if hascol(result, "type")
            rename!(result, "type" => "data_type")
        end
        
        return select(result, :column_name, :data_type)
    catch e
        @warn "Failed to get table columns metadata for $table: $e"
        return DataFrame(column_name=String[], data_type=String[])
    end
end

# ───────── Public Interface ─────────

"""
Main entry point for describing SQLite query variables.

Uses SQLite's PRAGMA commands and sqlite_master table for metadata discovery,
with CHECK constraint parsing for vocabulary extraction.
"""
function describe_query_variables_sqlite(db::SQLite.DB, sql::AbstractString;
                                        max_vocab::Int=200, vocab_row_threshold::Int=5_000)
    return describe_query_variables(SQLITE, db, sql; 
                                   max_vocab=max_vocab, 
                                   vocab_row_threshold=vocab_row_threshold)
end
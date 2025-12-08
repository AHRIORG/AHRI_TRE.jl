########################
# meta_duckdb.jl
########################

struct DuckDBFlavor <: DBFlavor end
const DUCK = DuckDBFlavor()

# ───────── DuckDB Specific Configuration ─────────

# DuckDB identifier quoting: "name" (same as ANSI SQL)
sql_ident(::DuckDBFlavor, s::AbstractString) = "\"" * replace(String(s), "\"" => "\"\"") * "\""

# ───────── Core Database Operations ─────────

"""Create temporary view for query analysis."""
function make_temp_view(::DuckDBFlavor, db::DuckDB.DB, sql::AbstractString)
    view_name = "_tmp_meta_view_"
    
    # Clean up any existing view
    DuckDB.execute(db, "DROP VIEW IF EXISTS $(sql_ident(DUCK, view_name));")
    
    # Create temporary view from the SQL
    create_sql = "CREATE TEMP VIEW $(sql_ident(DUCK, view_name)) AS $(String(sql));"
    DuckDB.execute(db, create_sql)
    
    return view_name
end

"""Get column information using DuckDB's DESCRIBE command."""
function describe_output(::DuckDBFlavor, db::DuckDB.DB, viewname::AbstractString)
    query = "DESCRIBE $(sql_ident(DUCK, viewname));"
    result_df = DataFrame(DuckDB.execute(db, query))
    
    # Handle different column naming conventions in DESCRIBE output
    column_names = lowercase.(String.(names(result_df)))
    
    # Find name and type columns (try different possible names)
    name_idx = something(
        findfirst(==("column_name"), column_names),
        findfirst(==("name"), column_names),
        nothing
    )
    type_idx = something(
        findfirst(==("column_type"), column_names),
        findfirst(==("type"), column_names),
        nothing
    )
    
    if name_idx === nothing || type_idx === nothing
        error("Could not find column name/type in DESCRIBE output. Available columns: $(names(result_df))")
    end
    
    # Extract and rename columns
    output = result_df[:, [name_idx, type_idx]]
    rename!(output, [names(output)[1] => :column_name, names(output)[2] => :data_type])
    
    return output
end

# ───────── Relation Discovery ─────────

"""Parse referenced tables from view definition, excluding CTEs."""
function referenced_relations(::DuckDBFlavor, db::DuckDB.DB, viewname::AbstractString, ::AbstractString)
    # Get the actual SQL from the view definition
    view_sql_df = DataFrame(DuckDB.execute(db, "SELECT sql FROM duckdb_views() WHERE view_name = ?", (viewname,)))
    isempty(view_sql_df) && return Tuple{Union{Missing,String},String}[]
    
    full_sql = String(view_sql_df.sql[1])
    cte_names = extract_cte_names(full_sql)
    final_select = final_select_segment(full_sql)
    
    # Pattern to match table references in FROM/JOIN clauses
    table_pattern = r"""
        \b(?:from|join)\s+
        (
          (?:"[^"]+"|\w+)             # table or schema name
          (?:\.(?:"[^"]+"|\w+))?      # optional .table_name
        )
    """ix
    
    relations = Tuple{Union{Missing,String},String}[]
    
    for match in eachmatch(table_pattern, final_select)
        relation_ref = match.captures[1]
        parts = split(relation_ref, '.')
        
        if length(parts) == 2
            # Schema-qualified: schema.table
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

"""Load column descriptions from DuckDB catalog."""
function load_all_column_descriptions(::DuckDBFlavor, db::DuckDB.DB, rels)
    isempty(rels) && return DataFrame(
        schema_name=String[], table_name=String[], 
        column_name=String[], description=String[]
    )
    
    # Get all column information
    all_columns = DataFrame(DuckDB.execute(db, "SELECT * FROM duckdb_columns();"))
    
    # Identify column names (handle schema presence)
    schema_col = hascol(all_columns, "schema_name") ? colname(all_columns, "schema_name") : nothing
    table_col = colname(all_columns, "table_name")
    column_col = colname(all_columns, "column_name")
    comment_col = hascol(all_columns, "comment") ? colname(all_columns, "comment") : nothing
    
    # Build set of wanted relations for efficient lookup
    wanted_relations = Set(rels)
    
    # Filter to only wanted tables
    matching_rows = Bool[]
    for row in eachrow(all_columns)
        schema_val = schema_col === nothing ? missing : row[schema_col]
        schema_str = (ismissing(schema_val) || String(schema_val) == "") ? missing : String(schema_val)
        table_str = String(row[table_col])
        
        is_wanted = ((schema_str, table_str) in wanted_relations) || 
                   ((missing, table_str) in wanted_relations)
        push!(matching_rows, is_wanted)
    end
    
    filtered_columns = all_columns[matching_rows, :]
    
    # Build standardized output
    return DataFrame(
        schema_name = schema_col === nothing ? 
                     Vector{Union{Missing,String}}(missing, nrow(filtered_columns)) :
                     String.(coalesce.(filtered_columns[!, schema_col], "")),
        table_name = String.(filtered_columns[!, table_col]),
        column_name = String.(filtered_columns[!, column_col]),
        description = comment_col === nothing ? 
                     Vector{String}(fill("", nrow(filtered_columns))) :
                     String.(coalesce.(filtered_columns[!, comment_col], ""))
    )
end

"""Load foreign key relationships from DuckDB constraints."""
function load_fk_edges(::DuckDBFlavor, db::DuckDB.DB, rels)
    isempty(rels) && return DataFrame(
        rel_schema=String[], rel_table=String[], src_column=String[],
        ref_schema=String[], ref_table=String[], ref_column=String[]
    )
    
    # Get all constraint information
    all_constraints = DataFrame(DuckDB.execute(db, "SELECT * FROM duckdb_constraints();"))
    wanted_relations = Set(rels)
    
    fk_edges = DataFrame(
        rel_schema=String[], rel_table=String[], src_column=String[],
        ref_schema=String[], ref_table=String[], ref_column=String[]
    )
    
    for row in eachrow(all_constraints)
        # Only process foreign key constraints
        String(row.constraint_type) == "FOREIGN KEY" || continue
        
        # Check if this table is in our wanted set
        schema_val = hascol(all_constraints, "schema_name") ? row.schema_name : missing
        table_val = row.table_name
        schema_str = (ismissing(schema_val) || String(schema_val) == "") ? missing : String(schema_val)
        table_str = String(table_val)
        
        is_wanted = ((schema_str, table_str) in wanted_relations) || 
                   ((missing, table_str) in wanted_relations)
        is_wanted || continue
        
        # Extract FK details
        src_columns = hascol(all_constraints, "column_names") ? row.column_names : missing
        ref_schema = hascol(all_constraints, "ref_schema_name") ? row.ref_schema_name : missing  
        ref_table = hascol(all_constraints, "ref_table_name") ? row.ref_table_name : missing
        ref_columns = hascol(all_constraints, "ref_columns") ? row.ref_columns : missing
        
        # Skip if missing required information
        any(ismissing, [src_columns, ref_table, ref_columns]) && continue
        
        # Normalize schema names
        rel_schema_str = schema_str === missing ? "" : schema_str
        ref_schema_str = (ismissing(ref_schema) || String(ref_schema) == "") ? "" : String(ref_schema)
        
        # Add all column pairs for this FK constraint
        @assert length(src_columns) == length(ref_columns)
        for (src_col, ref_col) in zip(src_columns, ref_columns)
            push!(fk_edges, (
                String(rel_schema_str), String(table_str), String(src_col),
                String(ref_schema_str), String(ref_table), String(ref_col)
            ))
        end
    end
    
    return fk_edges
end

# ───────── Vocabulary and Sampling Functions ─────────

"""Check if a DuckDB data type is text-based."""
is_text_type_duck(data_type::AbstractString) = begin
    upper_type = uppercase(String(data_type))
    return occursin("CHAR", upper_type) || upper_type in ("TEXT", "STRING", "VARCHAR")
end

"""Find the best label column for vocabulary display."""
function pick_label_column(::DuckDBFlavor, db::DuckDB.DB, schema::AbstractString, table::AbstractString)
    query = """
        SELECT column_name, data_type
        FROM duckdb_columns()
        WHERE table_name = ?
          AND (schema_name = ? OR (? IS NULL AND (schema_name IS NULL OR schema_name='')))
        ORDER BY column_index;
    """
    
    schema_param = isempty(schema) ? missing : schema
    columns = DataFrame(DuckDB.execute(db, query, (table, schema_param, schema_param)))
    isempty(columns) && return missing
    
    # Prefer columns with label-like names
    preferred_patterns = r"(?i)^(label|name|title|desc|description)$"
    preferred_cols = filter(row -> 
        is_text_type_duck(String(row.data_type)) && 
        occursin(preferred_patterns, String(row.column_name)), columns)
    
    !isempty(preferred_cols) && return String(preferred_cols.column_name[1])
    
    # Fall back to any text column
    text_cols = filter(row -> is_text_type_duck(String(row.data_type)), columns)
    !isempty(text_cols) && return String(text_cols.column_name[1])
    
    return missing
end

"""Sample vocabulary values from a table."""
function sample_vocab(::DuckDBFlavor, db::DuckDB.DB, schema::AbstractString, table::AbstractString,
                     codecol::AbstractString, labelcol::Union{AbstractString,Missing}, max_rows::Int)
    
    qualified_table = qualify(DUCK, isempty(schema) ? missing : schema, table)
    code_col_quoted = sql_ident(DUCK, codecol)
    
    if labelcol === missing
        query = string(
            "SELECT ", code_col_quoted, " AS code ",
            "FROM ", qualified_table, " ",
            "ORDER BY 1 LIMIT ?;"
        )
    else
        label_col_quoted = sql_ident(DUCK, labelcol)
        query = string(
            "SELECT ", code_col_quoted, " AS code, ", label_col_quoted, " AS label ",
            "FROM ", qualified_table, " ",
            "ORDER BY 1 LIMIT ?;"
        )
    end
    
    return DataFrame(DuckDB.execute(db, query, (max_rows,)))
end

"""Check if table exceeds row threshold."""
function table_exceeds_threshold(::DuckDBFlavor, db::DuckDB.DB, schema::AbstractString, table::AbstractString; threshold::Int)
    qualified_table = qualify(DUCK, isempty(schema) ? missing : schema, table)
    query = "SELECT 1 FROM $(qualified_table) LIMIT ?;"
    
    result = DataFrame(DuckDB.execute(db, query, (threshold + 1,)))
    return nrow(result) > threshold
end

# ───────── Advanced Vocabulary Processing ─────────

"""Try to get ENUM values from catalog metadata."""
function get_enum_values_from_catalog(db::DuckDB.DB, schema::AbstractString, table::AbstractString, 
                                     column::AbstractString, max_vocab::Int)
    try
        # Try newer DuckDB versions with enum metadata
        query = """
            SELECT ev.enum_value AS code
            FROM duckdb_enum_values() ev
            JOIN duckdb_columns() c
              ON c.user_defined_type_schema = ev.type_schema
             AND c.user_defined_type_name   = ev.type_name
            WHERE c.table_name = ?
              AND (c.schema_name = ? OR (? IS NULL AND (c.schema_name IS NULL OR c.schema_name='')))
              AND c.column_name = ?
            ORDER BY ev.enum_index
            LIMIT ?;
        """
        
        schema_param = isempty(schema) ? missing : schema
        enum_values = DataFrame(DuckDB.execute(db, query, (table, schema_param, schema_param, column, max_vocab)))
        
        return isempty(enum_values) ? String[] : String.(enum_values.code)
    catch
        return String[]  # Enum metadata not available
    end
end

"""Get ENUM values by scanning table (with safety checks)."""
function get_enum_values_by_scan(db::DuckDB.DB, schema::AbstractString, table::AbstractString, 
                                column::AbstractString, max_vocab::Int)
    # Safety check - don't scan very large tables
    if table_exceeds_threshold(DUCK, db, schema, table; threshold=500_000)
        return String[]
    end
    
    try
        qualified_table = qualify(DUCK, isempty(schema) ? missing : schema, table)
        quoted_column = sql_ident(DUCK, column)
        
        query = "SELECT DISTINCT $(quoted_column) AS code FROM $(qualified_table) ORDER BY 1 LIMIT ?;"
        distinct_values = DataFrame(DuckDB.execute(db, query, (max_vocab,)))
        
        return isempty(distinct_values) ? String[] : String.(distinct_values.code)
    catch
        return String[]
    end
end

"""Check if a column is an ENUM type."""
function is_enum_column(db::DuckDB.DB, schema::AbstractString, table::AbstractString, column::AbstractString)
    try
        query = """
            SELECT data_type
            FROM duckdb_columns()
            WHERE table_name = ? 
              AND (schema_name = ? OR (? IS NULL AND (schema_name IS NULL OR schema_name='')))
              AND column_name = ?;
        """
        
        schema_param = isempty(schema) ? missing : schema
        type_info = DataFrame(DuckDB.execute(db, query, (table, schema_param, schema_param, column)))
        
        if !isempty(type_info)
            return occursin(r"(?i)\benum\b", String(type_info[1, :data_type]))
        end
    catch
        # Ignore errors
    end
    return false
end

"""Post-process vocabulary by examining ENUM types."""
function postprocess_vocab!(::DuckDBFlavor, db::DuckDB.DB, cols::DataFrame; max_vocab::Int=200)
    for i in 1:nrow(cols)
        # Skip if already has vocabulary or missing source info
        if !ismissing(cols.vocabulary_relation[i]) || 
           ismissing(cols.source_relation[i]) || 
           ismissing(cols.base_column[i])
            continue
        end
        
        # Parse source relation
        schema, table = _split_relkey(String(cols.source_relation[i]))
        column = String(cols.base_column[i])
        
        # Check if this column is an ENUM type
        is_enum_column(db, schema, table, column) || continue
        
        # Try to get ENUM values from catalog first
        enum_values = get_enum_values_from_catalog(db, schema, table, column, max_vocab)
        
        # Fall back to table scan if catalog method failed
        if isempty(enum_values)
            enum_values = get_enum_values_by_scan(db, schema, table, column, max_vocab)
        end
        
        # Set vocabulary information if we found values
        if !isempty(enum_values)
            schema_prefix = isempty(schema) ? "" : "$(schema)."
            cols.vocabulary_relation[i] = "enum:$(schema_prefix)$(table).$(column)"
            cols.code_column[i] = "literal"
            cols.label_column[i] = missing
            cols.vocab_sample[i] = DataFrame(code = enum_values)
            cols.vocab_skipped[i] = false
        end
    end
    
    return cols
end

# ───────── Utility Functions ─────────

"""Get table column metadata for structure analysis."""
function get_table_columns_metadata(::DuckDBFlavor, db::DuckDB.DB, schema::AbstractString, table::AbstractString)
    query = """
    SELECT column_name, data_type
    FROM duckdb_columns()
    WHERE table_name = ? 
      AND (schema_name = ? OR (? IS NULL AND (schema_name IS NULL OR schema_name='')))
    ORDER BY column_index;
    """
    
    try
        schema_param = isempty(schema) ? missing : schema
        result = DataFrame(DuckDB.execute(db, query, (table, schema_param, schema_param)))
        return result
    catch e
        @warn "Failed to get table columns metadata for $schema.$table: $e"
        return DataFrame(column_name=String[], data_type=String[])
    end
end

# ───────── Public Interface ─────────

"""
Main entry point for describing DuckDB query variables.

Uses DuckDB's catalog functions (duckdb_views, duckdb_columns, duckdb_constraints)
for comprehensive metadata discovery and ENUM vocabulary extraction.
"""
function describe_query_variables_duckdb(db::DuckDB.DB, sql::AbstractString;
                                        max_vocab::Int=50, vocab_row_threshold::Int=5_000)
    return describe_query_variables(DUCK, db, sql; 
                                   max_vocab=max_vocab, 
                                   vocab_row_threshold=vocab_row_threshold)
end
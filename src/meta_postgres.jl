########################
# meta_postgres.jl
########################

# ──────── PostgreSQL Database Metadata Module ────────
#
# This module provides PostgreSQL-specific implementations for analyzing
# SQL query metadata using PostgreSQL's rich system catalogs.
#
# Key Features:
# • Direct query analysis using temporary views
# • ENUM type support with value extraction  
# • DOMAIN type resolution
# • Comprehensive column descriptions from system catalogs
# • Integration with standard meta_common.jl interface
#
# Dependencies: LibPQ.jl for native PostgreSQL connectivity
# ─────────────────────────────────────────────────────

using LibPQ
using DataFrames
using DBInterface

# ───────── Type Definitions ─────────

"""PostgreSQL-specific database flavor implementation."""
struct PostgresFlavor <: DBFlavor end

# ───────── Core Database Operations (Standard Interface) ─────────

"""Create temporary view for PostgreSQL query analysis."""
function make_temp_view(::PostgresFlavor, conn::LibPQ.Connection, sql::AbstractString)::String
    # Clean up the SQL and generate unique view name
    clean_sql = replace(strip(String(sql)), r";+\s*\z" => "")
    view_name = "_tmp_meta_view_" * string(rand(UInt32); base=16)
    
    # Drop existing view if it exists and create new one
    drop_sql = "DROP VIEW IF EXISTS $(view_name);"
    create_sql = "CREATE TEMP VIEW $(view_name) AS $(clean_sql);"
    
    LibPQ.execute(conn, drop_sql)
    LibPQ.execute(conn, create_sql)
    
    return view_name
end

"""Get column information from temporary view."""
function describe_output(::PostgresFlavor, conn::LibPQ.Connection, view_name::AbstractString)::DataFrame
    query = """
    SELECT 
        column_name,
        data_type,
        udt_name
    FROM information_schema.columns 
    WHERE table_name = \$1
    ORDER BY ordinal_position;
    """
    
    try
        result = LibPQ.execute(conn, query, (view_name,))
        df = DataFrame(result)
        
        # For PostgreSQL, when data_type is 'USER-DEFINED', the actual type name is in udt_name
        # We need to use udt_name for type checking but keep both for reference
        for i in 1:nrow(df)
            if String(df.data_type[i]) == "USER-DEFINED"
                # Replace data_type with the actual user-defined type name
                df.data_type[i] = df.udt_name[i]
            end
        end
        
        # Return only the required columns for the standard interface
        return select(df, :column_name, :data_type)
    catch e
        @warn "Failed to describe output for view '$view_name': $e"
        return DataFrame(column_name=String[], data_type=String[])
    end
end

"""Parse referenced tables from PostgreSQL query (simplified)."""
function referenced_relations(::PostgresFlavor, conn::LibPQ.Connection, view_name::AbstractString, sql::AbstractString)
    cte_names = extract_cte_names(sql)
    final_select = final_select_segment(sql)
    
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
                push!(relations, (missing, table_name))  # Default to public schema
            end
        end
    end
    
    return unique(relations)
end

"""Load column descriptions from PostgreSQL system catalogs."""
function load_all_column_descriptions(::PostgresFlavor, conn::LibPQ.Connection, relations)
    isempty(relations) && return DataFrame(
        schema_name=String[], table_name=String[], 
        column_name=String[], description=String[]
    )
    
    # Normalize relations (handle missing schemas)
    normalized_rels = [(String(ismissing(s) ? "public" : s), String(t)) for (s, t) in relations]
    values_clause = build_values_clause(normalized_rels)
    
    query = """
    SELECT
      n.nspname  AS schema_name,
      c.relname  AS table_name,
      a.attname  AS column_name,
      d.description AS description
    FROM (VALUES $(values_clause)) v(schema_name, table_name)
    JOIN pg_namespace n ON n.nspname = v.schema_name
    JOIN pg_class     c ON c.relnamespace = n.oid AND c.relname = v.table_name
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
    LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum
    WHERE d.description IS NOT NULL;
    """
    
    try
        result = LibPQ.execute(conn, query)
        return DataFrame(result)
    catch e
        @warn "Failed to load column descriptions: $e"
        return DataFrame(
            schema_name=String[], table_name=String[], 
            column_name=String[], description=String[]
        )
    end
end

"""Load foreign key relationships from PostgreSQL system catalogs."""
function load_fk_edges(::PostgresFlavor, conn::LibPQ.Connection, relations)
    isempty(relations) && return DataFrame(
        rel_schema=String[], rel_table=String[], src_column=String[],
        ref_schema=String[], ref_table=String[], ref_column=String[]
    )
    
    normalized_rels = [(String(ismissing(s) ? "public" : s), String(t)) for (s, t) in relations]
    values_clause = build_values_clause(normalized_rels)
    
    query = """
    SELECT 
        kcu.table_schema AS rel_schema,
        kcu.table_name AS rel_table,
        kcu.column_name AS src_column,
        ccu.table_schema AS ref_schema,
        ccu.table_name AS ref_table,
        ccu.column_name AS ref_column
    FROM (VALUES $(values_clause)) v(schema_name, table_name)
    JOIN information_schema.key_column_usage kcu 
         ON kcu.table_schema = v.schema_name AND kcu.table_name = v.table_name
    JOIN information_schema.table_constraints tc 
         ON tc.constraint_name = kcu.constraint_name 
         AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu 
         ON ccu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY';
    """
    
    try
        result = LibPQ.execute(conn, query)
        return DataFrame(result)
    catch e
        @warn "Failed to load foreign key edges: $e"
        return DataFrame(
            rel_schema=String[], rel_table=String[], src_column=String[],
            ref_schema=String[], ref_table=String[], ref_column=String[]
        )
    end
end

"""Check if table exceeds row threshold for vocabulary sampling."""
function table_exceeds_threshold(::PostgresFlavor, conn::LibPQ.Connection, schema::AbstractString, table::AbstractString; threshold::Int=5000)::Bool
    qualified_table = "\"$(schema)\".\"$(table)\""
    query = "SELECT 1 FROM $(qualified_table) LIMIT \$1;"
    
    try
        result = LibPQ.execute(conn, query, (threshold + 1,))
        return LibPQ.num_rows(result) > threshold
    catch e
        @warn "Failed to check table threshold: $e"
        return false
    end
end

"""Sample vocabulary from a specific column."""
function sample_vocab(::PostgresFlavor, conn::LibPQ.Connection, schema::AbstractString, 
                     table::AbstractString, codecol::AbstractString, labelcol::Union{AbstractString,Missing}, max_rows::Int)
    qualified_table = "\"$(schema)\".\"$(table)\""
    code_col_quoted = "\"$(codecol)\""
    
    if labelcol === missing
        query = string(
            "SELECT DISTINCT ", code_col_quoted, " AS code ",
            "FROM ", qualified_table, " ",
            "WHERE ", code_col_quoted, " IS NOT NULL ",
            "ORDER BY 1 LIMIT \$1;"
        )
    else
        label_col_quoted = "\"$(labelcol)\""
        query = string(
            "SELECT DISTINCT ", code_col_quoted, " AS code, ", label_col_quoted, " AS label ",
            "FROM ", qualified_table, " ",
            "WHERE ", code_col_quoted, " IS NOT NULL ",
            "ORDER BY 1 LIMIT \$1;"
        )
    end
    
    try
        result = LibPQ.execute(conn, query, (max_rows,))
        return DataFrame(result)
    catch e
        @warn "Failed to sample vocabulary from $schema.$table.$codecol: $e"
        return DataFrame(code=String[])
    end
end

"""Enhanced vocabulary processing for PostgreSQL (handles ENUM types)."""
function postprocess_vocab!(::PostgresFlavor, conn::LibPQ.Connection, cols::DataFrame; max_vocab::Int=200)
    # Process all columns to check for ENUM types
    for i in 1:nrow(cols)
        # Skip if already has vocabulary
        if !ismissing(cols.vocab_sample[i])
            continue
        end
        
        data_type = String(cols.data_type[i])
        
        # Directly check if this type is an ENUM in PostgreSQL system catalogs
        if is_enum_type(conn, data_type)
            # Get ENUM values
            enum_values = get_enum_values(conn, data_type)
            
            if !isempty(enum_values)
                # Update vocabulary columns
                vocab_sample = length(enum_values) > max_vocab ? enum_values[1:max_vocab] : enum_values
                cols.vocab_sample[i] = DataFrame(code = vocab_sample)
                cols.vocab_skipped[i] = max(0, length(enum_values) - max_vocab)
                
                # Set vocabulary relation to indicate it's from ENUM type
                cols.vocabulary_relation[i] = "ENUM:" * data_type
                cols.code_column[i] = cols.column_name[i]
                cols.label_column[i] = missing
            end
        end
    end
    
    return cols
end

"""Check if a PostgreSQL type is an ENUM by querying system catalogs."""
function is_enum_type(conn::LibPQ.Connection, type_name::AbstractString)::Bool
    query = """
    SELECT 1
    FROM pg_type t
    WHERE t.typname = \$1 AND t.typtype = 'e';
    """
    
    try
        result = LibPQ.execute(conn, query, (type_name,))
        return LibPQ.num_rows(result) > 0
    catch
        return false
    end
end

"""Get ENUM values from PostgreSQL system catalog."""
function get_enum_values(conn::LibPQ.Connection, enum_type::AbstractString)::Vector{String}
    query = """
    SELECT e.enumlabel
    FROM pg_type t
    JOIN pg_enum e ON t.oid = e.enumtypid
    WHERE t.typname = \$1
    ORDER BY e.enumsortorder;
    """
    
    try
        result = LibPQ.execute(conn, query, (enum_type,))
        return [String(row.enumlabel) for row in result]
    catch e
        @warn "Failed to get ENUM values for type '$enum_type': $e"
        return String[]
    end
end

# ───────── Utility Functions ─────────

"""Safely escape SQL string literals for PostgreSQL."""
escape_sql_string_pg(s::AbstractString) = replace(String(s), "'" => "''")

"""Build VALUES clause for PostgreSQL queries."""
function build_values_clause(pairs::Vector{Tuple{String,String}})
    value_strings = map(pairs) do (schema, table)
        escaped_schema = escape_sql_string_pg(schema)
        escaped_table = escape_sql_string_pg(table)
        "('$(escaped_schema)', '$(escaped_table)')"
    end
    return join(value_strings, ", ")
end

"""Get table column metadata for structure analysis."""
function get_table_columns_metadata(::PostgresFlavor, conn::LibPQ.Connection, schema::AbstractString, table::AbstractString)
    query = """
    SELECT column_name, data_type
    FROM information_schema.columns 
    WHERE table_schema = \$1 AND table_name = \$2
    ORDER BY ordinal_position;
    """
    
    try
        result = LibPQ.execute(conn, query, (schema, table))
        return DataFrame(result)
    catch e
        @warn "Failed to get table columns metadata for $schema.$table: $e"
        return DataFrame(column_name=String[], data_type=String[])
    end
end

# ───────── Convenience Functions ─────────

"""
Describe variables in a PostgreSQL query using the standard interface.

This function uses the standardized describe_query_variables from meta_common.jl
and returns the same DataFrame structure as other database flavors.

# Arguments
- `conn::LibPQ.Connection`: Active PostgreSQL connection
- `sql::AbstractString`: SQL query to analyze
- `max_vocab::Int=200`: Maximum vocabulary sample size per column  
- `vocab_row_threshold::Int=5000`: Row threshold for vocabulary sampling

# Returns
- `DataFrame`: Standardized metadata with columns:
  - column_name, data_type, source_relation, base_column
  - description, vocabulary_relation, code_column, label_column
  - vocab_sample, vocab_skipped
"""
function describe_query_variables_postgres(conn::LibPQ.Connection, sql::AbstractString; 
                                          max_vocab::Int=200, vocab_row_threshold::Int=5000)::DataFrame
    return describe_query_variables(PostgresFlavor(), conn, sql; 
                                  max_vocab=max_vocab, vocab_row_threshold=vocab_row_threshold)
end

# ───────── Export Definitions ─────────

# Export the main analysis function and flavor type
export PostgresFlavor, describe_query_variables_postgres
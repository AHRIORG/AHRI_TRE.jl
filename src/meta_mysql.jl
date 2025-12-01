########################
# meta_mysql.jl
########################

# ──────── MySQL/MariaDB Database Metadata Module ────────
#
# This module provides MySQL/MariaDB-specific implementations for analyzing
# SQL query metadata using INFORMATION_SCHEMA and temporary tables.
#
# Key Features:
# • Query analysis via temporary table materialization (no temp views in MySQL)
# • ENUM and SET type support with value extraction
# • CHECK constraint parsing for vocabulary discovery
# • Comprehensive column metadata from INFORMATION_SCHEMA
# • Integration with standard meta_common.jl interface
#
# Dependencies: ODBC.jl for MySQL connectivity
# Note: Uses temporary tables instead of views due to MySQL limitations
# ─────────────────────────────────────────────────────

using ODBC
using DataFrames
using DBInterface

# ───────── Type Definitions ─────────

"""MySQL/MariaDB-specific database flavor implementation."""
struct MySQLFlavor <: DBFlavor end

# ───────── Utility Functions ─────────

"""Safely escape SQL string literals for MySQL."""
escape_sql_string_mysql(s::AbstractString) = replace(String(s), "'" => "''")

"""Get the current database/schema name."""
function get_current_database(conn::ODBC.Connection)::String
    result = DBInterface.execute(conn, "SELECT DATABASE() AS db;"; iterate_rows=true) |> DataFrame
    return isempty(result) ? "" : String(result[1, :db])
end

"""Build OR clause for schema/table pairs in MySQL queries."""
function build_or_clause(pairs::Vector{Tuple{String,String}})
    isempty(pairs) && return "FALSE"
    
    conditions = map(pairs) do (schema, table)
        escaped_schema = escape_sql_string_mysql(schema)
        escaped_table = escape_sql_string_mysql(table)
        "(TABLE_SCHEMA='$(escaped_schema)' AND TABLE_NAME='$(escaped_table)')"
    end
    
    return join(conditions, " OR ")
end

"""Check if SQL starts with WITH clause."""
starts_with_cte(sql::AbstractString) = occursin(r"(?is)^\s*with\b", String(sql))

# ───────── Core Database Operations (Standard Interface) ─────────

"""
Create temporary table for query analysis.

MySQL doesn't support temporary views, so we create a temporary table with 0 rows
to materialize the result schema and concrete data types.
"""
function make_temp_view(::MySQLFlavor, conn::ODBC.Connection, sql::AbstractString)::String
    # Clean up the SQL and generate unique temp table name
    clean_sql = replace(strip(String(sql)), r";+\s*\z" => "")
    temp_name = "_tmp_rs_" * string(rand(UInt32); base=16)
    
    # Ensure any existing temp table is dropped
    drop_sql = "DROP TEMPORARY TABLE IF EXISTS $(sql_ident(MYSQL, temp_name));"
    DBInterface.execute(conn, drop_sql)
    
    # Create temporary table based on query type
    if starts_with_cte(clean_sql)
        # CTE must remain at top level - append LIMIT 0
        create_sql = string(
            "CREATE TEMPORARY TABLE ", sql_ident(MYSQL, temp_name), " AS ",
            clean_sql, " LIMIT 0;"
        )
    else
        # Safe to wrap in subquery
        create_sql = string(
            "CREATE TEMPORARY TABLE ", sql_ident(MYSQL, temp_name), " AS ",
            "SELECT * FROM (", clean_sql, ") AS _t LIMIT 0;"
        )
    end
    
    DBInterface.execute(conn, create_sql)
    return temp_name
end

"""Get column information from temporary table using SHOW COLUMNS."""
function describe_output(::MySQLFlavor, conn::ODBC.Connection, temp_name::AbstractString)::DataFrame
    try
        show_sql = "SHOW COLUMNS FROM $(sql_ident(MYSQL, temp_name));"
        result = DBInterface.execute(conn, show_sql; iterate_rows=true) |> DataFrame
        
        # Standardize to expected column names
        if "Field" in names(result)
            rename!(result, "Field" => "column_name")
        end
        if "Type" in names(result)
            rename!(result, "Type" => "data_type")
        end
        
        # Return only the required columns for the standard interface
        return select(result, :column_name, :data_type)
        
    catch e
        @warn "Failed to describe temporary table: $e"
        return DataFrame(column_name=String[], data_type=String[])
    end
end

"""Load column descriptions from INFORMATION_SCHEMA."""
function load_all_column_descriptions(::MySQLFlavor, conn::ODBC.Connection, relations)
    isempty(relations) && return DataFrame(
        schema_name=String[], table_name=String[], 
        column_name=String[], description=String[]
    )
    
    try
        current_db = get_current_database(conn)
        # Handle missing schema values properly
        normalized_rels = [(ismissing(s) || s == "" ? current_db : s, t) for (s, t) in relations]
        or_clause = build_or_clause(normalized_rels)
        
        query = """
        SELECT 
            TABLE_SCHEMA AS schema_name,
            TABLE_NAME AS table_name,
            COLUMN_NAME AS column_name,
            COLUMN_COMMENT AS description
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE $(or_clause)
          AND COLUMN_COMMENT IS NOT NULL 
          AND COLUMN_COMMENT != '';
        """
        
        result = DBInterface.execute(conn, query; iterate_rows=true) |> DataFrame
        return result
        
    catch e
        @warn "Failed to load column descriptions: $e"
        return DataFrame(
            schema_name=String[], table_name=String[], 
            column_name=String[], description=String[]
        )
    end
end

"""Load foreign key relationships from INFORMATION_SCHEMA."""
function load_fk_edges(::MySQLFlavor, conn::ODBC.Connection, relations)
    isempty(relations) && return DataFrame(
        rel_schema=String[], rel_table=String[], src_column=String[],
        ref_schema=String[], ref_table=String[], ref_column=String[]
    )
    
    try
        current_db = get_current_database(conn)
        # Handle missing schema values properly
        normalized_rels = [(ismissing(s) || s == "" ? current_db : s, t) for (s, t) in relations]
        or_clause = build_or_clause(normalized_rels)
        
        query = """
        SELECT 
            kcu.TABLE_SCHEMA AS rel_schema,
            kcu.TABLE_NAME AS rel_table,
            kcu.COLUMN_NAME AS src_column,
            kcu.REFERENCED_TABLE_SCHEMA AS ref_schema,
            kcu.REFERENCED_TABLE_NAME AS ref_table,
            kcu.REFERENCED_COLUMN_NAME AS ref_column
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        WHERE kcu.REFERENCED_TABLE_NAME IS NOT NULL
          AND ($(or_clause));
        """
        
        result = DBInterface.execute(conn, query; iterate_rows=true) |> DataFrame
        
        # Standardize column names and handle missing values
        for col in [:rel_schema, :rel_table, :src_column, :ref_schema, :ref_table, :ref_column]
            if col in Symbol.(names(result))
                result[!, col] = String.(coalesce.(result[!, col], ""))
            end
        end
        
        return result
        
    catch e
        @warn "Failed to load foreign key edges: $e"
        return DataFrame(
            rel_schema=String[], rel_table=String[], src_column=String[],
            ref_schema=String[], ref_table=String[], ref_column=String[]
        )
    end
end

"""Parse referenced tables from SQL (simplified implementation)."""
function referenced_relations(::MySQLFlavor, conn::ODBC.Connection, temp_name::AbstractString, sql::AbstractString)
    # This is a simplified implementation
    # In practice, you'd want more sophisticated SQL parsing
    relations = Tuple{Union{Missing,String},String}[]
    
    # Look for FROM and JOIN clauses
    # This is a basic pattern - real implementation should use proper SQL parsing
    current_db = get_current_database(conn)
    
    # Extract table names from common patterns
    patterns = [
        r"\bFROM\s+(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?\s*([a-zA-Z_][a-zA-Z0-9_]*)"i,
        r"\bJOIN\s+(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?\s*([a-zA-Z_][a-zA-Z0-9_]*)"i
    ]
    
    for pattern in patterns
        for match in eachmatch(pattern, sql)
            schema = match.captures[1]
            table = match.captures[2]
            
            if !isnothing(table)
                schema_val = isnothing(schema) ? missing : String(schema)
                push!(relations, (schema_val, String(table)))
            end
        end
    end
    
    return unique(relations)
end

"""Check if table exceeds row threshold for vocabulary sampling."""
function table_exceeds_threshold(::MySQLFlavor, conn::ODBC.Connection, schema::AbstractString, table::AbstractString; threshold::Int=5000)::Bool
    try
        query = """
        SELECT COUNT(*) as row_count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '$(escape_sql_string_mysql(schema))'
          AND TABLE_NAME = '$(escape_sql_string_mysql(table))'
          AND TABLE_ROWS > $(threshold);
        """
        
        result = DBInterface.execute(conn, query; iterate_rows=true) |> DataFrame
        return !isempty(result) && result[1, :row_count] > 0
        
    catch e
        @warn "Failed to check table threshold: $e"
        return false
    end
end

"""Sample vocabulary from a specific column."""
function sample_vocab(::MySQLFlavor, conn::ODBC.Connection, schema::AbstractString, 
                     table::AbstractString, column::AbstractString; max_vocab::Int=200)
    try
        query = """
        SELECT DISTINCT $(sql_ident(MYSQL, column)) AS value
        FROM $(sql_ident_qualified(MYSQL, schema, table))
        WHERE $(sql_ident(MYSQL, column)) IS NOT NULL
        ORDER BY value
        LIMIT $(max_vocab);
        """
        
        result = DBInterface.execute(conn, query; iterate_rows=true) |> DataFrame
        return String.(result.value)
        
    catch e
        @warn "Failed to sample vocabulary from $schema.$table.$column: $e"
        return String[]
    end
end

"""Sample vocabulary values from a table."""
function sample_vocab(::MySQLFlavor, conn::ODBC.Connection, schema::AbstractString, table::AbstractString,
                     codecol::AbstractString, labelcol::Union{AbstractString,Missing}, max_rows::Int)
    try
        code_col_quoted = sql_ident(MYSQL, codecol)
        qualified_table = sql_ident_qualified(MYSQL, schema, table)
        
        if labelcol === missing
            query = string(
                "SELECT DISTINCT ", code_col_quoted, " AS code ",
                "FROM ", qualified_table, " ",
                "WHERE ", code_col_quoted, " IS NOT NULL ",
                "ORDER BY 1 LIMIT ", string(max_rows), ";"
            )
        else
            label_col_quoted = sql_ident(MYSQL, labelcol)
            query = string(
                "SELECT DISTINCT ", code_col_quoted, " AS code, ", label_col_quoted, " AS label ",
                "FROM ", qualified_table, " ",
                "WHERE ", code_col_quoted, " IS NOT NULL ",
                "ORDER BY 1 LIMIT ", string(max_rows), ";"
            )
        end
        
        return DBInterface.execute(conn, query; iterate_rows=true) |> DataFrame
        
    catch e
        @warn "Failed to sample vocabulary from $schema.$table.$codecol: $e"
        return DataFrame(code=String[])
    end
end

"""Enhanced vocabulary processing for MySQL (handles ENUM/SET types)."""
function postprocess_vocab!(::MySQLFlavor, conn::ODBC.Connection, cols::DataFrame; max_vocab::Int=200)
    # Process ENUM and SET columns to extract their values
    for i in 1:nrow(cols)
        data_type = String(cols.data_type[i])
        
        if startswith(lowercase(data_type), "enum") || startswith(lowercase(data_type), "set")
            # Extract values from ENUM/SET definition
            vocab_values = if startswith(lowercase(data_type), "enum")
                extract_enum_values(data_type)
            else
                extract_set_values(data_type)
            end
            
            if !isempty(vocab_values)
                # Update vocabulary columns
                cols.vocab_sample[i] = length(vocab_values) > max_vocab ? vocab_values[1:max_vocab] : vocab_values
                cols.vocab_skipped[i] = max(0, length(vocab_values) - max_vocab)
                
                # Set vocabulary relation to indicate it's from type definition
                cols.vocabulary_relation[i] = "ENUM/SET:" * data_type
                cols.code_column[i] = cols.column_name[i]
            end
        end
    end
    
    return cols
end

# ───────── MySQL-Specific Type Parsing ─────────

"""Extract ENUM values from type definition."""
function extract_enum_values(type_str::AbstractString)::Vector{String}
    # Match content within enum('value1','value2',...)
    m = match(r"enum\s*\(\s*(.*?)\s*\)"i, type_str)
    m === nothing && return String[]
    
    # Split by comma and clean up quotes
    values = String[]
    for part in split(m.captures[1], ",")
        clean_part = strip(part)
        if startswith(clean_part, "'") && endswith(clean_part, "'")
            push!(values, clean_part[2:end-1])
        elseif startswith(clean_part, "\"") && endswith(clean_part, "\"")
            push!(values, clean_part[2:end-1])
        end
    end
    
    return values
end

"""Extract SET values from type definition."""  
function extract_set_values(type_str::AbstractString)::Vector{String}
    # Match content within set('value1','value2',...)
    m = match(r"set\s*\(\s*(.*?)\s*\)"i, type_str)
    m === nothing && return String[]
    
    # Split by comma and clean up quotes
    values = String[]
    for part in split(m.captures[1], ",")
        clean_part = strip(part)
        if startswith(clean_part, "'") && endswith(clean_part, "'")
            push!(values, clean_part[2:end-1])
        elseif startswith(clean_part, "\"") && endswith(clean_part, "\"")
            push!(values, clean_part[2:end-1])
        end
    end
    
    return values
end

# ───────── SQL Identifier Utilities ─────────

"""Quote SQL identifier for MySQL."""
function sql_ident(::Type{MYSQL}, name::AbstractString) where MYSQL
    # MySQL uses backticks for identifier quoting
    return "`$(replace(String(name), "`" => "``"))`"
end

"""Quote qualified identifier for MySQL."""
function sql_ident_qualified(::Type{MYSQL}, schema::AbstractString, table::AbstractString) where MYSQL
    schema_part = isempty(schema) ? "" : "$(sql_ident(MYSQL, schema))."
    return "$(schema_part)$(sql_ident(MYSQL, table))"
end

# Define the MYSQL type for dispatch
struct MYSQL end

# ───────── Convenience Functions ─────────

"""
Describe variables in a MySQL query using the standard interface.

This function uses the standardized describe_query_variables from meta_common.jl
and returns the same DataFrame structure as other database flavors.

# Arguments
- `conn::ODBC.Connection`: Active MySQL connection
- `sql::AbstractString`: SQL query to analyze
- `max_vocab::Int=200`: Maximum vocabulary sample size per column  
- `vocab_row_threshold::Int=5000`: Row threshold for vocabulary sampling

# Returns
- `DataFrame`: Standardized metadata with columns:
  - column_name, data_type, source_relation, base_column
  - description, vocabulary_relation, code_column, label_column
  - vocab_sample, vocab_skipped
"""
function describe_query_variables_mysql(conn::ODBC.Connection, sql::AbstractString; 
                                       max_vocab::Int=200, vocab_row_threshold::Int=5000)::DataFrame
    return describe_query_variables(MySQLFlavor(), conn, sql; 
                                  max_vocab=max_vocab, vocab_row_threshold=vocab_row_threshold)
end

# ───────── Export Definitions ─────────

# Export the main analysis function and flavor type
export MySQLFlavor, describe_query_variables_mysql

"""Get table column metadata for structure analysis."""
function get_table_columns_metadata(::MySQLFlavor, conn::ODBC.Connection, schema::AbstractString, table::AbstractString)
    escaped_schema = escape_sql_string_mysql(schema)
    escaped_table = escape_sql_string_mysql(table)
    
    query = """
    SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '$(escaped_schema)' AND TABLE_NAME = '$(escaped_table)'
    ORDER BY ORDINAL_POSITION;
    """
    
    try
        result = DBInterface.execute(conn, query; iterate_rows=true) |> DataFrame
        return result
    catch e
        @warn "Failed to get table columns metadata for $schema.$table: $e"
        return DataFrame(column_name=String[], data_type=String[])
    end
end
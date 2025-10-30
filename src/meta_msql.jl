########################
# meta_mssql.jl
########################

struct MSSQLFlavor <: DBFlavor end
const MSSQL = MSSQLFlavor()

# ───────── SQL Server Specific Configuration ─────────

# MSSQL identifier quoting: [name]
sql_ident(::MSSQLFlavor, s::AbstractString) = "[" * replace(String(s), "]" => "]]") * "]"

# ───────── Core Database Operations ─────────

"""Get result set metadata using SQL Server's DMV."""
function first_result_set_meta(conn::ODBC.Connection, sql::AbstractString)::DataFrame
    query = raw"""
    SELECT
        name              AS column_name,
        system_type_name  AS data_type,
        is_nullable,
        source_server,
        source_database,
        source_schema,
        source_table,
        source_column
    FROM sys.dm_exec_describe_first_result_set(?, NULL, 1);
    """
    return DBInterface.execute(conn, query, (sql,); iterate_rows=true) |> DataFrame
end

# No temporary view needed - pass SQL directly to DMV
make_temp_view(::MSSQLFlavor, ::ODBC.Connection, sql::AbstractString) = sql

# Extract column names and types from DMV result
describe_output(::MSSQLFlavor, conn::ODBC.Connection, sql::AbstractString) =
    first_result_set_meta(conn, sql)[:, [:column_name, :data_type]]

# ───────── SQL Parsing and Relation Discovery ─────────

"""Parse FROM/JOIN clauses in final SELECT, excluding CTEs."""
function referenced_relations(::MSSQLFlavor, ::ODBC.Connection, ::AbstractString, sql::AbstractString)
    cte_names = extract_cte_names(sql)
    tail = final_select_segment(sql)
    
    # Pattern to match table references in FROM/JOIN clauses
    pattern = r"""
        \b(?:from|join)\s+
        (
          (?:
            \[[^\]]+\] | "[^"]+" | \w+     # schema or table (quoted or bare)
          )
          (?:
            \.
            (?:
              \[[^\]]+\] | "[^"]+" | \w+   # table name after schema
            )
          )?
        )
    """ix
    
    relations = Tuple{Union{Missing,String},String}[]
    
    for match in eachmatch(pattern, tail)
        rel = match.captures[1]
        parts = split(rel, '.')
        
        if length(parts) == 2
            # Schema-qualified name: schema.table
            schema = strip_identifier_quotes(parts[1])
            table = strip_identifier_quotes(parts[2])
            push!(relations, (schema, table))
        else
            # Bare table name - check if it's a CTE
            table_name = strip_identifier_quotes(parts[1])
            if !(lowercase(table_name) in cte_names)
                push!(relations, ("dbo", table_name))  # Default to dbo schema
            end
        end
    end
    
    return unique(relations)
end

# ───────── Metadata Loading Functions ─────────

"""Safely escape SQL string literals."""
_escape_sql_string(s::AbstractString) = replace(String(s), "'" => "''")

"""Build VALUES clause for schema/table pairs."""
function _build_values_clause(pairs::Vector{Tuple{String,String}})
    value_strings = map(pairs) do (schema, table)
        escaped_schema = _escape_sql_string(schema)
        escaped_table = _escape_sql_string(table)
        "(N'$(escaped_schema)', N'$(escaped_table)')"
    end
    return join(value_strings, ", ")
end

"""Load column descriptions from SQL Server extended properties."""
function load_all_column_descriptions(::MSSQLFlavor, conn::ODBC.Connection, rels)
    isempty(rels) && return DataFrame(
        schema_name=String[], table_name=String[], 
        column_name=String[], description=String[]
    )
    
    # Normalize relations (handle missing schemas)
    normalized_rels = [(String(coalesce(s, "dbo")), String(t)) for (s, t) in rels]
    values_clause = _build_values_clause(normalized_rels)
    
    query = raw"""
    SELECT
        s.name  AS schema_name,
        t.name  AS table_name,
        c.name  AS column_name,
        CAST(ep.value AS nvarchar(4000)) AS description
    FROM (VALUES $VALUES$) AS v(schema_name, table_name)
    JOIN sys.schemas s ON s.name = v.schema_name
    JOIN sys.tables  t ON t.schema_id = s.schema_id AND t.name = v.table_name
    JOIN sys.columns c ON c.object_id = t.object_id
    LEFT JOIN sys.extended_properties ep
           ON ep.major_id = t.object_id
          AND ep.minor_id = c.column_id
          AND ep.name = N'MS_Description'
    ORDER BY s.name, t.name, c.column_id;
    """
    
    final_query = replace(query, "\$VALUES\$" => values_clause)
    return DBInterface.execute(conn, final_query; iterate_rows=true) |> DataFrame
end

"""Load foreign key relationships between tables."""
function load_fk_edges(::MSSQLFlavor, conn::ODBC.Connection, rels)
    isempty(rels) && return DataFrame(
        rel_schema=String[], rel_table=String[], src_column=String[],
        ref_schema=String[], ref_table=String[], ref_column=String[]
    )
    
    normalized_rels = [(String(coalesce(s, "dbo")), String(t)) for (s, t) in rels]
    values_clause = _build_values_clause(normalized_rels)
    
    query = raw"""
    WITH parent_tables AS (
      SELECT t.object_id
      FROM (VALUES $VALUES$) v(schema_name, table_name)
      JOIN sys.schemas s ON s.name = v.schema_name
      JOIN sys.tables  t ON t.schema_id = s.schema_id AND t.name = v.table_name
    )
    SELECT
      ps.name AS rel_schema,
      pt.name AS rel_table,
      pc.name AS src_column,
      rs.name AS ref_schema,
      rt.name AS ref_table,
      rc.name AS ref_column
    FROM parent_tables p
    JOIN sys.foreign_keys fk ON fk.parent_object_id = p.object_id
    JOIN sys.tables pt ON pt.object_id = fk.parent_object_id
    JOIN sys.schemas ps ON ps.schema_id = pt.schema_id
    JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
    JOIN sys.schemas rs ON rs.schema_id = rt.schema_id
    JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
    JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id 
                        AND pc.column_id = fkc.parent_column_id
    JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id 
                        AND rc.column_id = fkc.referenced_column_id
    ORDER BY rel_schema, rel_table, src_column;
    """
    
    final_query = replace(query, "\$VALUES\$" => values_clause)
    return DBInterface.execute(conn, final_query; iterate_rows=true) |> DataFrame
end

# ───────── Vocabulary and Sampling Functions ─────────

"""Check if a data type is text-based."""
is_text_type_mssql(type_name::AbstractString) = begin
    upper_type = uppercase(String(type_name))
    return occursin("CHAR", upper_type) || upper_type in ("TEXT", "NTEXT")
end

"""Find the best label column for vocabulary display."""
function pick_label_column(::MSSQLFlavor, conn::ODBC.Connection, schema::AbstractString, table::AbstractString)
    # Use string interpolation instead of parameters for schema/table names
    escaped_schema = _escape_sql_string(schema)
    escaped_table = _escape_sql_string(table)
    
    query = """
    SELECT c.name AS column_name, t.name AS type_name
    FROM sys.schemas s
    JOIN sys.tables  tb ON tb.schema_id = s.schema_id
    JOIN sys.columns c  ON c.object_id = tb.object_id
    JOIN sys.types   t  ON t.user_type_id = c.user_type_id
    WHERE s.name = N'$(escaped_schema)' AND tb.name = N'$(escaped_table)'
    ORDER BY c.column_id;
    """
    
    columns = try
        DBInterface.execute(conn, query; iterate_rows=true) |> DataFrame
    catch e
        @warn "Failed to get columns for $schema.$table: $e"
        DataFrame()
    end
    
    isempty(columns) && return missing
    
    # Prefer columns with label-like names
    preferred_patterns = r"(?i)^(label|name|title|desc|description)$"
    preferred_cols = filter(row -> 
        is_text_type_mssql(String(row.type_name)) && 
        occursin(preferred_patterns, String(row.column_name)), columns)
    
    !isempty(preferred_cols) && return String(preferred_cols.column_name[1])
    
    # Fall back to any text column
    text_cols = filter(row -> is_text_type_mssql(String(row.type_name)), columns)
    !isempty(text_cols) && return String(text_cols.column_name[1])
    
    return missing
end

"""Sample vocabulary values from a table."""
function sample_vocab(::MSSQLFlavor, conn::ODBC.Connection, schema::AbstractString, table::AbstractString,
                     codecol::AbstractString, labelcol::Union{AbstractString,Missing}, max_rows::Int)
    
    qualified_table = qualify(MSSQL, schema, table)
    code_col_quoted = sql_ident(MSSQL, codecol)
    
    if labelcol === missing
        query = string(
            "SELECT TOP(", max_rows, ") ", code_col_quoted, " AS code ",
            "FROM ", qualified_table, " ",
            "ORDER BY 1;"
        )
    else
        label_col_quoted = sql_ident(MSSQL, labelcol)
        query = string(
            "SELECT TOP(", max_rows, ") ", code_col_quoted, " AS code, ", label_col_quoted, " AS label ",
            "FROM ", qualified_table, " ",
            "ORDER BY 1;"
        )
    end
    
    return try
        DataFrame(DBInterface.execute(conn, query; iterate_rows=true))
    catch e
        @warn "Failed to sample vocabulary from $schema.$table.$codecol: $e"
        DataFrame(code=String[])
    end
end

"""Check if table exceeds row threshold."""
function table_exceeds_threshold(::MSSQLFlavor, conn::ODBC.Connection, schema::AbstractString, table::AbstractString; threshold::Int)
    qualified_table = qualify(MSSQL, schema, table)
    query = "SELECT TOP($(threshold + 1)) 1 FROM $(qualified_table);"
    
    result = try
        DataFrame(DBInterface.execute(conn, query; iterate_rows=true))
    catch e
        @warn "Failed to check table threshold for $schema.$table: $e"
        DataFrame()
    end
    
    return nrow(result) > threshold
end

# ───────── Advanced Vocabulary Processing ─────────

"""Extract vocabulary from CHECK constraints."""
function extract_check_constraint_values(definition::AbstractString)::Vector{String}
    # Look for IN ('value1', 'value2', ...) patterns
    in_match = match(r"(?is)\bIN\s*\((.*?)\)", definition)
    in_match === nothing && return String[]
    
    inner_content = in_match.captures[1]
    values = String[]
    
    # Extract quoted string literals
    for string_match in eachmatch(r"'((?:''|[^'])*)'", inner_content)
        # Unescape doubled quotes
        unescaped_value = replace(string_match.captures[1], "''" => "'")
        push!(values, unescaped_value)
    end
    
    return unique(values)
end

"""Post-process vocabulary by examining CHECK constraints."""
function postprocess_vocab!(::MSSQLFlavor, conn::ODBC.Connection, cols::DataFrame; max_vocab::Int=200)
    for i in 1:nrow(cols)
        # Skip if already has vocabulary or missing source info
        if !ismissing(cols.vocabulary_relation[i]) || 
           ismissing(cols.source_relation[i]) || 
           ismissing(cols.base_column[i])
            continue
        end
        
        # Parse source relation
        schema, table = _split_relkey(String(cols.source_relation[i]))
        schema = isempty(schema) ? "dbo" : schema
        column = String(cols.base_column[i])
        
        # Use string interpolation for schema/table names instead of parameters
        escaped_schema = _escape_sql_string(schema)
        escaped_table = _escape_sql_string(table)
        escaped_column = _escape_sql_string(column)
        
        query = """
        SELECT cc.definition
        FROM sys.check_constraints cc
        JOIN sys.objects o ON o.object_id = cc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = N'$(escaped_schema)' 
          AND o.name = N'$(escaped_table)' 
          AND cc.definition LIKE N'%$(escaped_column)%';
        """
        
        constraint_defs = try
            DBInterface.execute(conn, query; iterate_rows=true) |> DataFrame
        catch e
            @warn "Failed to get CHECK constraints for $schema.$table.$column: $e"
            DataFrame()
        end
        
        isempty(constraint_defs) && continue
        
        # Extract values from all matching constraints
        all_values = String[]
        for def_row in eachrow(constraint_defs)
            values = extract_check_constraint_values(String(def_row.definition))
            append!(all_values, values)
        end
        
        # Only proceed if we found values and they're within the limit
        isempty(all_values) && continue
        unique_values = unique(all_values)
        
        if length(unique_values) <= max_vocab
            cols.vocabulary_relation[i] = "check:$(schema).$(table).$(column)"
            cols.code_column[i] = "literal"
            cols.label_column[i] = missing
            cols.vocab_sample[i] = DataFrame(code = unique_values)
            cols.vocab_skipped[i] = false
        end
    end
    
    return cols
end

# ───────── Utility Functions ─────────

"""Get table column metadata for structure analysis."""
function get_table_columns_metadata(::MSSQLFlavor, conn::ODBC.Connection, schema::AbstractString, table::AbstractString)
    escaped_schema = _escape_sql_string(schema)
    escaped_table = _escape_sql_string(table)
    
    query = """
    SELECT c.name AS column_name, t.name AS data_type
    FROM sys.schemas s
    JOIN sys.tables tb ON tb.schema_id = s.schema_id
    JOIN sys.columns c ON c.object_id = tb.object_id
    JOIN sys.types t ON t.user_type_id = c.user_type_id
    WHERE s.name = N'$(escaped_schema)' AND tb.name = N'$(escaped_table)'
    ORDER BY c.column_id;
    """
    
    try
        result = DBInterface.execute(conn, query; iterate_rows=true) |> DataFrame
        return result
    catch e
        @warn "Failed to get table columns metadata for $schema.$table: $e"
        return DataFrame(column_name=String[], data_type=String[])
    end
end

# ───────── Public Interface ─────────

"""
Main entry point for describing SQL Server query variables.

Uses SQL Server's sys.dm_exec_describe_first_result_set DMV for accurate
column metadata and enhanced vocabulary discovery through CHECK constraints.
"""
function describe_query_variables_mssql(conn::ODBC.Connection, sql::AbstractString;
                                        max_vocab::Int=200, vocab_row_threshold::Int=5_000)
    return describe_query_variables(MSSQL, conn, sql; 
                                   max_vocab=max_vocab, 
                                   vocab_row_threshold=vocab_row_threshold)
end
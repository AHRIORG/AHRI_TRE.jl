########################
# meta_common.jl
########################

abstract type DBFlavor end

# ───────── Core Utilities ─────────

"""Split "schema.table" or just "table" safely."""
function _split_relkey(sr::AbstractString)::Tuple{String,String}
    s = String(sr)
    return occursin('.', s) ? (split(s, "."; limit=2)...,) : ("", s)
end

"""Check if DataFrame has column (case-insensitive)."""
hascol(df::AbstractDataFrame, name::AbstractString) =
    any(n -> lowercase(String(n)) == lowercase(String(name)), names(df))

"""Get DataFrame column name by case-insensitive match."""
function colname(df::AbstractDataFrame, name::AbstractString)
    idx = findfirst(n -> lowercase(String(n)) == lowercase(String(name)), names(df))
    idx === nothing && error("Column $(name) not found. Available: $(names(df))")
    return names(df)[idx]
end

"""Missing-safe String conversion."""
str0(x)::String = ismissing(x) ? "" : String(x)

"""Format schema.table for display."""
relkey(schema::Union{AbstractString,Missing}, table::AbstractString) =
    (ismissing(schema) || isempty(String(schema))) ? String(table) : 
    string(String(schema), ".", String(table))

# ───────── SQL Parsing Utilities ─────────

"""Remove identifier quotes and handle escaping."""
function strip_identifier_quotes(x::AbstractString)
    s = String(x)
    patterns = [("\"", "\"\""), ("[", "]]"), ("`", "``")]
    
    for (quote_char, escape_seq) in patterns
        if startswith(s, quote_char) && endswith(s, quote_char) && length(s) > 2
            return replace(s[2:end-1], escape_seq => quote_char)
        end
    end
    return s
end

# Identifier character predicates
@inline is_ident_start(c::Char) = Base.isletter(c) || c in ('_', '#', '$')
@inline is_ident_char(c::Char) = Base.isletter(c) || Base.isdigit(c) || c in ('_', '#', '$')

"""Get the final SELECT segment (outermost)."""
function final_select_segment(sql::AbstractString)::String
    s = String(sql)
    pos = 0
    for m in eachmatch(r"(?i)\bselect\b", s)
        pos = m.offset
    end
    return pos == 0 ? s : s[pos:end]
end

"""Split SELECT list by top-level commas (respecting parentheses and quotes)."""
function split_top_level_commas(s::AbstractString)::Vector{String}
    parts = String[]
    str, depth, in_quote, start = String(s), 0, false, firstindex(s)
    
    i = start
    while i <= lastindex(str)
        c = str[i]
        
        if in_quote
            if c == '\'' && (i == lastindex(str) || str[nextind(str, i)] != '\'')
                in_quote = false
            elseif c == '\'' && i < lastindex(str) && str[nextind(str, i)] == '\''
                i = nextind(str, i)  # Skip escaped quote
            end
        else
            if c == '\''
                in_quote = true
            elseif c == '('
                depth += 1
            elseif c == ')'
                depth = max(depth - 1, 0)
            elseif c == ',' && depth == 0
                push!(parts, strip(str[start:prevind(str, i)]))
                start = nextind(str, i)
            end
        end
        i = nextind(str, i)
    end
    
    if start <= lastindex(str)
        push!(parts, strip(str[start:end]))
    end
    return parts
end

"""Extract CTE names from WITH clause."""
function extract_cte_names(sql::AbstractString)::Set{String}
    s = String(sql)
    m = match(r"(?i)\bwith\b", s)
    m === nothing && return Set{String}()
    
    i = m.offset + length(m.match)
    # Skip whitespace and optional RECURSIVE
    while i <= lastindex(s) && isspace(s[i]); i += 1; end
    if i + 8 <= lastindex(s) && lowercase(s[i:i+8]) == "recursive"
        i += 9
        while i <= lastindex(s) && isspace(s[i]); i += 1; end
    end
    
    names = Set{String}()
    
    while i <= lastindex(s)
        # Parse CTE name (quoted or bare)
        nm = ""
        if i <= lastindex(s) && s[i] in ('"', '[')
            quote_char = s[i] == '"' ? '"' : ']'
            j = i + 1
            while j <= lastindex(s) && s[j] != quote_char; j += 1; end
            nm = strip_identifier_quotes(s[i:j])
            i = j + 1
        elseif i <= lastindex(s) && is_ident_start(s[i])
            j = i
            while j <= lastindex(s) && is_ident_char(s[j]); j += 1; end
            nm = s[i:j-1]
            i = j
        end
        
        !isempty(nm) && push!(names, lowercase(nm))
        
        # Skip column list and AS clause
        while i <= lastindex(s) && isspace(s[i]); i += 1; end
        if i <= lastindex(s) && s[i] == '('
            depth = 1; i += 1
            while i <= lastindex(s) && depth > 0
                s[i] == '(' && (depth += 1)
                s[i] == ')' && (depth -= 1)
                i += 1
            end
        end
        
        # Find AS clause and skip CTE body
        while i <= lastindex(s) && isspace(s[i]); i += 1; end
        am = match(r"(?i)\bas\b", s[i:end])
        am === nothing && break
        i += am.offset + length(am.match) - 1
        
        while i <= lastindex(s) && isspace(s[i]); i += 1; end
        (i > lastindex(s) || s[i] != '(') && break
        
        # Skip CTE body
        depth = 1; i += 1
        while i <= lastindex(s) && depth > 0
            s[i] == '(' && (depth += 1)
            s[i] == ')' && (depth -= 1)
            i += 1
        end
        
        # Check for next CTE
        while i <= lastindex(s) && isspace(s[i]); i += 1; end
        if i <= lastindex(s) && s[i] == ','
            i += 1
            while i <= lastindex(s) && isspace(s[i]); i += 1; end
            continue
        end
        break
    end
    
    return names
end

"""Extract projected column names from final SELECT."""
function extract_projection_names(sql::AbstractString)::Vector{String}
    tail = final_select_segment(sql)
    m = match(r"(?is)\bselect\b(.*?)\bfrom\b", tail)
    m === nothing && return String[]
    
    items = split_top_level_commas(m.captures[1])
    names = String[]
    
    for item in items
        s = strip(item)
        
        # Try different patterns for column names/aliases
        patterns = [
            r"(?is)\bas\s+(\[[^\]]+\]|\"[^\"]+\"|\w+)\s*$",  # AS alias
            r"(\[[^\]]+\]|\"[^\"]+\"|\w+)\s*$",              # Trailing identifier
            r"^\s*(?:\[[^\]]+\]|\"[^\"]+\"|\w+)\.(\[[^\]]+\]|\"[^\"]+\"|\w+)\s*$"  # Qualified name
        ]
        
        found = false
        for (i, pattern) in enumerate(patterns)
            m = match(pattern, s)
            if m !== nothing
                capture_idx = i == 3 ? 1 : 1  # For qualified names, take the column part
                push!(names, strip_identifier_quotes(m.captures[capture_idx]))
                found = true
                break
            end
        end
    end
    
    # Remove duplicates while preserving order
    unique_names = String[]
    seen = Set{String}()
    for name in names
        lname = lowercase(name)
        if !(lname in seen)
            push!(unique_names, name)
            push!(seen, lname)
        end
    end
    
    return unique_names
end

# ───────── Enhanced Code Table Detection ─────────

"""Optimized semantic analysis for code table detection (no database queries)."""
function is_code_table_by_semantics(schema::AbstractString, table::AbstractString, reference_column::AbstractString)
    schema_lower = lowercase(schema)
    table_lower = lowercase(table)
    ref_col_lower = lowercase(reference_column)
    
    # Schema-based indicators (high confidence)
    code_schema_patterns = [
        r"(?i)^(code|codes|lookup|lookups|reference|ref|master|dim|dimension)s?$",
        r"(?i)^(metadata|meta|catalog|dict|dictionary)$",
        r"(?i)^(enum|enums|vocab|vocabulary|domain)s?$",
        r"(?i)^(static|config|configuration|settings)$"
    ]
    
    # Strong table name indicators
    strong_table_patterns = [
        r"(?i)^(lookup|reference|code|master|enum)s?$",        # lookup tables
        r"(?i)^(ref_|lk_|lookup_|code_|dim_|master_)",         # prefixed reference tables  
        r"(?i)(status|type|category|kind|class)e?s$",          # domain value tables
        r"(?i)^(list_|tbl_|table_)?(status|type|category)s?$", # explicit domain tables
    ]
    
    # Strong column name indicators
    strong_column_patterns = [
        r"(?i)_(id|code|type|status|category|kind|class)$",    # typical FK patterns
        r"(?i)^(status|type|category|kind|class)_",            # categorization columns
        r"(?i)_(key|cd|tp|stat)$",                             # abbreviated patterns
    ]
    
    # Moderate table indicators
    moderate_table_patterns = [
        r"(?i)(countries|states|currencies|languages|roles|permissions|priorities)$",  # common domains
        r"(?i)_(enum|lookup|list|dict)$",                      # enum-style naming
        r"(?i)^(system_|sys_|app_)?(config|setting|option)s?$", # configuration tables
    ]
    
    # Schema context modifiers
    in_code_schema = any(p -> occursin(p, schema_lower), code_schema_patterns)
    
    # Score the indicators
    schema_boost = in_code_schema ? 2 : 0  # Schema context provides strong signal
    strong_table = any(p -> occursin(p, table_lower), strong_table_patterns) ? 3 : 0
    strong_column = any(p -> occursin(p, ref_col_lower), strong_column_patterns) ? 2 : 0
    moderate_table = any(p -> occursin(p, table_lower), moderate_table_patterns) ? 1 : 0
    
    total_score = schema_boost + strong_table + strong_column + moderate_table
    
    # Decision logic:
    # - Code schema + any table pattern = likely code table
    # - Strong table pattern alone = likely code table  
    # - Strong column pattern + moderate table = likely code table
    # - Multiple moderate signals = possible code table
    
    return total_score >= 3 || 
           (in_code_schema && total_score >= 1) ||
           (strong_table >= 3) ||
           (strong_column >= 2 && moderate_table >= 1)
end

"""Check if a schema is likely a code/lookup schema."""
function is_code_schema(schema::AbstractString)::Bool
    schema_lower = lowercase(schema)
    code_schema_patterns = [
        r"(?i)^(code|codes|lookup|lookups|reference|ref|master|dim|dimension)s?$",
        r"(?i)^(metadata|meta|catalog|dict|dictionary)$",
        r"(?i)^(enum|enums|vocab|vocabulary|domain)s?$",
        r"(?i)^(static|config|configuration|settings)$"
    ]
    
    return any(p -> occursin(p, schema_lower), code_schema_patterns)
end

"""Lightweight structure analysis using metadata (to be implemented per flavor)."""
function is_code_table_by_structure(fl::DBFlavor, conn, schema::AbstractString, table::AbstractString)
    # Default implementation - individual flavors should override this
    # This is a placeholder that tries to be conservative
    try
        # Get basic column count if possible
        columns = get_table_columns_metadata(fl, conn, schema, table)
        col_count = nrow(columns)
        
        # Code table characteristics:
        # - Small number of columns (2-6 typically)
        # - Has probable key columns
        # - Has probable label columns
        
        if col_count > 8  # Too many columns for a lookup table
            return false
        end
        
        column_names = lowercase.(String.(columns.column_name))
        
        has_key_col = any(name -> occursin(r"(?i)^(id|code|key|value)$", name), column_names)
        has_label_col = any(name -> occursin(r"(?i)^(name|label|desc|description|title)$", name), column_names)
        
        return has_key_col && has_label_col && col_count <= 6
        
    catch
        # If metadata query fails, default to false (conservative)
        return false
    end
end

"""Enhanced code table detection with schema awareness."""
function is_candidate_code_table(fl::DBFlavor, conn, schema::AbstractString, table::AbstractString, 
                                ref_column::AbstractString; max_vocab::Int=200, vocab_row_threshold::Int=200)
    
    # Strategy 1: Size threshold (existing approach) - FAST
    size_check = !table_exceeds_threshold(fl, conn, schema, table; threshold=vocab_row_threshold)
    
    # Strategy 2: Enhanced semantic naming patterns with schema awareness - VERY FAST
    semantic_check = is_code_table_by_semantics(schema, table, ref_column)
    
    # Strategy 3: Structure analysis - FAST (metadata only) - only if size allows or semantics are strong
    structure_check = (size_check || semantic_check) ? is_code_table_by_structure(fl, conn, schema, table) : false
    
    # Enhanced scoring system with schema awareness
    score = 0
    size_check && (score += 3)        # Size is important but not always decisive
    semantic_check && (score += 4)    # Semantic patterns are very reliable, especially with schema
    structure_check && (score += 2)   # Structure confirms the pattern
    
    # Special case: if in a clear "code" schema, be more permissive on size
    in_obvious_code_schema = is_code_schema(schema)
    
    if in_obvious_code_schema && semantic_check
        # Allow slightly larger tables in dedicated code schemas
        relaxed_size_check = !table_exceeds_threshold(fl, conn, schema, table; threshold=vocab_row_threshold * 2)
        if relaxed_size_check
            score += 1  # Bonus for being in obvious code schema
        end
    end
    
    # Decision thresholds:
    # - Score >= 6: High confidence code table
    # - Score >= 4 with semantic check: Good candidate  
    # - In code schema with any positive indicators: Likely candidate
    
    return score >= 6 || 
           (score >= 4 && semantic_check) ||
           (in_obvious_code_schema && score >= 2)
end

# ───────── Database Interface Hooks ─────────

# Default implementations (to be overridden by flavors)
make_temp_view(::DBFlavor, conn, sql::AbstractString) = error("make_temp_view not implemented")
describe_output(::DBFlavor, conn, viewname::AbstractString) = error("describe_output not implemented")
referenced_relations(::DBFlavor, conn, viewname::AbstractString, sql::AbstractString) = Tuple{Union{Missing,String},String}[]
load_all_column_descriptions(::DBFlavor, conn, rels) = error("load_all_column_descriptions not implemented")
load_fk_edges(::DBFlavor, conn, rels) = error("load_fk_edges not implemented")

# Optional hooks with sensible defaults
postprocess_vocab!(::DBFlavor, conn, cols::DataFrame; max_vocab::Int=200) = cols
pick_label_column(::DBFlavor, conn, schema::AbstractString, table::AbstractString) = missing
table_exceeds_threshold(::DBFlavor, conn, schema::AbstractString, table::AbstractString; threshold::Int) = error("table_exceeds_threshold not implemented")

function sample_vocab(::DBFlavor, conn, schema::AbstractString, table::AbstractString,
                     codecol::AbstractString, labelcol::Union{AbstractString,Missing}, max_rows::Int)
    error("sample_vocab not implemented")
end

# SQL identifier quoting (ANSI default)
sql_ident(::DBFlavor, s::AbstractString) = "\"" * replace(String(s), "\"" => "\"\"") * "\""

function qualify(fl::DBFlavor, schema::Union{AbstractString,Missing}, table::AbstractString)
    return ismissing(schema) || isempty(String(schema)) ? 
           sql_ident(fl, table) : 
           sql_ident(fl, String(schema)) * "." * sql_ident(fl, table)
end

# ───────── Core Processing Functions ─────────
# Add before the Database Interface Hooks section:

"""Enhanced vocabulary sampling that ensures both code and description columns are included."""
function enhanced_sample_vocab(fl::DBFlavor, conn, schema::AbstractString, table::AbstractString,
                              codecol::AbstractString, labelcol::Union{AbstractString,Missing}, max_rows::Int)
    # Call the flavor-specific sample_vocab implementation
    result = sample_vocab(fl, conn, schema, table, codecol, labelcol, max_rows)
    
    # Ensure result has appropriate column names for vocabulary item creation
    if isa(result, DataFrame) && !isempty(result)
        # Standardize column names to make vocabulary creation more consistent
        col_names = names(result)
        
        if length(col_names) >= 2
            # Rename columns to standard names for easier processing
            rename!(result, col_names[1] => :code, col_names[2] => :label)
            
            # If there are additional columns, keep them as potential descriptions
            if length(col_names) > 2
                for i in 3:length(col_names)
                    old_name = col_names[i]
                    # Keep description-like columns, rename others to desc_N
                    if occursin(r"(?i)(desc|description|detail|note|comment)", String(old_name))
                        rename!(result, old_name => :description)
                        break  # Use the first description-like column
                    end
                end
            end
        elseif length(col_names) == 1
            # Single column - rename to code
            rename!(result, col_names[1] => :code)
        end
    end
    
    return result
end

# Update the attach_vocab! function to use enhanced sampling:
"""Enhanced vocabulary attachment with improved code table detection."""
function attach_vocab!(fl::DBFlavor, conn, cols::DataFrame, fks::DataFrame;
                      max_vocab::Int=200, vocab_row_threshold::Int=5_000)
    # Initialize vocabulary columns
    for col_name in [:vocabulary_relation, :code_column, :label_column]
        cols[!, col_name] = Vector{Union{Missing,String}}(missing, nrow(cols))
    end
    cols[!, :vocab_sample] = Vector{Union{Missing,Any}}(missing, nrow(cols))
    cols[!, :vocab_skipped] = falses(nrow(cols))
    
    # Build FK lookup map
    fkmap = Dict{Tuple{String,String,String}, Tuple{String,String,String}}()
    for row in eachrow(fks)
        key = (String(row.rel_schema), String(row.rel_table), String(row.src_column))
        val = (String(row.ref_schema), String(row.ref_table), String(row.ref_column))
        fkmap[key] = val
    end
    
    # Process each column
    for i in 1:nrow(cols)
        sr, bc = cols.source_relation[i], cols.base_column[i]
        (ismissing(sr) || ismissing(bc)) && continue
        
        sch, tab = _split_relkey(String(sr))
        key = (sch, tab, String(bc))
        
        haskey(fkmap, key) || continue
        
        rs, rt, rc = fkmap[key]
        cols.vocabulary_relation[i] = isempty(rs) ? String(rt) : string(rs, ".", rt)
        cols.code_column[i] = rc
        
        # Enhanced code table detection
        if !is_candidate_code_table(fl, conn, rs, rt, rc; 
                                   max_vocab=max_vocab, 
                                   vocab_row_threshold=vocab_row_threshold)
            cols.vocab_skipped[i] = true
            continue
        end
        
        # Get label column and sample data with enhanced vocabulary sampling
        lbl = pick_label_column(fl, conn, rs, rt)
        cols.label_column[i] = lbl
        cols.vocab_sample[i] = enhanced_sample_vocab(fl, conn, rs, rt, rc, lbl, max_vocab)
    end
    
    return cols
end
"""Remove duplicate rows, keeping best match per projected column."""
function dedupe_by_projection(cols::DataFrame, proj::Vector{String})
    isempty(proj) && return cols
    
    # Group rows by lowercase column name
    groups = Dict{String, Vector{Int}}()
    for i in 1:nrow(cols)
        key = lowercase(String(cols.column_name[i]))
        push!(get!(groups, key, Int[]), i)
    end
    
    # Score rows (higher is better)
    score_row(i::Int) = (
        (!ismissing(cols.source_relation[i]) ? 8 : 0) +
        (!ismissing(cols.description[i]) ? 4 : 0) +
        (!ismissing(cols.vocabulary_relation[i]) ? 2 : 0) +
        (!ismissing(cols.label_column[i]) || !ismissing(cols.code_column[i]) ? 1 : 0)
    )
    
    # Select best row for each projected column
    selected = Int[]
    for proj_name in proj
        key = lowercase(proj_name)
        rows = get(groups, key, Int[])
        isempty(rows) && continue
        
        best_row = argmax(i -> score_row(i), rows)
        push!(selected, best_row)
    end
    
    return cols[unique(selected), :]
end

"""Build description lookup map from catalog data."""
function build_description_map(descs::DataFrame)
    descmap = Dict{Tuple{String,String,String}, String}()
    
    for row in eachrow(descs)
        sch_val = get(row, :schema_name, missing)
        tab_val = get(row, :table_name, missing)
        col_val = get(row, :column_name, missing)
        desc_val = get(row, :description, missing)
        
        if all(!ismissing, [sch_val, tab_val, col_val, desc_val])
            key = (lowercase(String(sch_val)), lowercase(String(tab_val)), lowercase(String(col_val)))
            descmap[key] = String(desc_val)
        end
    end
    
    return descmap
end

"""Apply same-name fallback for unmapped columns."""
function same_name_fallback!(cols::DataFrame, descs::DataFrame)
    isempty(descs) && return cols
    
    # Build lookup structures
    col_to_tables = Dict{String, Set{Tuple{String,String}}}()
    desc_map = Dict{Tuple{String,String,String}, Union{Missing,String}}()
    
    s_sym = hascol(descs, "schema_name") ? colname(descs, "schema_name") : nothing
    t_sym = colname(descs, "table_name")
    c_sym = colname(descs, "column_name")
    d_sym = hascol(descs, "description") ? colname(descs, "description") : nothing
    
    for row in eachrow(descs)
        sch = s_sym === nothing ? "" : str0(row[s_sym])
        tab = str0(row[t_sym])
        col = str0(row[c_sym])
        
        push!(get!(col_to_tables, lowercase(col), Set{Tuple{String,String}}()), (sch, tab))
        
        if d_sym !== nothing
            desc_map[(sch, tab, lowercase(col))] = ismissing(row[d_sym]) ? missing : String(row[d_sym])
        end
    end
    
    # Apply fallback mapping
    for i in 1:nrow(cols)
        if ismissing(cols.source_relation[i]) || ismissing(cols.base_column[i])
            col_name = lowercase(String(cols.column_name[i]))
            tables = get(col_to_tables, col_name, Set{Tuple{String,String}}())
            
            if length(tables) == 1
                sch, tab = first(tables)
                cols.source_relation[i] = relkey(sch == "" ? missing : sch, tab)
                cols.base_column[i] = String(cols.column_name[i])
                cols.description[i] = get(desc_map, (sch, tab, col_name), missing)
            end
        end
    end
    
    return cols
end

# ───────── Main Entry Point ─────────

"""
Describe variables (columns) produced by a SQL query.

Returns DataFrame with columns:
- column_name, data_type, source_relation, base_column
- description, vocabulary_relation, code_column, label_column  
- vocab_sample, vocab_skipped
"""
function describe_query_variables(fl::DBFlavor, conn, sql::AbstractString;
                                 max_vocab::Int=200, vocab_row_threshold::Int=5_000)
    # 1. Create temporary view/table for the query
    vname = make_temp_view(fl, conn, sql)
    
    # 2. Get output columns and data types
    cols = describe_output(fl, conn, vname)
    
    # 3. Filter to explicitly projected columns only
    proj = extract_projection_names(sql)
    if !isempty(proj)
        keepnames = Set(lowercase.(proj))
        filter!(row -> lowercase(String(row.column_name)) in keepnames, cols)
    end
    
    # 4. Initialize metadata columns
    for col_name in [:source_relation, :base_column, :description]
        cols[!, col_name] = Vector{Union{Missing,String}}(missing, nrow(cols))
    end
    
    # 5. Get referenced relations (excluding CTEs)
    rels = referenced_relations(fl, conn, vname, sql)
    
    # 6. Load column descriptions and build lookup map
    descs = load_all_column_descriptions(fl, conn, rels)
    descmap = build_description_map(descs)
    
    # 7. Map columns to their base relations
    if !isempty(descs)
        c_sym = colname(descs, "column_name")
        t_sym = colname(descs, "table_name") 
        s_sym = hascol(descs, "schema_name") ? colname(descs, "schema_name") : nothing
        d_sym = hascol(descs, "description") ? colname(descs, "description") : nothing
        
        # Direct unambiguous mapping
        for i in 1:nrow(cols)
            col_name = String(cols.column_name[i])
            candidates = filter(row -> lowercase(str0(row[c_sym])) == lowercase(col_name), descs)
            
            unique_rels = unique([(s_sym === nothing ? "" : str0(row[s_sym]), str0(row[t_sym])) 
                                 for row in eachrow(candidates)])
            
            if length(unique_rels) == 1
                sch, tab = unique_rels[1]
                cols.source_relation[i] = relkey(sch == "" ? missing : sch, tab)
                cols.base_column[i] = col_name
                
                if d_sym !== nothing && !isempty(candidates)
                    desc_val = candidates[1, d_sym]
                    cols.description[i] = ismissing(desc_val) ? missing : String(desc_val)
                end
            end
        end
        
        # Apply same-name fallback for remaining unmapped columns
        same_name_fallback!(cols, descs)
    end
    
    # 8. Attach vocabulary via foreign keys
    fks = load_fk_edges(fl, conn, rels)
    attach_vocab!(fl, conn, cols, fks; max_vocab=max_vocab, vocab_row_threshold=vocab_row_threshold)
    
    # 9. Remove duplicates, keeping best match per projected column
    cols = dedupe_by_projection(cols, proj)
    
    # 10. Allow flavor-specific vocabulary processing
    cols = postprocess_vocab!(fl, conn, cols; max_vocab=max_vocab)
    
    # 11. Final column selection and ordering
    final_cols = [:column_name, :data_type, :source_relation, :base_column,
                  :description, :vocabulary_relation, :code_column, :label_column,
                  :vocab_sample, :vocab_skipped]
    
    existing_cols = intersect(final_cols, Symbol.(names(cols)))
    select!(cols, existing_cols)
    
    return cols
end

"""
Convert describe_query_variables DataFrame output to Vector of Variable structures.

# Arguments
- `df::DataFrame`: Output from describe_query_variables function
- `domain_id::Int`: Domain ID to assign to all variables

# Returns
- `Vector{Variable}`: Vector of Variable structures with populated vocabularies
"""
function dataframe_to_variables(df::DataFrame, domain_id::Int)::Vector{Variable}
    variables = Variable[]
    
    for (i, row) in enumerate(eachrow(df))
        # Extract basic information
        column_name = String(row.column_name)
        data_type = String(row.data_type)
        
        # Get description if available
        description = if hascol(df, "description") && !ismissing(row.description)
            String(row.description)
        else
            missing
        end
        
        # Determine value_type and create vocabulary if needed
        vocabulary = missing
        vocabulary_id = missing  # This matches Variable struct which uses Union{Missing,Int}
        value_type_id = TRE_TYPE_INTEGER  # default
        
        # Check if there's vocabulary data
        has_vocab = hascol(df, "vocab_sample") && !ismissing(row.vocab_sample)
        
        if has_vocab
            # Variables with vocabulary are always categorical
            value_type_id = TRE_TYPE_CATEGORY
            
            # Create vocabulary from vocab_sample DataFrame
            vocab_df = row.vocab_sample
            if isa(vocab_df, DataFrame) && !isempty(vocab_df)
                vocab_name = "$(column_name)_vocabulary"
                vocab_description = "Vocabulary for $(column_name)"
                
                vocabulary = Vocabulary(
                    vocabulary_id = nothing,  # Matches Vocabulary struct Union{Int,Nothing}
                    name = vocab_name,
                    description = vocab_description,
                    items = VocabularyItem[]
                )
                
                # Create vocabulary items based on DataFrame structure
                if ncol(vocab_df) == 1
                    # Single column: use sequential integers as values, strings as codes
                    col_name = names(vocab_df)[1]
                    for (idx, vocab_row) in enumerate(eachrow(vocab_df))
                        item_code = safe_string_convert(vocab_row[col_name])
                        item = VocabularyItem(
                            vocabulary_item_id = nothing,  # Matches VocabularyItem struct Union{Int,Nothing}
                            vocabulary_id = 0,  # Use 0 as placeholder instead of nothing
                            value = Int64(idx),  # Sequential integer starting from 1
                            code = item_code,
                            description = missing  # No description available with single column
                        )
                        push!(vocabulary.items, item)
                    end
                elseif ncol(vocab_df) >= 2
                    # Two or more columns: first column is code, second could be label/description
                    code_col = names(vocab_df)[1] 
                    label_col = names(vocab_df)[2]
                    
                    # Determine if we have a third column that might be a better description
                    desc_col = if ncol(vocab_df) >= 3
                        # Look for description-like column names in the 3rd+ columns
                        desc_candidates = names(vocab_df)[3:end]
                        desc_idx = findfirst(col -> occursin(r"(?i)(desc|description|detail|note|comment)", String(col)), desc_candidates)
                        desc_idx !== nothing ? desc_candidates[desc_idx] : label_col
                    else
                        label_col
                    end
                    
                    for (idx, vocab_row) in enumerate(eachrow(vocab_df))
                        # Handle the value - try to convert first column to integer, fallback to sequential
                        item_value = try
                            val = vocab_row[code_col]
                            if isa(val, Integer)
                                Int64(val)
                            elseif isa(val, AbstractString) && !isempty(val)
                                # Try to parse as integer
                                parse(Int64, String(val))
                            else
                                # Use row index as fallback
                                Int64(idx)
                            end
                        catch
                            # Use row index as fallback
                            Int64(idx)
                        end
                        
                        item_code = safe_string_convert(vocab_row[code_col])
                        
                        # Extract description from appropriate column
                        item_description = try
                            desc_val = vocab_row[desc_col]
                            if ismissing(desc_val)
                                missing
                            else
                                desc_str = safe_string_convert(desc_val)
                                isempty(desc_str) ? missing : desc_str
                            end
                        catch
                            missing
                        end
                        
                        item = VocabularyItem(
                            vocabulary_item_id = nothing,  # Matches VocabularyItem struct Union{Int,Nothing}
                            vocabulary_id = 0,  # Use 0 as placeholder instead of nothing
                            value = item_value,
                            code = item_code,
                            description = item_description  # Now includes description when available
                        )
                        push!(vocabulary.items, item)
                    end
                end
                
                vocabulary_id = missing  # Will be assigned when saved, matches Variable struct Union{Missing,Int}
            end
        else
            # Determine value_type from data_type string
            value_type_id = infer_value_type_from_data_type(data_type)
        end
        
        # Create the Variable
        variable = Variable(
            variable_id = nothing,  # Matches Variable struct Union{Int,Nothing}
            domain_id = domain_id,
            name = column_name,
            value_type_id = value_type_id,
            value_format = missing,  # Could be enhanced based on data_type
            vocabulary_id = vocabulary_id,  # This is Union{Missing,Int} so missing is okay
            keyrole = "none",  # Default, could be enhanced based on column analysis
            description = description,
            ontology_namespace = missing,  # Could be populated from additional metadata
            ontology_class = missing,      # Could be populated from additional metadata
            vocabulary = vocabulary
        )
        
        push!(variables, variable)
    end
    
    return variables
end

"""
Safely convert any value to String, handling various numeric types.

# Arguments
- `value`: Any value that needs to be converted to String

# Returns
- `String`: String representation of the value
"""
function safe_string_convert(value)::String
    if ismissing(value)
        return ""
    elseif isa(value, AbstractString)
        return String(value)
    else
        # Use string() function which works with all numeric types
        return string(value)
    end
end

"""
Infer TRE value type from database data type string.

# Arguments
- `data_type::String`: Database-specific data type string

# Returns
- `Int`: One of the TRE_TYPE_* constants
"""
function infer_value_type_from_data_type(data_type::String)::Int
    lower_type = lowercase(data_type)
    
    # Integer types
    if any(pattern -> occursin(pattern, lower_type), [
        "int", "integer", "bigint", "smallint", "tinyint", "mediumint",
        "serial", "bigserial", "number", "numeric(", "decimal("
    ])
        return TRE_TYPE_INTEGER
    end
    
    # Float types  
    if any(pattern -> occursin(pattern, lower_type), [
        "float", "double", "real", "decimal", "numeric", "money"
    ]) && !occursin("(", lower_type)  # Exclude decimal(precision,scale) which are often integers
        return TRE_TYPE_FLOAT
    end
    
    # Date types
    if occursin("date", lower_type) && !occursin("time", lower_type)
        return TRE_TYPE_DATE
    end
    
    # DateTime types
    if any(pattern -> occursin(pattern, lower_type), [
        "datetime", "timestamp", "timestamptz"
    ])
        return TRE_TYPE_DATETIME
    end
    
    # Time types
    if occursin("time", lower_type) && !occursin("stamp", lower_type)
        return TRE_TYPE_TIME
    end
    
    # Enumerated types (these would typically have vocabularies)
    if any(pattern -> occursin(pattern, lower_type), [
        "enum", "set"
    ])
        return TRE_TYPE_CATEGORY
    end
    
    # Default to string for everything else
    return TRE_TYPE_STRING
end

"""
Update vocabulary IDs after vocabularies have been saved to database.

# Arguments
- `variables::Vector{Variable}`: Variables with vocabularies
- `vocab_id_map::Dict{String, Int}`: Map from vocabulary name to assigned ID

# Returns
- `Vector{Variable}`: Updated variables with correct vocabulary IDs
"""
function update_vocabulary_ids!(variables::Vector{Variable}, vocab_id_map::Dict{String, Int})
    for variable in variables
        if !ismissing(variable.vocabulary) && !ismissing(variable.vocabulary.name)
            vocab_name = variable.vocabulary.name
            if haskey(vocab_id_map, vocab_name)
                new_id = vocab_id_map[vocab_name]
                variable.vocabulary_id = new_id
                variable.vocabulary.vocabulary_id = new_id
                
                # Update all vocabulary items
                for item in variable.vocabulary.items
                    item.vocabulary_id = new_id
                end
            end
        end
    end
    return variables
end
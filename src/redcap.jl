"""
    redcap_metadata(url::AbstractString, token::AbstractString;
                    forms::Union{Nothing,Vector{String}}=nothing) -> DataFrame

Downloads REDCap data dictionary (metadata) as a DataFrame.
- `url`: REDCap API URL
- `token`: API token
- `forms`: Optional vector of form names to filter by (default: all forms)
- `fields`: Optional vector of field names to filter by (default: all fields)
**REDCap Bug**: The API ignores the `fields` parameter and returns all fields, irrespective of the `fields` parameter.
Returns a DataFrame with columns: field_name, field_type, text_validation_type_or_show_slider_number,
select_choices_or_calculations, field_label, field_note.
"""
function redcap_metadata(url::AbstractString, token::AbstractString; forms::Vector{String} = String[], fields::Vector{String} = String[])::DataFrame
    body = Dict(
        "token" => token,
        "content" => "metadata",
        "format" => "json",
        "returnFormat" => "json"
    )
    if !isempty(forms)
        for (i, f) in enumerate(forms)
            body["forms[$(i-1)]"] = f
        end
    end
    if !isempty(fields)
        for (i, f) in enumerate(fields)
            body["fields[$(i-1)]"] = f
        end
    end
    headers = ["Content-Type" => "application/x-www-form-urlencoded"]
    form = join(["$(HTTP.escapeuri(k))=$(HTTP.escapeuri(v))" for (k, v) in body], "&")
    resp = HTTP.post(url, headers, form)
    resp.status == 200 || error("REDCap metadata error $(resp.status): $(String(resp.body))")
    return DataFrame(JSON3.read(String(resp.body)))
end
"""
    _strip_html(text::AbstractString) -> String

Remove HTML tags, script/style blocks, and decode a few common entities. Collapse
whitespace to single spaces and trim ends. Used to clean REDCap rich-text labels
before persisting them as variable descriptions.
"""
function _strip_html(text::AbstractString)
    # Remove script and style blocks first (non-greedy, dot matches newlines)
    cleaned = replace(text, r"<script.*?</script>"s => " ", r"<style.*?</style>"s => " ")
    # Remove all remaining tags
    cleaned = replace(cleaned, r"<[^>]+>" => " ")
    # Decode a minimal set of common HTML entities
    cleaned = replace(cleaned,
        "&nbsp;" => " ",
        "&amp;" => "&",
        "&lt;" => "<",
        "&gt;" => ">",
        "&quot;" => "\"",
        "&#39;" => "'")
    # Collapse whitespace
    cleaned = replace(cleaned, r"[ \t\r\n]+" => " ")
    return strip(cleaned)
end

"""
    parse_redcap_choices(s::AbstractString) -> Vector{NamedTuple{(:value,:code,:description),Tuple{Int,String,Union{String,Missing}}}}

Parses "1, Male | 2, Female" into a vector of items.
- value: Int (left id)
- code:  String (tokenized label; spaces -> `_`)
- description: original label
"""
function parse_redcap_choices(s::AbstractString)
    s = strip(s)
    isempty(s) && return NamedTuple{(:value, :code, :description)}[]
    items = NamedTuple{(:value, :code, :description)}[]
    next_seq = 1
    for rawitem in split(s, '|')
        part = strip(rawitem)
        isempty(part) && continue
        # Support separators: comma or '=' (take first occurrence only)
        idstr::String = ""
        label::String = ""
        if occursin(',', part)
            m = match(r"^\s*([^,]+?)\s*,\s*(.+)$", part)
            if m !== nothing
                idstr = strip(m.captures[1])
                label = strip(m.captures[2])
            else
                idstr = part
                label = part
            end
        elseif occursin('=', part)
            m = match(r"^\s*([^=]+?)\s*=\s*(.+)$", part)
            if m !== nothing
                idstr = strip(m.captures[1])
                label = strip(m.captures[2])
            else
                idstr = part
                label = part
            end
        else
            # Single token – treat as code with no separate label
            idstr = part
            label = part
        end

        # Try to interpret idstr as integer (direct or float); otherwise assign sequential
        val::Int = 0
        parsed_ok = true
        try
            val = parse(Int, idstr)
        catch
            try
                val = round(Int, parse(Float64, idstr))
            catch
                parsed_ok = false
                val = next_seq
                next_seq += 1
            end
        end

        # Code field rules:
        #  * If numeric id: derive code from label (lower snake case) like previous logic
        #  * If non-numeric id: use the original id token (verbatim) as code
        code::String = if parsed_ok
            replace(lowercase(label), r"\s+" => "_")
        else
            strip(idstr)
        end

        push!(items, (; value=val, code=code, description=label))
    end
    return items
end
"""
    map_value_type(field_type::String, validation::Union{Missing,String}) -> Int

REDCap field_type and validation -> TRE value_type_id.
"""
function map_value_type(field_type::AbstractString, validation::Union{Missing,AbstractString})
    ft = lowercase(String(field_type))
    v = validation === missing ? "" : lowercase(String(validation))
    if ft in ("radio", "dropdown", "yesno", "truefalse")
        return _VT_ENUM
    elseif ft == "checkbox"
        return _VT_MULTIRESPONSE
    elseif ft == "calc"
        # often numeric; you may refine with validation
        return _VT_FLOAT
    elseif ft == "slider"
        return _VT_INT
    elseif ft == "text"
        if v in ("integer", "number")
            return v == "integer" ? _VT_INT : _VT_FLOAT
        elseif v in ("date_ymd", "date_mdy", "date_dmy")
            return _VT_DATE
        elseif v in ("datetime_ymd", "datetime_mdy", "datetime_dmy", "datetime_seconds_ymd", "datetime_seconds_mdy", "datetime_seconds_dmy")
            return _VT_DATETIME
        elseif v in ("time", "time_hh_mm_ss")
            return _VT_TIME
        else
            return _VT_STRING
        end
    else
        return _VT_STRING
    end
end
"""
    ensure_vocabulary!(db, vocab_name::String, description::String,
                       items::Vector{NamedTuple{(:value,:code,:description),Tuple{Int,String,Union{String,Missing}}}}) -> Int

Creates or reuses a vocabulary by name, and (re)loads items idempotently.
Returns vocabulary_id.
"""
function ensure_vocabulary!(db, vocab_name::String, description::String, items)
    # 1) Get or create vocabulary
    q_get = DBInterface.prepare(
        db,
        raw"""
            SELECT vocabulary_id FROM vocabularies WHERE name = $1 LIMIT 1;
        """
    )
    df = DBInterface.execute(q_get, (vocab_name,)) |> DataFrame
    # Initialize to sentinel; we'll always assign in one of the branches
    vocab_id = -1
    if nrow(df) == 0
        vocab_id = insertwithidentity(db, "vocabularies",
            ["name", "description"], (vocab_name, description))
    else
        vocab_id = df[1, :vocabulary_id]
        # Keep description up to date
        DBInterface.execute(db, raw"""UPDATE vocabularies SET description = $2 WHERE vocabulary_id = $1;""",
            (vocab_id, description))
    end

    # 2) Load items idempotently: delete & reinsert for simplicity (fast + clean)
    DBInterface.execute(db, raw"""DELETE FROM vocabulary_items WHERE vocabulary_id = $1;""", (vocab_id,))
    ins = DBInterface.prepare(
        db,
        raw"""
            INSERT INTO vocabulary_items (vocabulary_id, value, code, description)
            VALUES ($1,$2,$3,$4);
        """
    )
    for it in items
        DBInterface.execute(ins, (vocab_id, it.value, it.code, it.description))
    end
    return vocab_id
end

"""
    upsert_variable!(db, domain_id::Int, name::String; value_type_id::Int, vocabulary_id::Union{Nothing,Int}=nothing, description::Union{Missing,String}=missing) -> Int

Upserts into variables on (domain_id, name). Returns variable_id.
"""
function upsert_variable!(db, domain_id::Int, name::String; value_type_id::Int, vocabulary_id::Union{Nothing,Int,Missing,String}=nothing, description=missing, note=missing, keyrole::String="none")
    # Normalize optional / nullable parameters to proper SQL NULLs for LibPQ
    if vocabulary_id isa String && lowercase(vocabulary_id) == "nothing"
        vocabulary_id = missing
    end
    vid = (vocabulary_id === nothing || vocabulary_id === missing) ? missing : vocabulary_id
    desc = (description === nothing || description === missing) ? missing : description
    nte = (note === nothing || note === missing) ? missing : note

    stmt = DBInterface.prepare(
        db,
        raw"""
            INSERT INTO variables (domain_id, name, value_type_id, vocabulary_id, description, note, keyrole)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (domain_id, name) DO UPDATE
            SET value_type_id = EXCLUDED.value_type_id,
                vocabulary_id = EXCLUDED.vocabulary_id,
                description   = EXCLUDED.description,
                note          = EXCLUDED.note,
                keyrole       = EXCLUDED.keyrole
            RETURNING variable_id;
        """
    )
    df = DBInterface.execute(stmt, (domain_id, name, value_type_id, vid, desc, nte, keyrole)) |> DataFrame
    return df[1, :variable_id]
end

"""
    register_redcap_datadictionary(store::DataStore;
        domain_id::Int, redcap_url::String, redcap_token::String,
        dataset_id::Union{Nothing,UUID}=nothing, forms=nothing, vocabulary_prefix::String="") -> DataFrame

Returns a DataFrame mapping REDCap fields to (variable_id, value_type_id, vocabulary_id).
Field types: "descriptive","sql","signature","file" are ignored.
This function downloads the REDCap metadata, processes it, and registers variables in the given DataStore.
    Database actions are wrapped in a transaction and rolled back on error.
"""
function register_redcap_datadictionary(store::DataStore;
    domain_id::Int, redcap_url::String, redcap_token::String,
    forms=String[], vocabulary_prefix::String="")::DataFrame
    db = store.store
    DBInterface.execute(db, "BEGIN;")
    try
        md = redcap_metadata(redcap_url, redcap_token; forms=forms)
        @info "REDCap metadata downloaded: $(nrow(md)) fields"
        out = DataFrame(field_name=String[], variable_id=Int[], value_type_id=Int[],
            vocabulary_id=Union{Missing,Int}[], field_type=String[],
            validation=Union{Missing,String}[], label=Union{Missing,String}[], note=Union{Missing,String}[])
        first_row = true
        for row in eachrow(md)
            fname = String(row[:field_name])
            ftype = String(row[:field_type])
            fvalid = row[:text_validation_type_or_show_slider_number]
            raw_label = row[:field_label]
            flabel = _strip_html(raw_label)
            fnote = row[:field_note]
            choices = row[:select_choices_or_calculations]

            # Skip non-data fields
            if lowercase(ftype) in ["descriptive", "sql", "signature", "file"]
                continue
            end

            vtype_id = map_value_type(ftype, fvalid)

            vocab_id = missing
            if vtype_id == _VT_ENUM || vtype_id == _VT_MULTIRESPONSE
                vname = isempty(vocabulary_prefix) ? "dom$(domain_id).$(fname)" : "$(vocabulary_prefix).$(fname)"
                items = parse_redcap_choices(String(coalesce(choices, "")))
                vocab_id = ensure_vocabulary!(db, vname, "REDCap choices for $(fname)", items)
            end

            vocab_arg = (vocab_id === missing) ? missing : Int64(vocab_id)
            variable_id = upsert_variable!(db, domain_id, fname;
                value_type_id=vtype_id,
                vocabulary_id=vocab_arg,
                description=flabel,
                note=fnote,
                keyrole=first_row ? "record" : "none")
            first_row = false
            push!(out, (fname, variable_id, vtype_id, vocab_id, ftype, fvalid, flabel, fnote))
        end
        DBInterface.execute(db, "COMMIT;")
        return out
    catch e
        try
            DBInterface.execute(db, "ROLLBACK;")
        catch
        end
        rethrow(e)
    end
end
"""
    redcap_project_info(url, token; raw=false) -> NamedTuple | JSON3.Object

Fetch high-level REDCap project information (title, purpose, status, etc.).
When `raw=true`, return the parsed JSON3.Object directly.
When `raw=false` (default), return a `NamedTuple` with symbol keys suitable for
constructing a DataFrame or logging.
"""
function redcap_project_info(url::AbstractString, token::AbstractString; raw::Bool=false)
    body = Dict(
        "token" => token,
        "content" => "project",
        "format" => "json",
        "returnFormat" => "json"
    )
    headers = ["Content-Type" => "application/x-www-form-urlencoded"]
    form = join(["$(HTTP.escapeuri(k))=$(HTTP.escapeuri(v))" for (k, v) in body], "&")
    resp = HTTP.post(url, headers, form)
    resp.status == 200 || error("REDCap project info error $(resp.status): $(String(resp.body))")
    obj = JSON3.read(String(resp.body))  # single JSON object
    raw && return obj
    # Convert keys to symbols & values to plain Julia types where reasonable
    nt_pairs = NamedTuple{Tuple(Symbol.(keys(obj)))}(Tuple(values(obj)))
    return nt_pairs
end

"""
    redcap_project_info_df(url, token) -> DataFrame

Convenience wrapper returning a single-row DataFrame with project metadata.
"""
function redcap_project_info_df(url::AbstractString, token::AbstractString)::DataFrame
    info = redcap_project_info(url, token; raw=false)
    return DataFrame([info])
end
"""
    redcap_fields(api_url::AbstractString, api_token::AbstractString;
    forms::Union{Nothing,Vector{String}}=nothing,
    include_nondata::Bool=false)::Vector{String}

Fetches REDCap metadata fields, optionally filtering by forms and including non-data fields.
Returns a vector of field names.
- url: REDCap API URL
- token: API token
- forms: Optional vector of form names to filter by (default: all forms)
- include_nondata: If true, include non-data fields like descriptive, file, sql, signature
"""
function redcap_fields(api_url::AbstractString, api_token::AbstractString;
    forms::Union{Nothing,Vector{String}}=nothing,
    include_nondata::Bool=false)::Vector{String}
    # Build request for REDCap metadata
    body = Dict(
        "token" => api_token,
        "content" => "metadata",
        "format" => "json",
        "returnFormat" => "json",
    )
    if forms !== nothing
        for (i, f) in enumerate(forms)
            body["forms[$(i-1)]"] = f
        end
    end
    form = join(["$(HTTP.escapeuri(k))=$(HTTP.escapeuri(v))" for (k,v) in body], "&")
    resp = HTTP.post(api_url; headers=["Content-Type"=>"application/x-www-form-urlencoded", "Accept"=>"application/json"], body=form)
    resp.status == 200 || error("REDCap metadata request failed $(resp.status): $(String(resp.body))")

    md = JSON3.read(String(resp.body))  # array of objects
    nondata = Set(["descriptive","file","sql","signature"])  # commonly non-data fields
    names = String[]
    for obj in md
        nt = NamedTuple(obj)
        fname = hasproperty(nt, :field_name) ? String(getfield(nt, :field_name)) : ""
        isempty(fname) && continue
        ftype = hasproperty(nt, :field_type) ? lowercase(String(getfield(nt, :field_type))) : ""
        if include_nondata || !(ftype in nondata)
            push!(names, fname)
        end
    end
    return unique(names)
end
"""
    redcap_export_eav(api_url::AbstractString, api_token::AbstractString; forms::Vector{String}=String[], fields::Vector{String}=String[],lake_root = ENV["TRE_LAKE_PATH"], decode::Bool=false)::String

Exports REDCap data in EAV format (Entity-Attribute-Value) as a CSV file.
- api_url: REDCap API URL
- api_token: API token
- forms: Optional vector of form names to export (default: all forms)
- fields: Optional vector of field names to export (default: all fields)
- lake_root: Root directory for saving the export file (default: TRE_LAKE_PATH environment variable)
- decode: If true, decode the response body from ISO-8859-2 to UTF-8 before saving
Returns the path to the saved CSV file.
**REDCap Bug**: The API ignores the `fields` parameter and returns all fields, irrespective of the `fields` parameter.
"""
function redcap_export_eav(api_url::AbstractString, api_token::AbstractString; forms::Vector{String}=String[], fields::Vector{String}=String[],lake_root = ENV["TRE_LAKE_PATH"], decode::Bool=false)::String

    f = isempty(fields) ? redcap_fields(api_url, api_token) : fields

    body = OrderedDict(
        "token"   => api_token,
        "content" => "record",
        "action"  => "export",
        "format"  => "csv",
        "type"    => "eav",
        "csvDelimiter" => ",",
        "returnFormat" => "json",
    )
    if !isempty(forms)
        for (i, f) in enumerate(forms)
            body["forms[$(i-1)]"] = f
        end
    end
    if !isempty(fields)
        for (i, f) in enumerate(fields)
            body["fields[$(i-1)]"] = f
        end
    end
    form = join(["$(HTTP.escapeuri(k))=$(HTTP.escapeuri(v))" for (k,v) in body], "&")
    resp = HTTP.post(api_url; headers=["Content-Type"=>"application/x-www-form-urlencoded"], body=form)
    resp.status == 200 || error("EAV export failed: $(resp.status) $(String(resp.body))")
    # Ensure output directory exists: <TRE_LAKE_PATH>/ingests
    out_dir = joinpath(lake_root, "ingests")
    mkpath(out_dir)

    # Choose a random unique filename (JSON, since format=json)
    fname   = string("redcap_records_", Dates.format(now(), "yyyymmdd_HHMMSS"), "_", uuid4(), ".csv")
    outpath = joinpath(out_dir, fname)

    # Save to file
    open(outpath, "w") do io
        if !decode
            write(io, resp.body)  # Write raw bytes directly
            return outpath
        end
        s = StringEncodings.decode(resp.body, "ISO-8859-2") 
        write(io, s)  # Convert to UTF-8
    end

    return outpath
end

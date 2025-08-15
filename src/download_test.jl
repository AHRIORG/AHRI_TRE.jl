using HTTP
using JSON3
using ConfigEnv
using Logging, LoggingExtras
using UUIDs
using Dates
using DataFrames
using DuckDB
using StringEncodings

dotenv()
# Read required environment variables
api_url   = ENV["REDCAP_API_URL"]
api_token = ENV["REDCAP_API_TOKEN"]
lake_root = ENV["TRE_LAKE_PATH"]


"""
    download_redcap_records()::String

Posts to the REDCap API (URL and token read from environment) to export records
as JSON, saves to a unique file in a sub-folder `ingests`, and returns the file path.
Throws an error if required environment variables are missing or the download fails.
"""
function download_redcap_records(api_url, api_token, lake_root)::String

    # Ensure output directory exists: <TRE_LAKE_PATH>/ingests
    out_dir = joinpath(lake_root, "ingests")
    mkpath(out_dir)

    # Choose a random unique filename (JSON, since format=json)
    fname   = string("redcap_records_", Dates.format(now(), "yyyymmdd_HHMMSS"), "_", uuid4(), ".json")
    outpath = joinpath(out_dir, fname)

    # Prepare POST form fields
    form_data = Dict(
        "token"                  => api_token,
        "content"                => "record",
        "action"                 => "export",
        "format"                 => "json",
        "type"                   => "eav",
        "fields"=> "arrray(record_id,cs_cohort_name,cs_coh_countries)",  # Empty string means all fields
        "csvDelimiter"           => ",",
        "rawOrLabel"             => "raw",
        "rawOrLabelHeaders"      => "raw",
        "exportCheckboxLabel"    => "false",
        "exportSurveyFields"     => "false",
        "exportDataAccessGroups" => "false",
        "returnFormat"           => "json",
    )

    # Perform POST request
    resp = HTTP.post(api_url; 
        headers = ["Content-Type" => "application/x-www-form-urlencoded","Accept" => "application/json"],
        body    = join(["$(HTTP.escapeuri(k))=$(HTTP.escapeuri(v))" for (k,v) in form_data], "&")
    )

    # Check for HTTP errors
    if resp.status != 200
        error("REDCap API request failed with status $(resp.status): $(String(resp.body))")
    end

    # Save to file
    open(outpath, "w") do io
        write(io, resp.body)
    end

    return outpath
end

function redcap_export_eav(api_url::AbstractString, api_token::AbstractString; forms::Vector{String}=String[], fields::Vector{String}=String[],lake_root = ENV["TRE_LAKE_PATH"], decode::Bool=false)::String

    f = isempty(fields) ? redcap_fields(api_url, api_token) : fields
    println(f)
    form = Dict(
        "token"   => api_token,
        "content" => "record",
        "action"  => "export",
        "format"  => "csv",
        "type"    => "eav",
        "fields"  => join(f, ","),
        "forms"   => join(forms, ","),
        "csvDelimiter" => ",",
        "returnFormat" => "json",
    )
    body = join(["$(HTTP.escapeuri(k))=$(HTTP.escapeuri(v))" for (k,v) in form], "&")
    resp = HTTP.post(api_url; headers=["Content-Type"=>"application/x-www-form-urlencoded"], body=body)
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

path = redcap_export_eav(api_url, api_token, lake_root=lake_root, decode=true)
println("Saved REDCap export to: $path")
# Example usage:
#=

open(path, "r") do io
    json_data = JSON3.read(io)              # Array of JSON3.Object
    rows = (NamedTuple(obj) for obj in json_data)  # each object -> NamedTuple
    df = DataFrame(rows)
    @info "Loaded $(nrow(df)) rows x $(ncol(df)) cols"
    println(first(df, 5))
end
=#
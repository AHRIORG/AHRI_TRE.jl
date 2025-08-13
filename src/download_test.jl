using HTTP
using JSON3
using ConfigEnv
using Logging, LoggingExtras
using UUIDs
using Dates
using DataFrames
using DuckDB

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
        "type"                   => "flat",
        "csvDelimiter"           => "",
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

# Example usage:
path = download_redcap_records(api_url, api_token, lake_root)
println("Saved REDCap export to: $path")

open(path, "r") do io
    json_data = JSON3.read(io)              # Array of JSON3.Object
    rows = (NamedTuple(obj) for obj in json_data)  # each object -> NamedTuple
    df = DataFrame(rows)
    @info "Loaded $(nrow(df)) rows x $(ncol(df)) cols"
    println(first(df, 5))
end

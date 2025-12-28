# Constants for AHRI_TRE.jl
const LAKE_ALIAS = "tre_lake"
# Value types
const TRE_TYPE_INTEGER = 1
const TRE_TYPE_FLOAT = 2
const TRE_TYPE_STRING = 3
const TRE_TYPE_DATE = 4
const TRE_TYPE_DATETIME = 5
const TRE_TYPE_TIME = 6
const TRE_TYPE_CATEGORY = 7
const TRE_TYPE_MULTIRESPONSE = 8

# Transformation types
const TRE_TRANSFORMATION_TYPE_INGEST = 1
const TRE_TRANSFORMATION_TYPE_TRANSFORM = 2

# Transformation statuses
const TRE_TRANSFORMATION_STATUS_UNVERIFIED = 1
const TRE_TRANSFORMATION_STATUS_VERIFIED = 2

# asset type
const TRE_DATASET = 1
const TRE_DOCUMENT = 2

# Study types
const TRE_STUDY_TYPE_HDSS = 1
const TRE_STUDY_TYPE_COHORT = 2
const TRE_STUDY_TYPE_SURVEY = 3
const TRE_STUDY_TYPE_PANEL = 4
const TRE_STUDY_TYPE_CASE_CONTROL = 5
const TRE_STUDY_TYPE_RCT = 6

# REDCap metadata types
const _VT_INT     = TRE_TYPE_INTEGER
const _VT_FLOAT   = TRE_TYPE_FLOAT
const _VT_STRING  = TRE_TYPE_STRING
const _VT_DATE    = TRE_TYPE_DATE
const _VT_DATETIME= TRE_TYPE_DATETIME
const _VT_TIME    = TRE_TYPE_TIME
const _VT_ENUM    = TRE_TYPE_CATEGORY
const _VT_MULTIRESPONSE = TRE_TYPE_MULTIRESPONSE
const _VT_FORMATS = Dict(
    "date_mdy" => "%m/%d/%Y",
    "date_dmy" => "%d/%m/%Y",
    "date_ymd" => "%Y-%m-%d",
    "datetime_mdy" => "%m/%d/%Y %H:%M",
    "datetime_dmy" => "%d/%m/%Y %H:%M",
    "datetime_ymd" => "%Y-%m-%d %H:%M",
    "datetime_seconds_mdy" => "%m/%d/%Y %H:%M:%S",
    "datetime_seconds_dmy" => "%d/%m/%Y %H:%M:%S",
    "datetime_seconds_ymd" => "%Y-%m-%d %H:%M:%S",
    "time_mm_ss" => "%M:%S",
    "time" => "%H:%M"
)

# NCName validation
# Single-char predicates using Unicode properties.
# StartChar: '_' or any Letter
const _RE_START = r"^[_\p{L}]$"
# NameChar: Letter, Number, combining marks, connector punct, letter modifiers
const _RE_NAME  = r"^[\p{L}\p{N}\p{Mc}\p{Mn}\p{Pc}\p{Lm}\-\.]+$"
const _RE_STRICT= r"^[\p{L}\p{N}\p{Mc}\p{Mn}\p{Pc}\p{Lm}]+$"

"""
    ODBC_DRIVER_PATH

Path to the Microsoft ODBC Driver for SQL Server.

This is read from the environment variable `ODBC_DRIVER_PATH` (typically via a `.env` file).
If unset, it falls back to the standard Debian/Ubuntu installation path.
"""
const DEFAULT_ODBC_DRIVER_PATH = "/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1"

function _resolve_odbc_driver_path(candidate::AbstractString)::String
    if !isempty(candidate) && isfile(candidate)
        return String(candidate)
    end

    # Common misconfiguration: users sometimes append an extra ".so".
    # Example: ".../libmsodbcsql-18.5.so.1.1.so" -> ".../libmsodbcsql-18.5.so.1.1"
    stripped = String(candidate)
    while endswith(stripped, ".so") && !isfile(stripped)
        stripped = stripped[1:(end - 3)]
        if isfile(stripped)
            return stripped
        end
    end

    if isfile(DEFAULT_ODBC_DRIVER_PATH)
        return DEFAULT_ODBC_DRIVER_PATH
    end

    return String(candidate)
end

ODBC_DRIVER_PATH = _resolve_odbc_driver_path(get(ENV, "ODBC_DRIVER_PATH", DEFAULT_ODBC_DRIVER_PATH))

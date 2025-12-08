using DataFrames
using ConfigEnv
using AHRI_TRE

dotenv()
# Read required environment variables
api_url   = ENV["REDCAP_API_URL"]
api_token = ENV["REDCAP_API_TOKEN"]
lake_root = ENV["TRE_LAKE_PATH"]

df = AHRI_TRE.redcap_fields(api_url, api_token)


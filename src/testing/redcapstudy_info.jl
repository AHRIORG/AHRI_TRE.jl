using AHRI_TRE
using ConfigEnv

dotenv()
# Read required environment variables
api_url   = ENV["REDCAP_API_URL"]
api_token = ENV["REDCAP_API_TOKEN"]

df = AHRI_TRE.redcap_project_info_df(api_url, api_token)
println("REDCap projects:")
println(df)
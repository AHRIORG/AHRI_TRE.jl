using DataFrames
using ConfigEnv
using AHRI_TRE

dotenv()
# Read required environment variables
api_url   = ENV["REDCAP_API_URL"]
api_token = "CC676547C6B37FE6DD58AE6DB3B44AF5"
lake_root = ENV["TRE_LAKE_PATH"]


path = AHRI_TRE.redcap_export_eav(api_url, api_token, lake_root=lake_root, decode=true, forms=["institute_information"], fields=["ii_cohort_id","ii_cohort_name"])
println("Saved REDCap export to: $path")

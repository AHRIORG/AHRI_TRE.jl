using DataFrames
using ConfigEnv
using AHRI_TRE

dotenv()

df = AHRI_TRE.redcap_project_info_df(ENV["REDCAP_API_URL"], ENV["REDCAP_API_TOKEN"])
using AHRI_TRE
using ConfigEnv

dotenv()

createdatabase(ENV["TRE_SERVER"], ENV["TRE_USER"], ENV["TRE_PWD"], ENV["TRE_DBNAME"], replace=true)

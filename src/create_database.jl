using AHRI_TRE
using ConfigEnv

dotenv()

@time createdatabase(ENV["TRE_DATABASE_PATH"], ENV["TRE_DBNAME"], replace=true, sqlite=true)

using AHRI_TRE
using ConfigEnv

dotenv()

@time createdatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], replace=true, sqlite=true)

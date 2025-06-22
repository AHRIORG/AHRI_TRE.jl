using RDAIngest
using ConfigEnv

dotenv()

@time createdatabase(ENV["RDA_SERVER"], ENV["RDA_DBNAME"], replace=true, sqlite=false)

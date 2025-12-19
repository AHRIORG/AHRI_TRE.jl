using AHRI_TRE
using ConfigEnv
using DataFrames

#get environment variables
dotenv()

datastore = AHRI_TRE.DataStore(
    server=ENV["TRE_SERVER"],
    user=ENV["TRE_USER"],
    password=ENV["TRE_PWD"],
    dbname=ENV["TRE_DBNAME"],
    lake_password=ENV["LAKE_PASSWORD"],
    lake_user=ENV["LAKE_USER"],
    lake_data=ENV["TRE_LAKE_PATH"]
);
datastore = AHRI_TRE.opendatastore(datastore)
cohorts = AHRI_TRE.read_dataset(datastore,"APCC","redcap_1194")
cause = AHRI_TRE.read_dataset(datastore,"Test","cause_counts_mssql_nehiqzia")
closedatastore(datastore)
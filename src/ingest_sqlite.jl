using RDAIngest
using ConfigEnv
using Logging
using DBInterface
using DataFrames
using Dates
using BenchmarkTools

#get environment variables
dotenv()

function ingest_all()
    createdatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], replace=true, sqlite=true)
    source = CHAMPSSource()
    ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], sqlite=true)
    ingest_dictionary(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"],
        ENV["DATA_INGEST_PATH"], sqlite=true)
    CHAMPSIngest = Ingest(source=source,
        death_file="CHAMPS_deid_basic_demographics",
        datasets=Dict("CHAMPS deid basic demographics" => "CHAMPS_deid_basic_demographics",
            "CHAMPS deid verbal autopsy" => "CHAMPS_deid_verbal_autopsy",
            "CHAMPS deid decode results" => "CHAMPS_deid_decode_results",
            "CHAMPS deid tac results" => "CHAMPS_deid_tac_results",
            "CHAMPS deid lab results" => "CHAMPS_deid_lab_results"
        ),
        datainstruments=Dict("cdc_93759_DS9.pdf" => "CHAMPS_deid_verbal_autopsy"),
        ingest_desc="Raw CHAMPS Level-2 Data accessed 20230518",
        transform_desc="Ingest of CHAMPS Level-2 Data",
        code_reference="RDAIngest.ingest_data",
        author="Kobus Herbst; YUE CHU")
    ingestion_id_sqlite = ingest_deaths(CHAMPSIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)
    ingest_data(CHAMPSIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id_sqlite, sqlite=true)

    source = COMSASource()
    ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"])
    ingest_dictionary(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"],
        ENV["DATA_INGEST_PATH"], sqlite=true)
    COMSAIngest = Ingest(source=source,
        death_file="Comsa_WHO_VA_20230308",
        datasets=Dict("COMSA deid verbal autopsy" => "Comsa_WHO_VA_20230308"),
        datainstruments=Dict("5a_2018_COMSA_VASA_ADULTS-EnglishOnly_01262019_clean.pdf" => "Comsa_WHO_VA_20230308",
            "5a_2018_COMSA_VASA_CHILD-EnglishOnly_12152018Clean.pdf" => "Comsa_WHO_VA_20230308",
            "5a_2018_COMSA_VASA_SB_NN-EnglishOnly_12152018Clean.pdf" => "Comsa_WHO_VA_20230308",
            "5a_2018_COMSA_VASA-GenInfo_English_06272018_clean.pdf" => "Comsa_WHO_VA_20230308"),
        ingest_desc="Ingest raw COMSA Level-2 Data accessed 20230518",
        transform_desc="Ingest of COMSA Level-2 Data",
        code_reference="RDAIngest.ingest_data",
        author="Kobus Herbst; YUE CHU"
    )
    ingestion_id_sqlite = ingest_deaths(COMSAIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)
    ingest_data(COMSAIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id_sqlite, sqlite=true)
end

@btime ingest_all()
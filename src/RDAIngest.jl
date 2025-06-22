module RDAIngest

using DataFrames
using SQLite
using DBInterface
using ConfigEnv
using Dates
using Arrow
using DataStructures
using ODBC

using CSV
using XLSX
using Docx
using FileIO
using Base64
using JSON3

export
    Vocabulary, VocabularyItem,
    DataDocument, DocCSV, DocXLSX, DocPDF, read_data, 
    AbstractSource, rawSource, CHAMPSSource, COMSAMZSource, HEALSLSource, #Source structs
    AbstractIngest, sourceIngest, CHAMPSIngest, COMSAMZIngest, HEALSLIngest, userIngest, #Ingest structs

    ingest_source, 
    add_sites, add_instruments, add_protocols, add_ethics, add_source, add_domain, 
    ingest_dictionary, ingest_deaths, ingest_data, save_dataset, 
    read_variables, get_vocabulary, add_variables, add_vocabulary, lookup_variables, 
    add_datarows, add_data_column, 
    
    death_in_ingest, get_last_deathingest, link_instruments, link_deathrows, 
    get_namedkey, get_variable_id, get_variable, get_valuetype, get_datasetname, 

    updatevalue, rbind, insertdata, insertwithidentity, 
    get_table, selectdataframe, prepareselectstatement, selectsourcesites, 

    dataset_to_dataframe, dataset_to_arrow, dataset_to_csv, 
    dataset_variables, dataset_column,
    
    savedataframe, createdatabase, opendatabase

#ODBC.bindtypes(x::Vector{UInt8}) = ODBC.API.SQL_C_BINARY, ODBC.API.SQL_LONGVARBINARY

"""
Structs for vocabulary
"""

struct VocabularyItem
    value::Int64
    code::String
    description::Union{String,Missing}
end

struct Vocabulary
    name::String
    description::String
    items::Vector{VocabularyItem}
end

"""
Structs for document type
"""

# Define an abstract document input
abstract type DataDocument end
# Define subtypes of Document - csv, xlsx, pdf, docx
struct DocCSV <: DataDocument
    path::String
    name::String
    delim::Char
    quotechar::Char
    dateformat::String
    decimal::Char
end
struct DocXLSX <: DataDocument
    path::String
    name::String
    sheetname::String
    #cellrange::String
end
struct DocPDF <: DataDocument #Can take either .pdf or .docx
    path::String
    name::String
end

"""
Struct for source-related information
"""
abstract type AbstractSource end

Base.@kwdef mutable struct rawSource <: AbstractSource 

    # Domain info
    name::String = "source_name"

    # Study type
    study_type_id::Integer = 1

    # Sites
    datafolder::String = "De_identified_data"
    site_data::String = "CHAMPS_deid_basic_demographics.csv"
    site_col::String = "site_iso_code"
    country_col::String = "name of country name column"
    country_name::String = "single country name" # if country col is empty, specify country name
    #add_iso3::Bool = false # add iso3 automatically

    # Protocol - specify file extension in name
    protocolfolder::String = "Protocols"
    protocols::Dict{String,String} = Dict("protocol_file_name.pdf" => "Protocol name")

    # Instrument - specify file extension in name
    instrumentfolder::String = "Instruments"
    instruments::Dict{String,String} = Dict("questionnaire_file_name.pdf" => "Dataset name.extension")

    # Ethics - specify file extension in name
    ethicsfolder::String = "Ethics"
    ethics::Dict{String,Vector{String}} = Dict("ethics_file_name.pdf" => ["Ethics name","IRB committee","protocol_file_name.pdf"])

    # CSV format
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "yyyy-mm-dd"
    decimal::Char = '.'
    
end

"""
Provide CHAMPS specific source information
"""

Base.@kwdef mutable struct CHAMPSSource <: AbstractSource 

    # Domain info
    name::String = "CHAMPS"

    # Study type
    study_type_id::Integer = 1

    # Sites
    datafolder::String = "De_identified_data"
    site_data::String = "CHAMPS_deid_basic_demographics.csv"
    site_col::String = "site_iso_code"
    country_col::String = "site_iso_code"
    country_name::String = "single country name" # if country col is empty, specify country name
    #add_iso3::Bool = false # add iso3 automatically

    # Protocol - specify file extension in name
    protocolfolder::String = "Protocols"
    protocols::Dict{String,String} = Dict("CHAMPS-Mortality-Surveillance-Protocol-v1.3.pdf" => "CHAMPS Mortality Surveillance Protocol",
    "CHAMPS-Social-Behavioral-Science-Protocol-v1.0.pdf" => "CHAMPS Social Behavioral Science Protocol",
    "CHAMPS-Diagnosis-Standards.pdf" => "CHAMPS DeCoDe Diagnosis Standards" ,
    "CHAMPS-Manual-v3.pdf" => "CHAMPS Manual",
    "CHAMPS Online De-Identified DTA.pdf" => "CHAMPS Online De-Identified Data Transfer Agreement")

    # Instrument - specify file extension in name
    instrumentfolder::String = "Instruments"
    instruments::Dict{String,String} = Dict("cdc_93759_DS9.pdf" => "CHAMPS_deid_verbal_autopsy.csv")

    # Ethics - specify file extension in name
    ethicsfolder::String = "Ethics"
    ethics::Dict{String,Vector{String}} = Dict("ICF-04 CHAMPS VA v1.1.pdf" => ["ICF-04 CHAMPS VA v1.1","Local ethics institution", "CHAMPS-Mortality-Surveillance-Protocol-v1.3.pdf"],
    "ICF-03_CHAMPS non_MITS consent v1.3.pdf" => ["ICF-03_CHAMPS non_MITS consent v1.3","Local ethics institution", "CHAMPS-Mortality-Surveillance-Protocol-v1.3.pdf"],
    "ICF-01 CHAMPS MITS procedures v1.3.pdf" => ["ICF-01 CHAMPS MITS procedures v1.3","Local ethics institution", "CHAMPS-Mortality-Surveillance-Protocol-v1.3.pdf"])

    # CSV format
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "yyyy-mm-dd"
    decimal::Char = '.'

end


"""
Provide COMSA Mozambique specific information
"""

Base.@kwdef mutable struct COMSAMZSource <: AbstractSource 

    # Domain info
    name::String = "COMSAMZ"

    # Study type
    study_type_id::Integer = 1

    # Sites
    datafolder::String = "De_identified_data"
    site_data::String = "Comsa_WHO_VA_20230308.csv"
    site_col::String = "provincia"
    country_name::String = "Mozambique" 

    # Protocol - specify file extension in name
    protocolfolder::String = "Protocols"
    protocols::Dict{String,String} = Dict("COMSA-FR-protocol_version-1.0_05July2017.pdf" => "Countrywide Mortality Surveillance for Action (COMSA) Mozambique (Formative Research)",
    "COMSA-protocol_without-FR_version-1.1_15June2017_clean_REVISED.pdf" => "Countrywide Mortality Surveillance for Action (COMSA) Mozambique",
    "COMSA-Data-Access-Plan.pdf" => "COMSA Data Access Plan",
    "Data Use Agreement (DUA) - Comsa.pdf" => "COMSA Data Use Agreement")

    # Instrument - specify file extension in name
    instrumentfolder::String = "Instruments"
    instruments::Dict{String,String} = Dict("5a_2018_COMSA_VASA_ADULTS-EnglishOnly_01262019_clean.pdf" => "Comsa_WHO_VA_20230308.csv",
    "5a_2018_COMSA_VASA_CHILD-EnglishOnly_12152018Clean.pdf" => "Comsa_WHO_VA_20230308.csv",
    "5a_2018_COMSA_VASA_SB_NN-EnglishOnly_12152018Clean.pdf" => "Comsa_WHO_VA_20230308.csv",
    "5a_2018_COMSA_VASA-GenInfo_English_06272018_clean.pdf" => "Comsa_WHO_VA_20230308.csv" #,
    # "3.Death_2-23.pdf" => "Comsa_death_20230308.csv",
    # "2.Preg-outcome_2-23.pdf" => "Comsa_pregnancy_outcome_20230308.csv",
    # "1.Pregnancy.pdf" => "Comsa_pregnancy_20230308.csv",
    # "Household-members_2-23.pdf" => "Comsa_household_20230308.csv"
    )

    # Ethics - specify file extension in name
    ethicsfolder::String = "Ethics"
    ethics::Dict{String,Vector{String}} = Dict("adult vasa - version 1, 2020 07 15.docx" => ["adult vasa - version 1", "National Health Bioethics Committee of Mozambique", "COMSA-protocol_without-FR_version-1.1_15June2017_clean_REVISED.pdf"],
    "child assent vasa - version 1, 2020 07 15.docx" => ["child assent vasa - version 1", "National Health Bioethics Committee of Mozambique", "COMSA-protocol_without-FR_version-1.1_15June2017_clean_REVISED.pdf"])

    # CSV format
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "u dd, yyyy" 
    decimal::Char = '.'

end

"""
Provide COMSA Sierra Leone (HEALSL) specific information
"""

Base.@kwdef mutable struct HEALSLSource <: AbstractSource 

    # Domain info
    name::String = "HEALSL"

    # Study type
    study_type_id::Integer = 1

    # Sites
    datafolder::String = "De_identified_data"
    site_data::String = "healsl_all_v1.csv"
    site_col::String = "id10057"
    country_name::String = "Sierra Leone"

    # Protocol - specify file extension in name
    protocolfolder::String = "Protocols"
    protocols::Dict{String,String} = Dict("4_COMSA Protocol - 27-Feb-2022.pdf" => "HEAL-SL (COMSA Sierra Leone) Protocol",
    "HEAL-SL Research Data Processing Notes for Extenal.pdf" => "HEAL-SL Data Processing Notes")

    # Instrument - specify file extension in name
    instrumentfolder::String = "Instruments"
    instruments::Dict{String,String} = Dict("Adult_eVA_Questionnaire-SL.pdf" => "healsl_all_v1.csv",
    "Child_eVA_Questionnaire-SL.pdf" => "healsl_all_v1.csv",
    "Neonate_eVA_Questionnaire-SL.pdf" => "healsl_all_v1.csv")

    # Ethics - specify file extension in name
    ethicsfolder::String = "Ethics"
    ethics::Dict{String,Vector{String}} = Dict("IRB Renewed.pdf" => ["IRB Renewed", "Government of Sierra Leone", "4_COMSA Protocol - 27-Feb-2022.pdf"],
    "Annexe 2 Consent forms- 27-Feb-2022.pdf" => ["Consent forms", "Government of Sierra Leone", "4_COMSA Protocol - 27-Feb-2022.pdf"])

    # CSV format
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "u dd, yyyy" 
    decimal::Char = '.'
    
end

"""
Struct for ingestion and transformation metadata
"""

abstract type AbstractIngest end

Base.@kwdef mutable struct sourceIngest <: AbstractIngest 
    # Source
    source::AbstractSource

    # Unique identifier of a row
    id_col::String = "id"

    # Domain info
    domain_name::String = "source"
    domain_description::String = "domain for user 001"

    # Death file
    death_file::String = "data_death.csv"
    # CSV format
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "yyyy-mm-dd"
    decimal::Char = '.'
    
    # Datasets matching to deaths
    datasets::Dict{String,String} = Dict("VA" => "data_va.csv", 
                                         "MITS" => "data_mits.csv") 

    # Data dictionaries
    datadictionaries::Vector{String} = ["data_va_dictionary.xlsx",
                                        "data_mits_dictionary.xlsx"]

    # Metadata for ingestion 
    ingest_desc::String = "Ingest raw de-identified data"
    ingest_date::Date = today() #"yyyy-mm-dd"
    
    # Metadata for transformation
    transform_desc::String = "Ingest raw de-identified data"
    code_reference::Vector{UInt8} = b"example code script"
    author::String = ""

    # Default metadata for source ingestion
    unit_of_analysis_id::Integer = 1 #Individual
    transformation_type_id::Integer = 1 #Raw data ingest
    transformation_status_id::Integer = 2 # Verified

end

"""
Struct of ingest metadata for CHAMPS data 
"""

Base.@kwdef mutable struct CHAMPSIngest <: AbstractIngest 
    # Source
    source::AbstractSource = CHAMPSSource()

    # Unique identifier of a row
    id_col::String = "champs_deid"

    # Domain info
    domain_name::String = "CHAMPS"
    domain_description::String = "Raw CHAMPS level-2 deidentified data"

    # Death file
    death_file::String = "CHAMPS_deid_basic_demographics.csv"
    # CSV format
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "yyyy-mm-dd"
    decimal::Char = '.'
    
    # Datasets matching to deaths
    datasets::Dict{String,String} = Dict("CHAMPS deid basic demographics" => "CHAMPS_deid_basic_demographics.csv",
        "CHAMPS deid verbal autopsy" => "CHAMPS_deid_verbal_autopsy.csv",
        "CHAMPS deid decode results" => "CHAMPS_deid_decode_results.csv",
        "CHAMPS deid tac results" => "CHAMPS_deid_tac_results.csv",
        "CHAMPS deid lab results" => "CHAMPS_deid_lab_results.csv" )

    # Data dictionaries
    datadictionaries::Vector{String} = [
        "CHAMPS_deid_basic_demographics_dictionary.xlsx",
        "CHAMPS_deid_decode_results_dictionary.xlsx",
        "CHAMPS_deid_tac_results_dictionary.xlsx",
        "CHAMPS_deid_lab_results_dictionary.xlsx",
        "CHAMPS_deid_verbal_autopsy_dictionary.xlsx"
        # "Format_CHAMPS_deid_basic_demographics.csv",
        # "Format_CHAMPS_deid_verbal_autopsy.csv",
        # "Format_CHAMPS_deid_decode_results.csv",
        # "Format_CHAMPS_deid_tac_results.csv",
        # "Format_CHAMPS_deid_lab_results.csv"
        ]

    # Metadata for ingestion 
    ingest_desc::String = "Raw CHAMPS level-2 Data accessed 20230518"
    ingest_date::Date = today() #"yyyy-mm-dd"
    
    # Metadata for transformation
    transform_desc::String = "Ingest of CHAMPS Level-2 Data"
    code_reference::Vector{UInt8} = b"RDAIngest.ingest_data"
    author::String = "Yue Chu, Kobus Herbst"

    # Default metadata for source ingestion
    unit_of_analysis_id::Integer = 1 #Individual
    transformation_type_id::Integer = 1 #Raw data ingest
    transformation_status_id::Integer = 2 # Verified

end


"""
Struct of ingest metadata for COMSA - Mozambique data 
"""

Base.@kwdef mutable struct COMSAMZIngest <: AbstractIngest 
    # Source
    source::AbstractSource = COMSAMZSource()

    # Unique identifier of a row
    id_col::String = "comsa_id"

    # Domain info
    domain_name::String = "COMSAMZ"
    domain_description::String = "Raw COMSA Mozambique level-2 deidentified data"

    # Death file
    death_file::String = "Comsa_WHO_VA_20230308.csv"
    # CSV format
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "u dd, yyyy" 
    decimal::Char = '.'

    # Datasets matching to deaths
    datasets::Dict{String,String} = Dict("COMSA Mozambique deid verbal autopsy" => "Comsa_WHO_VA_20230308.csv")

    # Data dictionaries
    datadictionaries::Vector{String} = [
        "Comsa_WHO_VA_20230308_dictionary.xlsx",
        "Comsa_death_20230308_dictionary.xlsx"
        # "Format_Comsa_death_20230308.csv",
        # "Format_Comsa_WHO_VA_20230308.csv"
    ]

    # Metadata for ingestion 
    ingest_desc::String = "Raw COMSA Mozambique level-2 Data accessed 20230518"
    ingest_date::Date = today() #"yyyy-mm-dd"
    
    # Metadata for transformation
    transform_desc::String = "Ingest of COMSA MZ Level-2 Data"
    code_reference::Vector{UInt8} = b"RDAIngest.ingest_data"
    author::String = "Yue Chu"

    # Default metadata for source ingestion
    unit_of_analysis_id::Integer = 1 #Individual
    transformation_type_id::Integer = 1 #Raw data ingest
    transformation_status_id::Integer = 2 # Verified
    
end

"""
Struct of ingest metadata for HEALSL (COMSA Sierra Leone) data 
"""

Base.@kwdef mutable struct HEALSLIngest <: AbstractIngest 
    # Source
    source::AbstractSource = HEALSLSource()

    # Unique identifier of a row
    id_col::String = "rowid"

    # Domain info
    domain_name::String = "HEALSL"
    domain_description::String = "Raw HEAL Sierra Leone level-2 deidentified data"

    # Death file
    death_file::String = "Comsa_WHO_VA_20230308.csv"
    # CSV format
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "u dd, yyyy" 
    decimal::Char = '.'

    # Datasets matching to deaths
    datasets::Dict{String,String} = Dict("HEALSL deid verbal autopsy" => "healsl_all_v1.csv")

    # Data dictionaries
    datadictionaries::Vector{String} = [
        "healsl_all_v1_dictionary.xlsx"
        # "Format_ddict_healsl.csv" 
    ]

    # Metadata for ingestion 
    ingest_desc::String = "Raw HEALSL level-2 Data accessed 20230518"
    ingest_date::Date = today() #"yyyy-mm-dd"
    
    # Metadata for transformation
    transform_desc::String = "Ingest of HEALSL Level-2 Data"
    code_reference::Vector{UInt8} = b"RDAIngest.ingest_data"
    author::String = "Yue Chu"

    # Default metadata for source ingestion
    unit_of_analysis_id::Integer = 1 #Individual
    transformation_type_id::Integer = 1 #Raw data ingest
    transformation_status_id::Integer = 2 # Verified
    
end

"""
Struct of ingest data and dictionary of user data products
"""
Base.@kwdef mutable struct userIngest <: AbstractIngest 

    # Unique identifier of a row
    id_col::String = "id"

    # Domain info
    domain_name::String = "userORCID"
    domain_description::String = "Domain for user ORCID"

    # Datasets
    datasets::Dict{String,DataFrame} #Dict("data1" => df1)

    # Data dictionaries
    datadictionaries::Dict{String,String} = ["data1.csv" => "data_1_dictionary.xlsx",
                                            "data2.csv" => "data_2_dictionary.xlsx"]

    # Transformation input - list of input datasets for each dataset
    input_datasets::Dict{String,Vector{Integer}} = Dict("data1.csv" => [1],
                                                        "data2.csv" => [2,3]) 
    
    # Metadata for ingestion 
    ingest_desc::String = "Raw CHAMPS level-2 Data accessed 20230518"
    ingest_date::Date = today() #"yyyy-mm-dd"

    # Metadata for transformation
    transform_desc::String = "Description of data processing methodology."
    code_reference::Vector{UInt8} = b"code.ipynb"
    author::String = "user ORCID"
    
    # Default metadata for user data ingestion
    unit_of_analysis_id::Integer = 2 #Aggregation
    transformation_type_id::Integer = 2 #Dataset transform
    transformation_status_id::Integer = 1 # Unverified

    # CSV format - specify if csv
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "yyyy-mm-dd"
    decimal::Char = '.'
    
end

"""
    ingest_source(source::AbstractSource, dbpath::String, dbname::String,
    datapath::String; sqlite=true)

Ingest macro data of sources: sites, instruments, protocols, ethics

datapath: root folder with data from all sources [DATA_INGEST_PATH]
dbpath: path to open RDA database
dbname: name of RDA database
"""

function ingest_source(source::AbstractSource, dbpath::String, dbname::String,
    datapath::String, iso3_path::String; sqlite=true)
    db = opendatabase(dbpath, dbname; sqlite)
    try
        DBInterface.transaction(db) do

            source_id = add_source(source.name, db)

            # Add study type id
            updatevalue(db, "sources", "source_id", "study_type_id", source_id, source.study_type_id)

            # Add sites and country iso2 codes
            add_sites(source, db, source_id, datapath, iso3_path)
            #@info "Site names, country names and country iso3 codes ingested."

            # Add instruments
            add_instruments(source, db, datapath)
            #@info "Instrument document $value ingested."

            # Add Protocols
            add_protocols(source, db, datapath)

            # Add Ethics
            add_ethics(source, db, datapath) 

        end

        return nothing
    finally
        DBInterface.close!(db)
    end
end
 

"""
    add_sites(source::AbstractSource, db::DBInterface.Connection, sourceid::Integer, datapath::String, iso3_path::String)

Add sites, countries and country iso3 codes to sites table
"""
function add_sites(source::AbstractSource, db::DBInterface.Connection, sourceid::Integer, datapath::String, iso3_path::String)
    
    # Get sites of the study
    df = read_data(DocCSV(joinpath(datapath, source.name, source.datafolder),
        source.site_data,
        source.delim, source.quotechar, source.dateformat, source.decimal))

    if (:country_col in fieldnames(typeof(source))) && (source.country_col != "")
        sites = unique(select(df, Symbol(source.site_col) => :site_name, 
                                Symbol(source.country_col) => :country_name))
    else
        sites = unique(select(df, Symbol(source.site_col) => :site_name))
        sites.country_name = fill(source.country_name, nrow(sites))
    end
    
    # Identify country name input and add iso3 
    iso3_mapping = CSV.File(iso3_path) |> DataFrame
    if all(length.(sites.country_name) .== 2)
        sites = transform(sites, :country_name => ByRow(uppercase) => :iso2)
        sites = leftjoin(sites, iso3_mapping, on = :iso2)
    elseif all(length.(sites.country_name) .== 3)
        sites = transform(sites, :country_name => ByRow(uppercase) => :iso3)
        sites = leftjoin(sites, iso3_mapping, on = :iso3)
    else
        sites.country = sites.country_name
        sites = leftjoin(sites, iso3_mapping, on = :country)
    end

    sites = select(sites, :site_name, :country => :country_name, :iso3 => :country_iso3)

    # Add sourceid 
    insertcols!(sites, 1, :source_id => sourceid)

    savedataframe(db, sites, "sites")
    
    return nothing
end


"""
    add_instruments(source::AbstractSource, db::SQLite.DB, datapath::String)

Add survey instruments
"""

function add_instruments(source::AbstractSource, db::DBInterface.Connection, datapath::String)

    # Insert instrument names
    stmt_name = prepareinsertstatement(db, "instruments", ["name", "description"])

    # Insert instrument documents
    stmt_doc = prepareinsertstatement(db, "instrument_documents", ["instrument_id", "name", "document"])

    for (key, value) in source.instruments

        # Add instruments
        DBInterface.execute(stmt_name, [key, value]) 

        # Get instrument id
        instrument_id = get_namedkey(db, "instruments", key, Symbol("instrument_id"))

        # Add instrument documents
        file = read_data(DocPDF(joinpath(datapath, source.name, source.instrumentfolder), "$key"))
        DBInterface.execute(stmt_doc, [instrument_id, key, file])
        
    end
    return nothing
end


"""
    add_protocols(source::AbstractSource, db::SQLite.DB, datapath::String)

Add protocols
Todo: how protocols link to enthics_id, need a mapping dictionary? 
"""
function add_protocols(source::AbstractSource, db::DBInterface.Connection, datapath::String)

    # Insert protocol names
    stmt_name = prepareinsertstatement(db, "protocols", ["name", "description"])

    # Insert protocol documents
    stmt_doc = prepareinsertstatement(db, "protocol_documents", ["protocol_id", "name", "document"])

    # Insert site protocols #Todo: current assumes all sites using standardized protocols
    stmt_site = prepareinsertstatement(db, "site_protocols", ["site_id", "protocol_id"])

    sites = selectsourcesites(db, source)

    for (key, value) in source.protocols

        # Add protocol
        DBInterface.execute(stmt_name, [key, value])

        # Get protocol id
        protocol_id = get_namedkey(db, "protocols", key, Symbol("protocol_id"))

        # Add site protocol
        for site in eachrow(sites)
            DBInterface.execute(stmt_site, [site.site_id, protocol_id])
        end

        # Add protocol documents
        file = read_data(DocPDF(joinpath(datapath, source.name, source.protocolfolder), "$key"))
        DBInterface.execute(stmt_doc, [protocol_id, key, file])
        # @info "Protocol document $value ingested."
    end
    return nothing
end


"""
    add_ethics(source::AbstractSource, db::DBInterface.Connection, datapath::String)

Ethics document, committee and reference need to be in matching order

"""

function add_ethics(source::AbstractSource, db::DBInterface.Connection, datapath::String)

    source_id = get_source(db, source.name)

    # Insert ethics names
    stmt_name = prepareinsertstatement(db, "ethics", ["source_id", "name", "ethics_committee", "ethics_reference"])

    for (key, value) in source.ethics
        DBInterface.execute(stmt_name, [source_id, key, value[2], value[3]])
    end

    # Insert ethics documents
    stmt_doc = prepareinsertstatement(db, "ethics_documents", ["ethics_id", "name", "description", "document"])

    for (key, value) in source.ethics
        file = read_data(DocPDF(joinpath(datapath, source.name, source.ethicsfolder), "$key"))

        # Get ethics id
        ethics_id = get_namedkey(db, "ethics", key, Symbol("ethics_id"))

        DBInterface.execute(stmt_doc, [ethics_id, key, value[1], file])
    
        # Update ethics_id in protocols
        protocol_id = get_namedkey(db, "protocols", value[3], Symbol("protocol_id"))

        sql = """
        UPDATE protocols
        SET 
            ethics_id = ?
        WHERE protocol_id = ? 
        """
        update_stmt = DBInterface.prepare(db, sql)
        DBInterface.execute(update_stmt, [ethics_id, protocol_id])

        # @info "Ethics document $(value[2]) ingested. Ethics id = $ethics_id."
    end
    return nothing
end

"""
    add_source(source_name, db::DBInterface.Connection)

Add source `name` to the sources table, and returns the `source_id`
"""
function add_source(source_name::String, db::DBInterface.Connection)
    id = get_source(db, source_name)
    if ismissing(id)  # insert source
        id = insertwithidentity(db, "sources", ["name"], [source_name], "source_id")
    end
    return id
end

"""
    get_source(db::DBInterface.Connection, name)

Return the `source_id` of source `name`, returns `missing` if source doesn't exist
"""
function get_source(db::DBInterface.Connection, name)
    return get_namedkey(db, "sources", name, :source_id)
end


"""
    ingest_deaths(ingest::AbstractIngest, dbpath::String, dbname::String, datapath::String; sqlite=true)

Ingest deaths to deathrows for source data, return ingestion_id. 
"""

function ingest_deaths(ingest::AbstractIngest, dbpath::String, dbname::String, datapath::String; sqlite=true)
    db = opendatabase(dbpath, dbname; sqlite)

    try
        DBInterface.transaction(db) do

            source_id = get_source(db, ingest.source.name)

            # Add ingestion info
            ingestion_id = insertwithidentity(db, "data_ingestions", ["source_id", "date_received", "description"], [source_id, isa(db, SQLite.DB) ? Dates.format(today(), "yyyy-mm-dd") : today(), ingest.ingest_desc], "data_ingestion_id")
            # transformation should not be created for the death ingestion

            # Ingest deaths
            deaths = read_data(DocCSV(joinpath(datapath, ingest.source.name, ingest.source.datafolder),
                ingest.death_file, ingest.delim, ingest.quotechar, ingest.dateformat, ingest.source.decimal))
            sites = selectdataframe(db, "sites", ["site_id", "site_name"], ["source_id"], [source_id])

            sitedeaths = innerjoin(transform!(deaths, Symbol(ingest.source.site_col) => :site_name),
                sites, on=:site_name, matchmissing=:notequal)

            savedataframe(db, select(sitedeaths, :site_id, Symbol(ingest.id_col) => :external_id,
                    [] => Returns(ingestion_id) => :data_ingestion_id, copycols=false), "deaths")


            return ingestion_id 
        end

    finally
        DBInterface.close!(db)
    end
end

"""
    ingest_dictionary(ingest::AbstractIngest, dbpath::String, dbname::String, dictionarypath::String; sqlite=true)

Ingest data dictionaries, add variables and vocabularies
Expect either an xlsx with sheets "variables" and "vocabularies"
Or a csv with single sheets containing vocabularies as multiple lines, with delimiter ";"
"""

function ingest_dictionary(ingest::AbstractIngest, dbpath::String, dbname::String, dictionarypath::String; sqlite=true)
    db = opendatabase(dbpath, dbname; sqlite)

    try
        DBInterface.transaction(db) do
            # @info "Ingest dictionaries for $(source.name). sqlite = $sqlite"
            domain = add_domain(db, ingest.domain_name, ingest.domain_description)

            # Add variables
            for filename in ingest.datadictionaries

                #filename = "CHAMPS_deid_basic_demographics_dictionary.xlsx"
                ext = split(filename, ".") |> last
                if ext == "xlsx"
                    sheets = XLSX.readxlsx(joinpath(dictionarypath, "$(ingest.domain_name)", filename))
                    if length(XLSX.sheetnames(sheets))==1
                        dict_var = DocXLSX(joinpath(dictionarypath, "$(ingest.domain_name)"), filename, XLSX.sheetnames(sheets)[1]) 
                        dict_voc = missing
                    else
                        #Expected xlsx with sheets variables and vocabularies 
                        dict_var = DocXLSX(joinpath(dictionarypath, "$(ingest.domain_name)"), filename, "variables") # Variables
                        dict_voc = DocXLSX(joinpath(dictionarypath, "$(ingest.domain_name)"), filename, "vocabularies") # Vocabularies
                    end
                elseif ext == "csv"
                    #Expected csv with single sheet delimited by semi-colons, not the default source delimeter
                    dict_var = DocCSV(joinpath(dictionarypath, "$(source.domain_name)"), filename, 
                    ';', ingest.quotechar, ingest.dateformat, ingest.decimal)
                    dict_voc = missing
                else 
                    error("Data dictionary '$filename' not supported. Please use .xlsx or .csv format.")
                end
                
                variables = read_variables(dict_var, dict_voc)
                add_variables(variables, db, domain)

                # @info "Variables from $filename ingested."
            end

            # Mark key fields for easier reference later
            row = lookup_variables(db, ingest.id_col, domain)
            DBInterface.execute(db, "UPDATE variables SET keyrole = 'id' WHERE domain_id = $domain AND variable_id = $(row.variable_id[1])")

            # if source is available in ingest
            if ingest.source!=Missing  
                row = lookup_variables(db, ingest.source.site_col, domain)
                DBInterface.execute(db, "UPDATE variables SET keyrole = 'site_name' WHERE domain_id = $domain AND variable_id = $(row.variable_id[1])")
            end
        end
        return nothing
    finally
        DBInterface.close!(db)
    end
end


"""
    add_domain(db::DBInterface.Connection, domain_name::String, domain_description::String)

Add domain to the domain table if not exist, and returns the domain id
"""
function add_domain(db::DBInterface.Connection, domain_name::String, domain_description::String="")
    domain = get_domain(db, domain_name)
    if ismissing(domain)  # insert source
        domain = insertwithidentity(db, "domains", ["name", "description"], [domain_name, domain_description], "domain_id")
    end
    return domain
end

"""
    get_domain(db::DBInterface.Connection, domain_name::String)

Return the domain_id for domain named `domain_name`
"""
get_domain(db::DBInterface.Connection, domain_name::String) = get_namedkey(db, "domains", domain_name, Symbol("domain_id"))


"""
    read_variables(dict_var::DocXLSX, dict_voc::DocXLSX)

Read variable table `dict_var` listing variables, variable descriptions and data types in a dataframe.
Read vocabulary table `dict_voc` listing vocabulary item for each variable, with descriptions in a dataframe.

For dict_var read from .csv file where vocabularies are part of the variables table, dict_voc is set to be missing.
"""

# function read_variables(dict_var::DocXLSX, dict_voc::DocXLSX)

#     vocabularies = Vector{Union{Vocabulary,Missing}}()

#     df_var = read_data(dict_var) 
#     df_voc = read_data(dict_voc) 
#     if size(df_voc)[1]==0
#         df_var.Vocabulary .= missing
#     else
#         for row in eachrow(df_var)
#             if row.Column_Name in unique(df_voc.Column_Name)
#                 push!(vocabularies, get_vocabulary(row.Column_Name, df_voc, row.Description))
#             else
#                 push!(vocabularies, missing)
#             end
#         end
#         df_var.Vocabulary = vocabularies
#     end
    
#     return df_var
# end

function read_variables(dict_var::DocXLSX, dict_voc::Union{Missing,DocXLSX})

    vocabularies = Vector{Union{Vocabulary,Missing}}()

    df_var = read_data(dict_var) 
    if dict_voc==Missing
        df_var.Vocabulary .= missing
    else 
        df_voc = read_data(dict_voc) 
        if size(df_voc)[1]==0
            df_var.Vocabulary .= missing
        else
            for row in eachrow(df_var)
                if row.Column_Name in unique(df_voc.Column_Name)
                    push!(vocabularies, get_vocabulary(row.Column_Name, df_voc, row.Description))
                else
                    push!(vocabularies, missing)
                end
            end
            df_var.Vocabulary = vocabularies
        end
    end
    return df_var
end


function read_variables(dict_var::DocCSV, dict_voc::Missing)
    vocabularies = Vector{Union{Vocabulary,Missing}}()
        
    df_var = read_data(dict_var) 

    for row in eachrow(df_var)
        if !ismissing(row.Description) && length(lines(row.Description)) > 1
            l = lines(row.Description)
            push!(vocabularies, get_vocabulary(row.Column_Name, l))
            row.Description = l[1]
        else
            push!(vocabularies, missing)
        end
    end

    df_var.Vocabulary = vocabularies

    return df_var
end

"""
    get_vocabulary(variable, l)::Vocabulary

Get a vocabulary, name of vocabulary in line 1 of l, vocabulary items (code and description) in subsequent lines, comma-separated
"""
function get_vocabulary(name, l::Vector{SubString{String}})::Vocabulary
    items = Vector{VocabularyItem}()
    description = ""
    for i in eachindex(l)
        if i == 1
            description = l[i]
        else
            item = split(l[i], ',')
            push!(items, length(item) > 1 ? VocabularyItem(i - 1, item[1], item[2]) : VocabularyItem(i - 1, item[1], missing))
        end
    end
    return Vocabulary(name, description, items)
end

"""
    get_vocabulary(variable, dataframe_vocabulary, variable_description)::Vocabulary

Get a vocabulary items (code and description) for a variable named "name" from vocabulary dataframe
"""
function get_vocabulary(name, df_voc, var_description)::Vocabulary
    voc = df_voc[df_voc.Column_Name .==name,:] 
    items = Vector{VocabularyItem}()
    i=0
    for row in eachrow(voc)
        i=i+1
        push!(items, VocabularyItem(i, row.Items, row.Description))
    end
    return Vocabulary(name, var_description, items)
end


"""
    add_variables(variables::AbstractDataFrame, db::SQLite.DB, domain_id::Integer)

Add variables from a variable dataframe to variables table
"""
function add_variables(variables::AbstractDataFrame, db::SQLite.DB, domain_id::Integer)

    # Check if variables dataframe has all required columns
    required_columns = ["Column_Name", "DataType", "Description", "Note", "Vocabulary"]
    missing_columns = filter(col -> !(col in names(variables)), required_columns)
    if !isempty(missing_columns)
        error("Variables dataframe missing columns: ", join(missing_columns, ", "))
    end

    # Variable insert SQL
    sql = raw"""
    INSERT INTO variables (domain_id, name, value_type_id, vocabulary_id, description, note)
    VALUES (@domain_id, @name, @value_type_id, @vocabulary_id, @description, @note)
    ON CONFLICT DO UPDATE
    SET vocabulary_id = excluded.vocabulary_id,
        description = excluded.description,
        note = excluded.note 
    WHERE variables.vocabulary_id IS NULL OR variables.description IS NULL OR variables.note IS NULL;
    """
    stmt = DBInterface.prepare(db, sql)

    # Add variables
    if !("domain_id" in names(variables))
        insertcols!(variables, 1, :domain_id => domain_id)
    end

    for row in eachrow(variables)
        id = missing
        if !ismissing(row.Vocabulary)
            id = add_vocabulary(db, row.Vocabulary)
        end
        DBInterface.execute(stmt, (domain_id=row.domain_id, name=row.Column_Name,
            value_type_id=row.DataType, vocabulary_id=id,
            description=row.Description, note=row.Note))
    end
    return nothing
end

"""
    add_variables(variables::AbstractDataFrame, db::ODBC.Connection, domain_id::Integer)

Add variables from a variable dataframe to variables table
"""
function add_variables(variables::AbstractDataFrame, db::ODBC.Connection, domain_id::Integer)

    # Check if variables dataframe has all required columns
    required_columns = ["Column_Name", "DataType", "Description", "Note", "Vocabulary"]
    missing_columns = filter(col -> !(col in names(variables)), required_columns)
    if !isempty(missing_columns)
        error("Variables dataframe missing columns: ", join(missing_columns, ", "))
    end
    # ODBC can't deal with InlineStrings
    transform!(variables, :Column_Name => ByRow(x -> String(x)) => :Column_Name)

    # Check if variable exists
    sql = """
        SELECT 1 
        FROM variables 
        WHERE domain_id = ? AND name = ?
    """
    exist_stmt = DBInterface.prepare(db, sql)
    sql = """
        UPDATE variables
        SET 
            vocabulary_id = ?,
            description = ?,
            note = ?
        WHERE domain_id = ? AND name = ? 
    """
    update_stmt = DBInterface.prepare(db, sql)
    sql = """
    INSERT INTO variables (domain_id, name, value_type_id, vocabulary_id, description, note)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    insert_stmt = DBInterface.prepare(db, sql)
    # Add variables
    if !("domain_id" in names(variables))
        insertcols!(variables, 1, :domain_id => domain_id)
    end

    for row in eachrow(variables)
        id = missing
        if !ismissing(row.Vocabulary)
            id = add_vocabulary(db, row.Vocabulary)
        end
        var_exist = !isempty(DBInterface.execute(exist_stmt, [row.domain_id, row.Column_Name]))
        if var_exist
            DBInterface.execute(update_stmt, [id, row.Description, row.Note, row.domain_id, row.Column_Name])
        else
            DBInterface.execute(insert_stmt, [row.domain_id, row.Column_Name, row.DataType, id, row.Description, row.Note])
        end
    end
    return nothing
end


"""
    add_vocabulary(db::SQLite.DB, vocabulary::Vocabulary)

Insert a vocabulary and its items into a RDA database, returns the vocabulary_id of the inserted vocabulary
"""
function add_vocabulary(db, vocabulary::Vocabulary)
    id = get_namedkey(db, "vocabularies", vocabulary.name, "vocabulary_id")
    if !ismissing(id)
        return id
    end
    id = insertwithidentity(db, "vocabularies", ["name", "description"], [vocabulary.name, vocabulary.description], "vocabulary_id")
    stmt = prepareinsertstatement(db, "vocabulary_items", ["vocabulary_id", "value", "code", "description"])
    for item in vocabulary.items
        DBInterface.execute(stmt, [id, item.value, item.code, item.description])
    end
    return id
end


"""
    lookup_variables(db, variable_names, domain)

Returns a DataFrame with dataset variable names and ids
"""
function lookup_variables(db, variable_names, domain)
    names = DataFrame(:name => variable_names)
    variables = selectdataframe(db, "variables", ["name", "variable_id", "value_type_id"], ["domain_id"], [domain]) |> DataFrame
    return innerjoin(variables, names, on=:name) #just the variables in this dataset
end

"""
read_data(datadoc)

Read file names and formatting parameters, returns a DataFrame with the data
"""
function read_data(datadoc::DocPDF)
    file = joinpath(datadoc.path, "$(datadoc.name)") #.pdf or .docx needs to be specified
    extension = last(split(datadoc.name, "."))
    if !isfile(file)
        error("File '$file' not found.")
    else # lowercase(extension) == "docx"
        df = read(file)
        df = Base64.base64encode(df)
        return df
    end
end
function read_data(datadoc::DocCSV)
    file = joinpath(datadoc.path, "$(datadoc.name)") #.csv needs to be specified
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=datadoc.delim, quotechar=datadoc.quotechar,
            dateformat=datadoc.dateformat, decimal=datadoc.decimal) |> DataFrame
        return df
    end
end
# function read_data(datadoc::DocXLSX)
#     file = joinpath(datadoc.path, "$(datadoc.name)") #.xlsx needs to be specified
#     if !isfile(file)
#         error("File '$file' not found.")
#     else
#         df = DataFrame(XLSX.readdata(file, datadoc.sheetname, datadoc.cellrange), :auto)
#         return df
#     end
# end
function read_data(datadoc::DocXLSX)
    file = joinpath(datadoc.path, "$(datadoc.name)") #.xlsx needs to be specified
    if !isfile(file)
        error("File '$file' not found.")
    else
        #df = DataFrame(XLSX.readdata(file, datadoc.sheetname), :auto)
        df = DataFrame(XLSX.getdata(XLSX.readxlsx(file)[datadoc.sheetname]),:auto)
        # First row as column name
        df = rename(df,[Symbol(x) for x in values(df[1,:])])
        # Drop first row
        df = df[2:size(df)[1],:]
        return df
    end
end

"""
    ingest_data(ingest::AbstractIngest, dbpath::String, dbname::String, datapath::String; ingestion_id=0, sqlite=true)
    
    ingest datasets, for source data link to deathrows
"""

function ingest_data(ingest::AbstractIngest, dbpath::String, dbname::String, datapath::String; ingestion_id=0, sqlite=true)
    db = opendatabase(dbpath, dbname; sqlite)
    try
        source_id = add_source(ingest.source.name, db)
        domain_id = add_domain(db, ingest.domain_name, ingest.domain_description)
        death_idvar = get_variable_id(db, domain_id, ingest.id_col)

        DBInterface.transaction(db) do
            if ingestion_id == 0
                ingestion_id = insertwithidentity(db, "data_ingestions", ["source_id", "date_received", "description"], [source_id, isa(db, SQLite.DB) ? Dates.format(today(), "yyyy-mm-dd") : today(), ingest.ingest_desc], "data_ingestion_id")
            end

            transformation_id = insertwithidentity(db, "transformations", ["transformation_type_id", "transformation_status_id", "description", "code_reference", "date_created", "created_by"],
                [ingest.transformation_type_id, ingest.transformation_status_id, 
                ingest.transform_desc, ingest.code_reference, 
                isa(db, SQLite.DB) ? Dates.format(today(), "yyyy-mm-dd") : today(), ingest.author], "transformation_id")

            for (dataset_desc, dataset_name) in ingest.datasets

                # Import datasets
                ds = read_data(DocCSV(joinpath(datapath, ingest.source.name, ingest.source.datafolder), dataset_name,
                    ingest.delim, ingest.quotechar, ingest.dateformat, ingest.decimal))

                dataset_id = save_dataset(db, ds, dataset_name, dataset_desc, ingest.unit_of_analysis_id,
                    domain_id, transformation_id, ingestion_id)

                # Link to deathrows
                if !death_in_ingest(db, ingestion_id)
                    # @info "Death data is not part of currrent data ingest $ingestion_id"
                    death_ingestion_id = get_last_deathingest(db, source_id)
                    # @info "Death ingestion id not specified. By default, use lastest ingested deaths from source $(ingest.source.name) from ingestion id $death_ingestion_id."
                else
                    death_ingestion_id = ingestion_id
                end
                # @info "Linking dataset $dataset_name to deathrows from ingestion id $death_ingestion_id, dataset_id = $dataset_id, death_idvar = $death_idvar"
                link_deathrows(db, death_ingestion_id, dataset_id, death_idvar)

                # @info "Dataset $dataset_name imported and linked to deathrows."

            end

            # Link to instruments in instrument_datasets
            instruments = ingest.source.instruments
            if !isempty(instruments)
                for (instrument_name, dataset_name) in instruments #instrument name, dataset name
                    link_instruments(db, instrument_name, dataset_name)
                    # @info "Linked dataset $dataset_name to instrument $instrument_name"
                end
            end
        end
    finally
        DBInterface.close!(db)
    end
end


"""
    save_dataset(db::DBInterface.Connection, dataset::AbstractDataFrame, name::String, description::String, unit_of_analysis_id::Integer,
    domain_id::Integer, transformation_id::Integer, ingestion_id::Integer)::Integer

Insert dataframe containing dataset into RDA database and returns the dataset_id
"""
function save_dataset(db::DBInterface.Connection, dataset::AbstractDataFrame, name::String, description::String, unit_of_analysis_id::Integer,
    domain_id::Integer, transformation_id::Integer, ingestion_id::Integer)::Integer

    variables = lookup_variables(db, names(dataset), domain_id)
    transform!(variables, [:variable_id, :value_type_id] => ByRow((x, y) -> Tuple([x, y])) => :variable_id_type)
 
    var_lookup = Dict{String,Tuple{Integer,Integer}}(zip(variables.name, variables.variable_id_type))

    # Add dataset entry to datasets table
    dataset_id = insertwithidentity(db, "datasets", ["name", "date_created", "description", "unit_of_analysis_id"],
        [name, isa(db, SQLite.DB) ? Dates.format(today(), "yyyy-mm-dd") : today(), description, unit_of_analysis_id], "dataset_id")

    insertdata(db, "ingest_datasets", ["data_ingestion_id", "transformation_id", "dataset_id"],
        [ingestion_id, transformation_id, dataset_id])
    insertdata(db, "transformation_outputs", ["transformation_id", "dataset_id"],
        [transformation_id, dataset_id])

    savedataframe(db, select(variables, [] => Returns(dataset_id) => :dataset_id, :variable_id), "dataset_variables")

    # Store datarows in datarows table and get row_ids 
    datarows = add_datarows(db, nrow(dataset), dataset_id)

    #prepare data for storage
    d = hcat(datarows, dataset, makeunique=true, copycols=false) #add the row_id to each row of data
    #store whole column at a time
    for col in propertynames(dataset)
        variable_id, value_type = var_lookup[string(col)]
        coldata = select(d, :row_id, col => :value; copycols=false)
        #println("Saving column $col : type=$(eltype(coldata.value)) : value_type=$value_type : $name.")
        add_data_column(db, variable_id, value_type, coldata)
    end
    # @info "Dataset $name ingested."
    return dataset_id
end


"""
    add_datarows(db::SQLite.DB, nrow::Integer, dataset_id::Integer)

    Define data rows in the datarows table
"""
function add_datarows(db::DBInterface.Connection, nrow::Integer, dataset_id::Integer)
    stmt = prepareinsertstatement(db, "datarows", ["dataset_id"])
    #Create a row_id for every row in the dataset
    for i = 1:nrow
        DBInterface.execute(stmt, [dataset_id])
    end
    return selectdataframe(db, "datarows", ["row_id"], ["dataset_id"], [dataset_id]) |> DataFrame
end


"""
    add_data_column(db::SQLite.DB, variable_id, coldata)

Insert data for a column of the source dataset
"""
function add_data_column(db::SQLite.DB, variable_id, value_type, coldata)
    stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value"])
    if eltype(coldata.value) <: TimeType
        if value_type == RDA_TYPE_DATE
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x, "yyyy-mm-dd") : x) => :value)
        elseif value_type == RDA_TYPE_TIME
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x, "HH:MM:SS.sss") : x) => :value)
        elseif value_type == RDA_TYPE_DATETIME
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x, "yyyy-mm-ddTHH:MM:SS.sss") : x) => :value)
        else
            error("Variable $variable_id is not a date/time type. value_type = $value_type, eltype = $(eltype(coldata.value))")
        end
    end
    for row in eachrow(coldata)
        DBInterface.execute(stmt, [row.row_id, variable_id, row.value])
    end
    return nothing
end
"""
    add_data_column(db::ODBC.Connection, variable_id, value_type, coldata)

Insert data for a column of the source dataset
"""
function add_data_column(db::ODBC.Connection, variable_id, value_type, coldata)
    #println("Add data column variable_id = $variable_id, value_type = $value_type, eltype = $(eltype(coldata.value))")
    if value_type == RDA_TYPE_INTEGER
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_integer"])
    elseif value_type == RDA_TYPE_FLOAT
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_float"])
    elseif value_type == RDA_TYPE_STRING
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_string"])
        if eltype(coldata.value) <: Union{Missing,Number}
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? string(x) : x) => :value)
        elseif eltype(coldata.value) <: Union{Missing,TimeType}
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x,"yyyy-mm-dd") : x) => :value)
        else
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? String(x) : x) => :value)
        end
    elseif value_type == RDA_TYPE_DATE || value_type == RDA_TYPE_TIME || value_type == RDA_TYPE_DATETIME
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_datetime"])
    elseif value_type == RDA_TYPE_CATEGORY && eltype(coldata.value) <: Union{Missing,Integer}
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_integer"])
    elseif value_type == RDA_TYPE_CATEGORY && eltype(coldata.value) <: Union{Missing,AbstractString}
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_string"])
        transform!(coldata, :value => ByRow(x -> !ismissing(x) ? String(x) : x) => :value)
    else
        error("Variable $variable_id is not a valid type. value_type = $value_type, eltype = $(eltype(coldata.value))")
    end
    for row in eachrow(coldata)
        DBInterface.execute(stmt, [row.row_id, variable_id, row.value])
    end
    return nothing
end


"""
    death_in_ingest(db, ingestion_id)

If ingested deaths as part of ingestion_id
"""
function death_in_ingest(db, ingestion_id)
    df = selectdataframe(db, "deaths", ["COUNT(*)"], ["data_ingestion_id"], [ingestion_id]) |> DataFrame
    return nrow(df) > 0 && df[1, 1] > 0
end

"""
    get_last_deathingest(source)

Get ingestion id for latest death ingestion for source
"""
function get_last_deathingest(db::SQLite.DB, source_id::Integer)
    sql = """
    SELECT 
        di.data_ingestion_id
    FROM data_ingestions di
    JOIN deaths d ON di.data_ingestion_id = d.data_ingestion_id
    WHERE source_id = @source_id
    ORDER BY di.date_received DESC
    LIMIT 1;
    """
    death_ingests = DBInterface.execute(db, sql, (source_id = source_id)) |> DataFrame
    if nrow(death_ingests) > 0
        return death_ingests[1, :data_ingestion_id]
    else
        error("Death from source $source_id hasn't been ingested.")
    end
end

"""
    get_last_deathingest(db::ODBC.Connection, source_id::Integer)

Get ingestion id for latest death ingestion for source
"""
function get_last_deathingest(db::ODBC.Connection, source_id::Integer)
    sql = """
    SELECT TOP 1
        di.data_ingestion_id
    FROM data_ingestions di
    JOIN deaths d ON di.data_ingestion_id = d.data_ingestion_id
    WHERE source_id = ?
    ORDER BY di.date_received DESC;
    """
    death_ingests = DBInterface.execute(db, sql, [source_id]) |> DataFrame
    if nrow(death_ingests) > 0
        return death_ingests[1, :data_ingestion_id]
    else
        error("Death from source $source_id hasn't been ingested.")
    end
end


"""
    link_deathrows(db::SQLite.DB, ingestion_id, dataset_id, death_identifier)

Insert records into `deathrows` table to link dataset `dataset_id` to `deaths` table. Limited to a specific ingest.
`death_identifier` is the variable in the dataset that corresponds to the `external_id` of the death.
"""
function link_deathrows(db::SQLite.DB, ingestion_id, dataset_id, death_identifier)

    sql = """
    INSERT OR IGNORE INTO death_rows (death_id, row_id)
    SELECT
        d.death_id,
        data.row_id
    FROM deaths d
        JOIN data ON d.external_id = data.value
        JOIN datarows r ON data.row_id = r.row_id
    WHERE d.data_ingestion_id = @ingestion_id
    AND data.variable_id = @death_identifier
    AND r.dataset_id = @dataset_id
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (ingestion_id=ingestion_id, death_identifier=death_identifier, dataset_id=dataset_id))

    return nothing
end
"""
    link_deathrows(db::ODBC.Connection, ingestion_id, dataset_id, death_identifier)

Insert records into `deathrows` table to link dataset `dataset_id` to `deaths` table. Limited to a specific ingest.
`death_identifier` is the variable in the dataset that corresponds to the `external_id` of the death.
"""
function link_deathrows(db::ODBC.Connection, ingestion_id, dataset_id, death_identifier)

    sql = """
        INSERT INTO death_rows (death_id, row_id)
        SELECT
            d.death_id,
            data.row_id
        FROM deaths d
        JOIN data ON d.external_id = data.value_string
        JOIN datarows r ON data.row_id = r.row_id
        WHERE d.data_ingestion_id = ?
        AND data.variable_id = ?
        AND r.dataset_id = ?
        AND NOT EXISTS (
            SELECT 1
            FROM death_rows dr
            WHERE dr.death_id = d.death_id AND dr.row_id = data.row_id
        )
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, [ingestion_id, death_identifier, dataset_id])

    return nothing
end



"""
    link_instruments(db::SQLite.DB, instrument_name, dataset_name)

Insert records into `instrument_datasets` table, linking datasets with instruments.
"""
function link_instruments(db::DBInterface.Connection, instrument_name::String, dataset_name::String)

    # get id for dataset and matching instrument
    dataset_id = get_namedkey(db, "datasets", dataset_name, :dataset_id)
    instrument_id = get_namedkey(db, "instruments", instrument_name, :instrument_id)

    if ismissing(dataset_id)
        error("Data file $dataset_name is not ingested.")
    end
    if ismissing(instrument_id)
        error("Instrument file $instrument_name is not ingested.")
    end

    insertdata(db, "instrument_datasets", ["instrument_id", "dataset_id"], [instrument_id, dataset_id])

    return nothing
end


"""
Supporting fuctions
"""


"""
    get_namedkey(db::DBInterface.Connection, table, key, keycol)

 Return the integer key from table `table` in column `keycol` (`keycol` must be a `Symbol`) for key with name `key`
"""
function get_namedkey(db::DBInterface.Connection, table, key, keycol)
    stmt = prepareselectstatement(db, table, ["*"], ["name"])
    df = DBInterface.execute(stmt, [key]) |> DataFrame
    if nrow(df) == 0
        return missing
    else
        return df[1, keycol]
    end
end



"""
    get_variable_id(db::SQLite.DB, domain, name)

Returns the `variable_id` of variable named `name` in domain with id `domain`
"""
function get_variable_id(db::SQLite.DB, domain, name)
    stmt = prepareselectstatement(db, "variables", ["variable_id"], ["domain_id", "name"])
    result = DBInterface.execute(stmt, [domain, name]) |> DataFrame
    if nrow(result) == 0
        return missing
    else
        return result[1, :variable_id]
    end
end


"""
    get_variable_id(db::DBInterface.Connection, domain, name)

    Returns the `variable_id` of variable named `name` in domain with id `domain`
"""
function get_variable_id(db::ODBC.Connection, domain, name)
    stmt = prepareselectstatement(db, "variables", ["variable_id"], ["domain_id", "name"])
    result = DBInterface.execute(stmt, [domain, name]; iterate_rows=true) |> DataFrame
    if nrow(result) == 0
        return missing
    else
        return result[1, :variable_id]
    end
end


"""
    get_variable(db::SQLite.DB, variable_id::Integer)

Returns the entry of variable with `variable_id`
"""
function get_variable(db::SQLite.DB, variable_id::Integer)
    stmt = prepareselectstatement(db, "variables", ["*"], ["variable_id"])
    result = DBInterface.execute(stmt, [variable_id]) |> DataFrame
    if nrow(result) == 0
        return missing
    else
        return result
    end
end


"""
    rbind(dfs::Vector{DataFrame})

    Row binding dataframes. If a column is missing in a dataframe, fill with missing value.
"""

function rbind(dfs::Vector{DataFrame})
    all_columns = union([names(df) for df in dfs]...)
    for df in dfs
        for col in setdiff(all_columns, names(df))
            df[!, col] = Vector{Union{Missing, Int}}(missing, nrow(df))
        end
    end
    ouptut = vcat(dfs...)
    return output
end


"""
    lines(str)

Returns an array of lines in `str` 
"""
lines(str) = split(str, '\n')


"""
Export dataset 
"""

"""
    dataset_to_dataframe(db::SQLite.DB, dataset)::AbstractDataFrame

Return a dataset with id `dataset` as a DataFrame in the wide format
"""
function dataset_to_dataframe(db::SQLite.DB, dataset)::AbstractDataFrame
    sql = """
    SELECT
        d.row_id,
        v.name variable,
        d.value
    FROM data d
      JOIN datarows r ON d.row_id = r.row_id
      JOIN variables v ON d.variable_id = v.variable_id
    WHERE r.dataset_id = @dataset;
    """
    stmt = DBInterface.prepare(db, sql)
    long = DBInterface.execute(stmt, (dataset = dataset)) |> DataFrame
    return unstack(long, :row_id, :variable, :value)
end

"""
    dataset_to_dataframe(db::ODBC.Connection, dataset)::AbstractDataFrame

Extract a dataset into a DataFrame
"""
function dataset_to_dataframe(db::ODBC.Connection, dataset)::AbstractDataFrame
    variables = dataset_variables(db, dataset)
    df = DataFrame()
    col1 = true
    for variable in eachrow(variables)
       if col1
           df = dataset_column(db, dataset, variable.variable_id, variable.variable)
           col1 = false
       else
           df = outerjoin(df, dataset_column(db, dataset, variable.variable_id, variable.variable), on=:row_id)
       end 
    end
    return df
end


"""
    dataset_variables(db::SQLite.DB, dataset)::AbstractDataFrame

Return the list of variables in a dataset
"""
function dataset_variables(db::SQLite.DB, dataset)::AbstractDataFrame
    sql = """
    SELECT
        v.variable_id,
        v.name variable,
        v.value_type_id
    FROM dataset_variables dv
      JOIN variables v ON dv.variable_id = v.variable_id
    WHERE dv.dataset_id = @dataset;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, (dataset = dataset)) |> DataFrame
end
"""
    dataset_variables(db::ODBC.Connection, dataset)::AbstractDataFrame

Return the list of variables in a dataset
"""
function dataset_variables(db::ODBC.Connection, dataset)::AbstractDataFrame
    sql = """
    SELECT
        v.variable_id,
        v.name variable,
        v.value_type_id
    FROM dataset_variables dv
      JOIN variables v ON dv.variable_id = v.variable_id
    WHERE dv.dataset_id = ?;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, [dataset]; iterate_rows=true) |> DataFrame
end


"""
    dataset_to_arrow(db, dataset, datapath)

Save a dataset in the arrow format
"""
function dataset_to_arrow(db, dataset, datapath)
    outputdir = joinpath(datapath, "arrowfiles")
    if !isdir(outputdir)
        mkpath(outputdir)
    end
    df = dataset_to_dataframe(db, dataset)
    Arrow.write(joinpath(outputdir, "$(get_datasetname(db,dataset)).arrow"), df, compress=:zstd)
end


"""
    dataset_to_csv(db, dataset_id, datapath, compress)

Save a dataset in compressed csv format
"""
function dataset_to_csv(db, dataset_id, datapath, compress=false)
    outputdir = joinpath(datapath, "csvfiles")
    if !isdir(outputdir)
        mkpath(outputdir)
    end
    df = dataset_to_dataframe(db, dataset_id)
    if (compress)
        CSV.write(joinpath(outputdir, "$(get_datasetname(db,dataset_id)).gz"), df, compress=true) #have trouble opening on MacOS
    else
        CSV.write(joinpath(outputdir, "$(get_datasetname(db,dataset_id)).csv"), df)
    end
end


"""
    dataset_column(db::SQLite.DB, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame

Return one column of data in a dataset (representing a variable)
"""
function dataset_column(db::SQLite.DB, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame
    sql = """
    SELECT
        d.row_id,
        d.value as $variable_name
    FROM data d
      JOIN datarows r ON d.row_id = r.row_id
    WHERE r.dataset_id = @dataset_id
      AND d.variable_id = @variable_id;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, (dataset_id = dataset_id, variable_id=variable_id)) |> DataFrame
end

"""
    dataset_column(db::ODBC.Connection, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame

Return one column of data in a dataset (representing a variable)
"""
function dataset_column(db::ODBC.Connection, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame
    value_column = get_valuetype(db, variable_id)
    sql = """
    SELECT
        d.row_id,
        $value_column as $variable_name
    FROM data d
      JOIN datarows r ON d.row_id = r.row_id
    WHERE r.dataset_id = ?
      AND d.variable_id = ?;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, [dataset_id, variable_id]; iterate_rows=true) |> DataFrame
end


"""
    get_valuetype(db::ODBC.Connection, variable_id::Integer)

Get the data type of a variable
"""
function get_valuetype(db::ODBC.Connection, variable_id::Integer)
    sql = """
    SELECT
        value_type_id
    FROM variables
    WHERE variable_id = ?;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, [variable_id]; iterate_rows=true) |> DataFrame
    if nrow(df) > 0
        if df[1, :value_type_id] == RDA_TYPE_INTEGER
            return "value_integer"
        elseif df[1, :value_type_id] == RDA_TYPE_FLOAT
            return "value_float"
        elseif df[1, :value_type_id] == RDA_TYPE_STRING
            return "value_string"
        elseif df[1, :value_type_id] == RDA_TYPE_DATE || df[1, :value_type_id] == RDA_TYPE_TIME || df[1, :value_type_id] == RDA_TYPE_DATETIME
            return "value_datetime"
        elseif df[1, :value_type_id] == RDA_TYPE_CATEGORY
            return "COALESCE(CAST(d.value_integer AS varchar), d.value_string)"
        else 
            error("Variable $variable_id has an invalid value type $(df[1, :value_type_id]).")
        end
    else
        error("Variable $variable_id not found.")
    end
end

"""
    get_datasetname(db::SQLite.DB, dataset)

Return dataset name, given the `dataset_id`
"""
function get_datasetname(db::SQLite.DB, dataset)
    sql = """
    SELECT
      name
    FROM datasets
    WHERE dataset_id = @dataset
    """
    stmt = DBInterface.prepare(db, sql)
    result = DBInterface.execute(stmt, (dataset = dataset))
    if isempty(result)
        return missing
    else
        df = DataFrame(result)
        return df[1, :name]
    end
end
"""
    get_datasetname(db::ODBC.Connection, dataset)

Get the name of a dataset
"""
function get_datasetname(db::ODBC.Connection, dataset)
    sql = """
    SELECT
      name
    FROM datasets
    WHERE dataset_id = ?
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, [dataset], iterate_rows=true) |> DataFrame
    if nrow(df) == 0
        return missing
    else
        return df[1, :name]
    end
end

include("constants.jl")
include("rdadatabase.jl")

end #module
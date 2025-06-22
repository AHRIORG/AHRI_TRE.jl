using Pkg
Pkg.develop(PackageSpec(path="/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/RDAIngest.jl"))
# Pkg.develop(PackageSpec(path="/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/RDAClean.jl"))
# Pkg.develop(PackageSpec(path="/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/RDAConvert.jl"))
# Pkg.develop(PackageSpec(path="/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/RDANada.jl"))

using RDAIngest
using ConfigEnv
using Logging
using DBInterface
using DataFrames
using Dates
using CSV
using SQLite
#using BenchmarkTools

#get environment variables
dotenv()


@info "===== Create CHAMPS Lab variable descriptions from vocabulary"

lab_pre = CSV.read(joinpath(ENV["DATA_INGEST_PATH"],"CHAMPS","De_identified_data","CHAMPS_deid_lab_variables.csv"), DataFrame)
lab_voc = CSV.read(joinpath(ENV["DATA_INGEST_PATH"],"CHAMPS","De_identified_data","CHAMPS_vocabulary.csv"), DataFrame)
lab = CSV.read(joinpath(ENV["DATA_INGEST_PATH"],"CHAMPS","De_identified_data","CHAMPS_deid_lab_results.csv"), DataFrame)

dict = DataFrame(Column_Name = names(lab))
dict.Sample_code = [split(x, r"(?i)_ch")[1] for x in dict.Column_Name]
dict.Result_code = [uppercase(split(row.Column_Name, string(row.Sample_code,"_"))[end]) for row in eachrow(dict)]
dict = leftjoin(dict, lab_pre, on=:Sample_code)
dict.Description = ifelse.(ismissing.(dict.Desc),dict.Desc,dict.Desc .* ": ")

for i in 1:nrow(dict)
    for j in 1:nrow(lab_voc)
        if occursin(lab_voc.champs_local_code[j], dict.Result_code[i])
            dict.Description[i] *= lab_voc.c_pref_name[j] * ", "  # Concatenate label values
        end
    end
    if occursin(r"_\d$", dict.Result_code[i])  # Check if the string ends with "_x"
        result = match(r"\d$", dict.Result_code[i]).match
        dict.Description[i] *= " result " * result
    end
    if !ismissing(dict.Description[i])
        if endswith(dict.Description[i],", ")
            dict.Description[i] = dict.Description[i][1:end-2]
        end
    end
end

dict.Description = replace.(dict.Description, "," => "^")
dict.Key = map(x -> x == "champs_deid" ? "Yes" : "", dict.Column_Name)
dict.Note .= ""
dict.DataType .= 3

dict = select(dict, :Column_Name, :Key, :Description, :Note, :DataType)
file = joinpath(ENV["DATA_DICTIONARY_PATH"],"CHAMPS","Format_CHAMPS_deid_lab_results.csv")
CSV.write(file, dict; delim=';', quotechar='"', decimal='.')

#tac = CSV.read(joinpath(ENV["DATA_INGEST_PATH"],"CHAMPS","De_identified_data","CHAMPS_deid_tac_results.csv"), DataFrame)



@info "===== Update CHAMPS variable data type"

# file = joinpath(dictionarypath,source.name, "$(filename).csv") #default as .csv file
# raw_dict = CSV.File(file; delim=";", quotechar=source.quotechar,
#     dateformat=source.dateformat, decimal=source.decimal) |> DataFrame

# # Fix Data Type errors
# fix = raw_dict[occursin.(r"(?i)record the date", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.DataType,Ref(fix)), :DataType] .=4 #.& in.(raw_dict.Column_Name, Ref(duplicates))

# fix = raw_dict[occursin.(r"(?i)the weight", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=2

# fix = raw_dict[occursin.(r"(?i)how old", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=2

# fix = raw_dict[occursin.(r"(?i)how many", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=2

# fix = raw_dict[occursin.(r"(?i)how long", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=2

# fix = raw_dict[occursin.(r"(?i)_unit", raw_dict.Column_Name), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=7

# fix = raw_dict[occursin.(r"(?i)enter length of", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=2

# CSV.write(file, raw_dict; delim=";", quotechar='"')

# df = CSV.read(file, DataFrame; delim=";", quotechar='"')


@info "===== Update HEALSL variable table"

# raw_dict = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/ddict_healsl.csv", DataFrame)

# # raw_dict = combine(groupby(raw_dict, [:column_name, :description, :type]), 
# #                    :data_name => (x -> join(x, " & ")) => :Note)
# raw_dict.Note = fill("", nrow(raw_dict))

# # Fix Data Type errors
# fix = ["id10011", "id10481"]
# #map!(a -> in(a, fix) ? "timestamp with time zone" : a, raw_dict.type, raw_dict.column_name)
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="timestamp with time zone" 

# fix = raw_dict[occursin.(r"(?i)record the date", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="date" #.& in.(raw_dict.column_name, Ref(duplicates))

# fix = ["id10021","id10023_a"]
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="date" 

# fix = raw_dict[occursin.(r"(?i)calculated number", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="double precision"

# fix = raw_dict[occursin.(r"(?i)what was the weight", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="double precision"

# fix = raw_dict[occursin.(r"(?i)how old", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="double precision"

# fix = raw_dict[occursin.(r"(?i)how many", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="double precision" 

# fix = raw_dict[occursin.(r"(?i)how long", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="double precision"

# fix = raw_dict[occursin.(r"(?i)_unit", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="text"

# fix = raw_dict[occursin.(r"(?i)_unit", raw_dict.column_name), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="text"

# raw_dict[occursin.(r"(?i)when ", raw_dict.description), :]

# raw_dict = unique(raw_dict,[:column_name, :description, :type])

# # Multiple datatype provided in the data dictionary for the same variable
# duplicates = filter(r -> count(x -> x == r.column_name, raw_dict.column_name) > 1, eachrow(raw_dict)).column_name
# check = DataFrame(filter(row -> row.column_name in duplicates, eachrow(raw_dict)))
# println(sort(check,:column_name))
# raw_dict[in.(raw_dict.column_name,Ref(duplicates)), :type] .="text"

# # Remove duplicates
# raw_dict = unique(raw_dict,[:column_name, :description, :type])

# # Map datatype to RDA datatypes 
# unique(raw_dict.type)
# value_type_mapping = Dict("integer" => 1, "double precision" => 2, "text" => 3,"date"=>4,
#                           "timestamp with time zone"=>5, "time"=>6, "category"=>7) 
#                           #type 3, 6, 7 doesn't exist in current version (v1), listed as placeholders
# raw_dict.DataType = map(x -> get(value_type_mapping, x, missing), raw_dict.type)

# # Flag key variable
# raw_dict.Key = map(x -> x == "rowid" ? "Yes" : "", raw_dict.column_name)

# # Format dictionary file and save a copy
# rename!(raw_dict, :column_name => :Column_Name)
# rename!(raw_dict, :description => :Description)

# raw_dict.Description = replace.(raw_dict.Description, "," => "^")

# new_dict = select(raw_dict, :Column_Name, :Key, :Description, :Note, :DataType)
# new_dict = DataFrame("Column_Name;Key;Description;Note;DataType" => [join(map(x -> string(x), row), ";") for row in eachrow(new_dict)])

# file = "/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/RDA/Data Dictionaries/HEALSL/Format_ddict_healsl.csv"
# CSV.write(file, new_dict; delim=';', quotechar='"', decimal='.')
# # Needs one last step of manual replacing ; in csv, otherwise CSV.File has trouble recognizing the delim.

# db_path = "/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/RDAIngest.jl/database/RDA.sqlite"
# db = SQLite.DB(db_path)

@info "===== Get variables and descriptions for all variables"

# Load database
dbname = "RDA"
db = opendatabase(ENV["RDA_DATABASE_PATH"],dbname)
filepath = joinpath(pwd(),"test")

sql = "SELECT * FROM variables"
df = DataFrame(DBInterface.execute(db,sql))

vars = unique(select(filter(row -> row.domain_id <=3 , df), :name, :description))

CSV.write(joinpath(filepath,"variable_descriptions.csv"), vars)

vars2 = CSV.read(joinpath(filepath,"variable_descriptions.csv"), DataFrame)
CSV.write(joinpath(filepath,"variable_descriptions.txt"), vars2, delim = ";")
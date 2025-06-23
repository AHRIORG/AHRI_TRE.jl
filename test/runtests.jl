using RDAIngest
using Test

#@testset "RDAIngest.jl" begin
    # Write your tests here.
#end

using Pkg
using DataFrames
using CSV

# Replace "path/to/your/file.csv" with the actual path to your CSV file
df1 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd1_who_adult_v1.csv", DataFrame)
df2 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd2_who_adult_v1.csv", DataFrame)
df3 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd3_who_adult_v1.csv", DataFrame)

df4 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd1_who_child_v1.csv", DataFrame)
df5 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd2_who_child_v1.csv", DataFrame)
df6 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd3_who_child_v1.csv", DataFrame)

df7 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd1_who_neo_v1.csv", DataFrame)
df8 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd2_who_neo_v1.csv", DataFrame)
df9 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd3_who_neo_v1.csv", DataFrame)


function rbind(dfs::Vector{DataFrame})
    all_columns = union([names(df) for df in dfs]...)
    for df in dfs
        for col in setdiff(all_columns, names(df))
            df[!, col] = Vector{Union{Missing, Int}}(missing, nrow(df))
        end
    end
    vcat(dfs...)
    return vcat(dfs...)
end

df = rbind([df1,df2,df3,df4,df5,df6,df7,df8,df9])

println(df)
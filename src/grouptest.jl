using DataFrames
using XLSX

xf = XLSX.readxlsx(raw"D:\Data\RDA\Data\DataIngests\CHAMPS\De_identified_data\CHAMPS_deid_tac_vocabulary.xlsx")
pathogens = pathogens = XLSX.gettable(xf[1]) |> DataFrame
insertcols!(pathogens, 1, :vocabulary_id => 0) #to record saved vocabulary
select!(pathogens, :vocabulary_id, :Pathogen => :name, Symbol("Multi-target result code") => :description)
for row in eachrow(pathogens)
    df = XLSX.gettable(xf[row.name]) |> DataFrame
    select!(df, :Interpretation => ByRow(x -> strip(x)) => :Interpretation, AsTable(Not(:Interpretation)) =>
        ByRow(x -> replace(join([join([keys(x)[i], values(x)[i]], ":") for i in 1:length(x)], ";"), " " => "")) => :values)
    gdf = groupby(df, :Interpretation)
    r = combine(gdf, groupindices => :key, :values => (x -> join(x, "|")) => :description)
    show(r)
end
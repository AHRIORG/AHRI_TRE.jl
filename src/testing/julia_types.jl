using Dates

function showtypetree(T, level=0)
    println("\t"^level, T)
    for t in subtypes(T)
        showtypetree(t, level + 1)
    end
end

showtypetree(Dates.Date)
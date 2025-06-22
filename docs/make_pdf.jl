using RDAIngest
using Documenter

DocMeta.setdocmeta!(RDAIngest, :DocTestSetup, :(using RDAIngest); recursive=true)

makedocs(;
    modules=[RDAIngest],
    authors="Kobus Herbst<kobus.herbst@ahri.org>",
    repo="https://github.com/RDAORG/RDAIngest.jl/blob/{commit}{path}#{line}",
    sitename="RDAIngest.jl",
    format=Documenter.LaTeX(),
    pages=[
        "Introduction" => "introduction.md",
        "Functions" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/RDAORG/RDAIngest.jl",
    devbranch="main",
    push_preview=true
)

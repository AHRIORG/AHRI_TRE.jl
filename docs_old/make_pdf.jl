using RDAIngest
using Documenter

DocMeta.setdocmeta!(RDAIngest, :DocTestSetup, :(using RDAIngest); recursive=true)

makedocs(;
    modules=[RDAIngest],
    authors="Kobus Herbst<kobus.herbst@ahri.org>",
    repo="github.com/AHRIORG/AHRI_TRE.jl/blob/{commit}{path}#{line}",
    sitename="AHRI_TRE.jl",
    format=Documenter.LaTeX(),
    pages=[
        "Introduction" => "introduction.md",
        "Functions" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/AHRIORG/AHRI_TRE.jl",
    devbranch="main",
    push_preview=true
)

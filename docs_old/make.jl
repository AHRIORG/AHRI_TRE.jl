using AHRI_TRE
using Documenter

DocMeta.setdocmeta!(RDAIngest, :DocTestSetup, :(using RDAIngest); recursive=true)

makedocs(;
    modules=[AHRI_TRE],
    authors="Kobus Herbst<kobus.herbst@ahri.org>",
    repo="https://github.com/AHRIORG/AHRI_TRE.jl/blob/{commit}{path}#{line}",
    sitename="AHRI_TRE.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://kobusherbst.github.io/AHRI_TRE.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Introduction" => "introduction.md",
        "Functions" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/AHRIORG/AHRI_TRE.jl",
    devbranch="main",
    push_preview = true,
)

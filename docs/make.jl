using RDAIngest
using Documenter

DocMeta.setdocmeta!(RDAIngest, :DocTestSetup, :(using RDAIngest); recursive=true)

makedocs(;
    modules=[RDAIngest],
    authors="Kobus Herbst<kobus.herbst@ahri.org>, Yue Chu<ychu612@gmail.com>",
    repo="https://github.com/RDAORG/RDAIngest.jl/blob/{commit}{path}#{line}",
    sitename="RDAIngest.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://kobusherbst.github.io/RDAIngest.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Introduction" => "introduction.md",
        "Functions" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/RDAORG/RDAIngest.jl",
    devbranch="main",
    push_preview = true,
)

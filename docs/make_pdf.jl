using Documenter
using DocumenterTypst
using .AHRI_TRE

makedocs(
    sitename = "AHRI_TRE",
    format = DocumenterTypst.Typst(),
    modules = [AHRI_TRE],
    pages = [
        "Home" => "index.md",
        "Introduction" => "introduction.md",
        "API"  => "api.md",
    ],
    checkdocs = :exports, # or :all; or set to :none to silence
)

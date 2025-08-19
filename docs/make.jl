using Documenter
using AHRI_TRE

makedocs(
    sitename = "AHRI_TRE",
    format = Documenter.HTML(sidebar_sitename = false, size_threshold=1_000_000, size_threshold_warn=500_000),
    modules = [AHRI_TRE],
    pages = [
        "Home" => "index.md",
        "Introduction" => "introduction.md",
        "API"  => "api.md",
    ],
    checkdocs = :exports, # or :all; or set to :none to silence
)

# Documenter can also automatically deploy documentation to gh-pages.
# See "Hosting Documentation" and deploydocs() in the Documenter manual
# for more information.
#=deploydocs(
    repo = "<repository url>"
)=#

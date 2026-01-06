using Test

@testset "git_commit_info script path detection" begin
    # `pathof(AHRI_TRE)` points at the module entrypoint (typically `.../src/AHRI_TRE.jl`).
    package_src_dir = abspath(dirname(pathof(AHRI_TRE)))

    internal_utils = abspath(joinpath(package_src_dir, "utils.jl"))
    external_script_in_repo = abspath(joinpath(package_src_dir, "testing", "commit_test.jl"))
    external_script_in_src_root = abspath(joinpath(package_src_dir, "ingest_episodes_db.jl"))

    @test AHRI_TRE._is_package_internal_source(internal_utils, package_src_dir)
    @test !AHRI_TRE._is_package_internal_source(external_script_in_repo, package_src_dir)
    @test !AHRI_TRE._is_package_internal_source(external_script_in_src_root, package_src_dir)
end

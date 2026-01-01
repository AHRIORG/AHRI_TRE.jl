using AHRI_TRE
using Test
using UUIDs
using DBInterface 

function _gather_env_datafiles(keys::Vector{String})
    values = Dict{String,String}()
    missing = String[]
    for key in keys
        value = get(ENV, key, "")
        if isempty(value)
            push!(missing, key)
        else
            values[key] = value
        end
    end
    return values, missing
end

function _tre_port_datafiles()::Int
    port_str = get(ENV, "TRE_PORT", "5432")
    try
        return parse(Int, port_str)
    catch
        return 5432
    end
end

function open_tre_store_with_lake()
    keys = [
        "TRE_SERVER",
        "TRE_USER",
        "TRE_PWD",
        "TRE_DBNAME",
        "TRE_LAKE_PATH",
        "LAKE_USER",
        "LAKE_PASSWORD",
    ]
    values, missing = _gather_env_datafiles(keys)
    if !isempty(missing)
        return nothing, missing
    end

    store = AHRI_TRE.DataStore(
        server=values["TRE_SERVER"],
        user=values["TRE_USER"],
        password=values["TRE_PWD"],
        dbname=values["TRE_DBNAME"],
        lake_data=values["TRE_LAKE_PATH"],
        lake_user=values["LAKE_USER"],
        lake_password=values["LAKE_PASSWORD"],
        lake_db=get(ENV, "TRE_LAKE_DB", "ducklake_catalog"),
    )

    try
        conn, lake = AHRI_TRE.opendatastore(
            store.server,
            store.user,
            store.password,
            store.dbname,
            store.lake_data,
            store.lake_db,
            store.lake_user,
            store.lake_password;
            port=_tre_port_datafiles(),
        )
        store.store = conn
        store.lake = lake
        return store, String[]
    catch e
        @warn "Unable to open TRE datastore" exception=(e, catch_backtrace())
        return nothing, ["TRE datastore connection failed"]
    end
end

@testset "DataFile ingest + metadata" begin
    store, missing = open_tre_store_with_lake()
    if isnothing(store)
        @test_skip "TRE datastore not configured: $(join(missing, ", "))"
        return
    end

    try
        domain_name = "domain_" * replace(string(uuid4()), "-" => "")
        study_name = "study_" * replace(string(uuid4()), "-" => "")
        asset_name = "file_" * replace(string(uuid4()), "-" => "")

        domain = AHRI_TRE.add_domain!(store, AHRI_TRE.Domain(name=domain_name, description="Test domain for datafiles"))
        study = AHRI_TRE.Study(
            name=study_name,
            description="Test study for datafiles",
            external_id="ext_" * replace(string(uuid4()), "-" => ""),
            study_type_id=AHRI_TRE.TRE_STUDY_TYPE_SURVEY,
        )
        study = AHRI_TRE.add_study!(store, study, domain)

        mktempdir() do tmp
            # v1
            file_v1 = joinpath(tmp, "data_v1.csv")
            write(file_v1, "a,b\n1,2\n")

            df1 = AHRI_TRE.ingest_file(
                store,
                study,
                asset_name,
                file_v1,
                "http://edamontology.org/format_3752";
                description="v1",
                compress=false,
                new_version=false,
            )
            @test df1 !== nothing
            @test df1.version !== nothing
            @test df1.version.version_id isa UUID
            @test df1.version.asset.asset_type == "file"

            meta1 = AHRI_TRE.get_datafile_metadata(store, AHRI_TRE.DataFile(version=df1.version))
            @test meta1 !== nothing
            @test meta1.version.version_id == df1.version.version_id
            @test meta1.storage_uri == df1.storage_uri
            @test meta1.digest == df1.digest

            listed_latest = AHRI_TRE.get_study_datafiles(store, study)
            @test any(f -> f.version.version_id == df1.version.version_id, listed_latest)

            # v2 via ingest_file_version (defaults should bump patch)
            file_v2 = joinpath(tmp, "data_v2.csv")
            write(file_v2, "a,b\n3,4\n")

            df2 = AHRI_TRE.ingest_file_version(store, file_v2, df1, false, false, "v2")
            @test df2 !== nothing
            @test df2.version !== nothing
            @test df2.version.version_id isa UUID
            @test df2.version.version_id != df1.version.version_id
            @test df2.version.asset.asset_id == df1.version.asset.asset_id
            @test (df2.version.major, df2.version.minor, df2.version.patch) == (df1.version.major, df1.version.minor, df1.version.patch + 1)

            meta2 = AHRI_TRE.get_datafile_metadata(store, AHRI_TRE.DataFile(version=df2.version))
            @test meta2.version.version_id == df2.version.version_id
            @test meta2.digest == df2.digest

            listed_all = AHRI_TRE.get_study_datafiles(store, study; include_versions=true)
            ids = [f.version.version_id for f in listed_all]
            @test df1.version.version_id in ids
            @test df2.version.version_id in ids
        end
    finally
        try
            AHRI_TRE.closedatastore(store)
        catch
            try
                DBInterface.close!(store.store)
            catch
            end
        end
    end
end

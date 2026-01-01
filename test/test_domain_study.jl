using AHRI_TRE
using Test
using Random
using UUIDs
using DBInterface

function _gather_env(keys::Vector{String})
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

function _tre_port()::Int
    port_str = get(ENV, "TRE_PORT", "5432")
    try
        return parse(Int, port_str)
    catch
        return 5432
    end
end

function open_tre_store_nolake()
    keys = ["TRE_SERVER", "TRE_USER", "TRE_PWD", "TRE_DBNAME"]
    values, missing = _gather_env(keys)
    if !isempty(missing)
        return nothing, missing
    end

    store = AHRI_TRE.DataStore(
        server=values["TRE_SERVER"],
        user=values["TRE_USER"],
        password=values["TRE_PWD"],
        dbname=values["TRE_DBNAME"],
    )

    try
        conn, lake = AHRI_TRE.opendatastore(
            store.server,
            store.user,
            store.password,
            store.dbname,
            nothing,
            nothing,
            nothing,
            nothing;
            port=_tre_port(),
        )
        store.store = conn
        store.lake = lake
        return store, String[]
    catch e
        @warn "Unable to open TRE datastore" exception=(e, catch_backtrace())
        return nothing, ["TRE datastore connection failed"]
    end
end

@testset "Domain/Study CRUD" begin
    store, missing = open_tre_store_nolake()
    if isnothing(store)
        @test_skip "TRE datastore not configured: $(join(missing, ", "))"
        return
    end

    try
        @testset "Domain CRUD" begin
            domain_name = "domain_" * replace(string(uuid4()), "-" => "")
            domain = AHRI_TRE.Domain(name=domain_name, description="Test domain")

            domain = AHRI_TRE.add_domain!(store, domain)
            @test domain.domain_id isa Int

            # Duplicate (name, uri=NULL) should error.
            @test_throws ErrorException AHRI_TRE.add_domain!(store, AHRI_TRE.Domain(name=domain_name, description="Duplicate"))

            fetched = AHRI_TRE.get_domain(store, domain_name)
            @test fetched !== nothing
            @test fetched.domain_id == domain.domain_id
            @test fetched.name == domain.name
            @test fetched.description == domain.description

            domain.description = "Updated test domain"
            domain.uri = "http://example.org/" * replace(string(uuid4()), "-" => "")
            AHRI_TRE.update_domain(store, domain)

            fetched2 = AHRI_TRE.get_domain(store, domain_name; uri=domain.uri)
            @test fetched2 !== nothing
            @test fetched2.domain_id == domain.domain_id
            @test fetched2.uri == domain.uri
            @test fetched2.description == domain.description

            all_domains = AHRI_TRE.get_domains(store)
            @test any(d -> d.domain_id == domain.domain_id, all_domains)

            # Duplicate (name, uri=non-NULL) should error.
            @test_throws ErrorException AHRI_TRE.add_domain!(store, AHRI_TRE.Domain(name=domain_name, uri=domain.uri, description="Duplicate"))
        end

        @testset "Study CRUD + Domains" begin
            domain_a = AHRI_TRE.Domain(name="domain_" * replace(string(uuid4()), "-" => ""), description="Domain A")
            domain_a = AHRI_TRE.add_domain!(store, domain_a)

            domain_b = AHRI_TRE.Domain(name="domain_" * replace(string(uuid4()), "-" => ""), description="Domain B")
            domain_b = AHRI_TRE.add_domain!(store, domain_b)

            study_name = "study_" * replace(string(uuid4()), "-" => "")
            study = AHRI_TRE.Study(
                name=study_name,
                description="Test study",
                external_id="ext_" * replace(string(uuid4()), "-" => ""),
                study_type_id=AHRI_TRE.TRE_STUDY_TYPE_SURVEY,
            )

            study = AHRI_TRE.add_study!(store, study, domain_a)
            @test study.study_id isa UUID

            got_by_name = AHRI_TRE.get_study(store, study_name)
            @test got_by_name !== nothing
            @test got_by_name.study_id == study.study_id
            @test any(d -> d.domain_id == domain_a.domain_id, got_by_name.domains)

            got_by_id = AHRI_TRE.get_study(store, study.study_id)
            @test got_by_id !== nothing
            @test got_by_id.name == study_name

            studies = AHRI_TRE.get_studies(store)
            @test any(s -> s.study_id == study.study_id, studies)

            # Add a second domain and verify `get_study_domains` returns both (ordered by name).
            AHRI_TRE.add_study_domain!(store, study, domain_b)
            domains = AHRI_TRE.get_study_domains(store, study)
            ids = [d.domain_id for d in domains]
            @test domain_a.domain_id in ids
            @test domain_b.domain_id in ids
            @test issorted([d.name for d in domains])

            # Idempotent for the same domain object.
            before = length(study.domains)
            AHRI_TRE.add_study_domain!(store, study, domain_b)
            @test length(study.domains) == before
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

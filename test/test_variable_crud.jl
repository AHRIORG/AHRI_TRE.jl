using AHRI_TRE
using Test
using UUIDs
using DBInterface

unique_test_suffix() = replace("$(time_ns())_$(getpid())_$(uuid4())", "-" => "")

function _gather_env_varcrud(keys::Vector{String})
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

function open_tre_store_varcrud()
    keys = ["TRE_SERVER", "TRE_USER", "TRE_PWD", "TRE_TEST_DBNAME"]
    values, missing = _gather_env_varcrud(keys)
    if !isempty(missing)
        return nothing, missing
    end

    # PostgreSQL-only for this test (no DuckDB lake attach).
    store = AHRI_TRE.DataStore(
        server=values["TRE_SERVER"],
        user=values["TRE_USER"],
        password=values["TRE_PWD"],
        dbname=values["TRE_TEST_DBNAME"],
    )

    try
        conn, lake = AHRI_TRE.opendatastore(
            values["TRE_SERVER"],
            values["TRE_USER"],
            values["TRE_PWD"],
            values["TRE_TEST_DBNAME"],
            nothing,
            nothing,
            nothing,
            nothing,
        )
        store.store = conn
        store.lake = lake
        return store, String[]
    catch e
        @warn "Unable to open TRE datastore" exception=(e, catch_backtrace())
        return nothing, ["TRE datastore connection failed"]
    end
end

@testset "Variable creation + retrieval" begin
    store, missing = open_tre_store_varcrud()
    if isnothing(store)
        @test_skip "TRE datastore not configured: $(join(missing, ", "))"
        return
    end

    domain_id = nothing
    variable_id = nothing
    try
        suffix = unique_test_suffix()
        domain_name = "domain_varcrud_" * suffix
        var_name = "var_" * suffix

        domain = AHRI_TRE.add_domain!(store, AHRI_TRE.Domain(name=domain_name))
        @test domain.domain_id !== nothing
        domain_id = Int(domain.domain_id)

        v = AHRI_TRE.Variable(
            domain_id=Int(domain.domain_id),
            name=var_name,
            value_type_id=AHRI_TRE.TRE_TYPE_INTEGER,
            description="test var",
        )

        v = AHRI_TRE.add_variable!(store, v)
        @test v.variable_id !== nothing
        @test Int(v.variable_id) > 0
        variable_id = Int(v.variable_id)

        fetched_by_id = AHRI_TRE.get_variable(store, Int(v.variable_id))
        @test fetched_by_id !== missing
        @test fetched_by_id.variable_id == v.variable_id
        @test fetched_by_id.domain_id == Int(domain.domain_id)
        @test fetched_by_id.name == var_name
        @test fetched_by_id.value_type_id == AHRI_TRE.TRE_TYPE_INTEGER
        @test fetched_by_id.description == "test var"

        # Retrieve by (domain name, variable name)
        fetched_by_name = AHRI_TRE.get_variable(store, domain_name, var_name)
        @test fetched_by_name !== missing
        @test fetched_by_name.variable_id == v.variable_id
        @test fetched_by_name.name == var_name
    finally
        # Cleanup: delete variable then domain (FK order)
        try
            if !isnothing(variable_id)
                DBInterface.execute(store.store, raw"DELETE FROM variables WHERE variable_id = $1;", (Int(variable_id),))
            end
            if !isnothing(domain_id)
                DBInterface.execute(store.store, raw"DELETE FROM domains WHERE domain_id = $1;", (Int(domain_id),))
            end
        catch e
            @warn "Variable test cleanup failed" exception=(e, catch_backtrace())
        end
        AHRI_TRE.closedatastore(store)
    end
end

@testset "Categorical variable updates vocabulary_id" begin
    store, missing = open_tre_store_varcrud()
    if isnothing(store)
        @test_skip "TRE datastore not configured: $(join(missing, ", "))"
        return
    end

    domain_id = nothing
    variable_id = nothing
    vocab_id_1 = nothing
    vocab_id_2 = nothing
    try
        suffix = unique_test_suffix()
        domain_name = "domain_varcrud_cat_" * suffix
        var_name = "var_cat_" * suffix
        vocab_name_1 = "vocab_cat_1_" * suffix
        vocab_name_2 = "vocab_cat_2_" * suffix

        domain = AHRI_TRE.add_domain!(store, AHRI_TRE.Domain(name=domain_name))
        @test domain.domain_id !== nothing
        domain_id = Int(domain.domain_id)

        items_v1 = AHRI_TRE.VocabularyItem[
            AHRI_TRE.VocabularyItem(vocabulary_id=0, value=1, code="A", description="Alpha"),
            AHRI_TRE.VocabularyItem(vocabulary_id=0, value=2, code="B", description="Beta"),
        ]
        vocab_v1 = AHRI_TRE.Vocabulary(
            domain_id=Int(domain.domain_id),
            name=vocab_name_1,
            description="desc v1",
            items=AHRI_TRE.AbstractVocabularyItem[items_v1...],
        )

        v = AHRI_TRE.Variable(
            domain_id=Int(domain.domain_id),
            name=var_name,
            value_type_id=AHRI_TRE.TRE_TYPE_CATEGORY,
            description="categorical var",
            vocabulary=vocab_v1,
        )

        v = AHRI_TRE.add_variable!(store, v)
        @test v.variable_id !== nothing
        variable_id = Int(v.variable_id)
        @test !ismissing(v.vocabulary_id)
        vocab_id_1 = Int(v.vocabulary_id)
        @test vocab_id_1 > 0

        fetched_1 = AHRI_TRE.get_variable(store, Int(v.variable_id))
        @test fetched_1 !== missing
        @test !ismissing(fetched_1.vocabulary_id)
        @test Int(fetched_1.vocabulary_id) == vocab_id_1
        @test fetched_1.vocabulary !== missing
        @test fetched_1.vocabulary.name == vocab_name_1

        # Update the variable to point at a *different* vocabulary and ensure the
        # persisted vocabulary_id changes accordingly.
        items_v2 = AHRI_TRE.VocabularyItem[
            AHRI_TRE.VocabularyItem(vocabulary_id=0, value=10, code="X", description="Ex"),
        ]
        vocab_v2 = AHRI_TRE.Vocabulary(
            domain_id=Int(domain.domain_id),
            name=vocab_name_2,
            description="desc v2",
            items=AHRI_TRE.AbstractVocabularyItem[items_v2...],
        )

        v.vocabulary = vocab_v2
        v = AHRI_TRE.update_variable!(store, v)
        @test !ismissing(v.vocabulary_id)
        vocab_id_2 = Int(v.vocabulary_id)
        @test vocab_id_2 > 0
        @test vocab_id_2 != vocab_id_1

        fetched_2 = AHRI_TRE.get_variable(store, Int(v.variable_id))
        @test fetched_2 !== missing
        @test !ismissing(fetched_2.vocabulary_id)
        @test Int(fetched_2.vocabulary_id) == vocab_id_2
        @test fetched_2.vocabulary !== missing
        @test fetched_2.vocabulary.name == vocab_name_2
    finally
        # Cleanup: delete variable, then vocabularies, then domain (respecting foreign key constraints)
        try
            if !isnothing(variable_id)
                DBInterface.execute(store.store, raw"DELETE FROM variables WHERE variable_id = $1;", (Int(variable_id),))
            end

            for vid in (vocab_id_1, vocab_id_2)
                if !isnothing(vid)
                    DBInterface.execute(store.store, raw"DELETE FROM vocabulary_items WHERE vocabulary_id = $1;", (Int(vid),))
                    DBInterface.execute(store.store, raw"DELETE FROM vocabularies WHERE vocabulary_id = $1;", (Int(vid),))
                end
            end
            
            if !isnothing(domain_id)
                DBInterface.execute(store.store, raw"DELETE FROM domains WHERE domain_id = $1;", (Int(domain_id),))
            end
        catch e
            @warn "Categorical variable test cleanup failed" exception=(e, catch_backtrace())
        end
        AHRI_TRE.closedatastore(store)
    end
end

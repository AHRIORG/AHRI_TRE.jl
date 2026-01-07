using AHRI_TRE
using Test
using UUIDs
using DBInterface

unique_test_suffix() = replace("$(time_ns())_$(getpid())_$(uuid4())", "-" => "")

function _gather_env_vocab(keys::Vector{String})
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

function open_tre_store_vocab()
    keys = ["TRE_SERVER", "TRE_USER", "TRE_PWD", "TRE_TEST_DBNAME"]
    values, missing = _gather_env_vocab(keys)
    if !isempty(missing)
        return nothing, missing
    end

    # Avoid opening/attaching the DuckDB lake in this test; we only need the
    # PostgreSQL metadata store connection.
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

@testset "Vocabulary CRUD" begin
    store, missing = open_tre_store_vocab()
    if isnothing(store)
        @test_skip "TRE datastore not configured: $(join(missing, ", "))"
        return
    end

    vocab_id_1 = nothing
    try
        vocab_name = "vocab_" * unique_test_suffix()

        # 1) Add via ensure_vocabulary! and retrieve
        items_v1 = AHRI_TRE.VocabularyItem[
            AHRI_TRE.VocabularyItem(vocabulary_id=0, value=1, code="A", description="Alpha"),
            AHRI_TRE.VocabularyItem(vocabulary_id=0, value=2, code="B", description="Beta"),
        ]

        vocab_id_1 = AHRI_TRE.ensure_vocabulary!(store, vocab_name, "desc v1", items_v1)
        @test vocab_id_1 isa Int
        @test vocab_id_1 > 0

        fetched_1 = AHRI_TRE.get_vocabulary(store, vocab_name)
        @test fetched_1 !== nothing
        @test fetched_1.vocabulary_id == vocab_id_1
        @test fetched_1.name == vocab_name
        @test fetched_1.description == "desc v1"
        @test length(fetched_1.items) == 2
        @test Set([(it.value, it.code) for it in fetched_1.items]) == Set([(1, "A"), (2, "B")])

        # 2) Assert the UNIQUE constraint exists (without relying on a thrown error
        # that can be noisy in CI logs).
        df_constraint = DBInterface.execute(
            store.store,
            raw"""
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'public.vocabularies'::regclass
                  AND contype = 'u'
                  AND conname = 'vocabularies_name_key';
            """,
        ) |> DataFrame
        @test nrow(df_constraint) == 1

        # 3) Update via ensure_vocabulary! (same name, new description + new items)
        items_v2 = AHRI_TRE.VocabularyItem[
            AHRI_TRE.VocabularyItem(vocabulary_id=0, value=10, code="X", description="Ex"),
        ]

        vocab_id_2 = AHRI_TRE.ensure_vocabulary!(store, vocab_name, "desc v2", items_v2)
        @test vocab_id_2 == vocab_id_1

        fetched_2 = AHRI_TRE.get_vocabulary(store, vocab_name)
        @test fetched_2 !== nothing
        @test fetched_2.vocabulary_id == vocab_id_1
        @test fetched_2.description == "desc v2"
        @test length(fetched_2.items) == 1
        @test fetched_2.items[1].value == 10
        @test fetched_2.items[1].code == "X"
        @test fetched_2.items[1].description == "Ex"
        @test fetched_2.items[1].vocabulary_id == vocab_id_1
        @test fetched_2.items[1].vocabulary_item_id !== nothing
    finally
        # Cleanup: keep test DB from accumulating vocabularies across runs
        if !isnothing(vocab_id_1)
            try
                DBInterface.execute(store.store, raw"DELETE FROM vocabulary_items WHERE vocabulary_id = $1;", (Int(vocab_id_1),))
                DBInterface.execute(store.store, raw"DELETE FROM vocabularies WHERE vocabulary_id = $1;", (Int(vocab_id_1),))
            catch e
                @warn "Vocabulary test cleanup failed" exception=(e, catch_backtrace())
            end
        end
        AHRI_TRE.closedatastore(store)
    end
end

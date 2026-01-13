using AHRI_TRE
using Test
using UUIDs
using DBInterface
using DataFrames

unique_test_suffix() = replace("$(time_ns())_$(getpid())_$(uuid4())", "-" => "")

@testset "Vocabulary CRUD - Domain Scoping" begin
    if isnothing(TRE_TEST_STORE)
        @test_skip "TRE test datastore not configured"
        return
    end

    store = TRE_TEST_STORE
    domain_id_1 = nothing
    domain_id_2 = nothing
    vocab_id_1 = nothing
    vocab_id_2 = nothing
    
    try
        # Setup: Create two test domains
        suffix = unique_test_suffix()
        domain1 = AHRI_TRE.add_domain!(store, AHRI_TRE.Domain(name="vocab_test_domain1_$suffix"))
        domain2 = AHRI_TRE.add_domain!(store, AHRI_TRE.Domain(name="vocab_test_domain2_$suffix"))
        @test domain1.domain_id !== nothing
        @test domain2.domain_id !== nothing
        domain_id_1 = Int(domain1.domain_id)
        domain_id_2 = Int(domain2.domain_id)

        @testset "Create and retrieve vocabulary by (domain_id, name)" begin
            vocab_name = "status_codes_$suffix"
            items = AHRI_TRE.VocabularyItem[
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=1, code="ACTIVE", description="Active"),
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=2, code="INACTIVE", description="Inactive"),
            ]

            # Create vocabulary in domain 1
            vocab_id_1 = AHRI_TRE.ensure_vocabulary!(store, domain_id_1, vocab_name, "Status codes for domain 1", items)
            @test vocab_id_1 > 0

            # Retrieve by domain_id and name
            fetched = AHRI_TRE.get_vocabulary(store, domain_id_1, vocab_name)
            @test fetched !== nothing
            @test fetched.vocabulary_id == vocab_id_1
            @test fetched.domain_id == domain_id_1
            @test fetched.name == vocab_name
            @test length(fetched.items) == 2

            # Vocabulary should not exist in domain 2 with same name
            not_found = AHRI_TRE.get_vocabulary(store, domain_id_2, vocab_name)
            @test isnothing(not_found)
        end

        @testset "Same vocabulary name in different domains" begin
            vocab_name = "shared_name_$suffix"
            items_d1 = AHRI_TRE.VocabularyItem[
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=1, code="D1_A", description="Domain 1 A"),
            ]
            items_d2 = AHRI_TRE.VocabularyItem[
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=1, code="D2_A", description="Domain 2 A"),
            ]

            # Create vocabulary with same name in both domains
            v1_id = AHRI_TRE.ensure_vocabulary!(store, domain_id_1, vocab_name, "D1 vocab", items_d1)
            v2_id = AHRI_TRE.ensure_vocabulary!(store, domain_id_2, vocab_name, "D2 vocab", items_d2)

            # IDs should be different
            @test v1_id != v2_id

            # Each should be retrievable with correct domain context
            v1_fetched = AHRI_TRE.get_vocabulary(store, domain_id_1, vocab_name)
            v2_fetched = AHRI_TRE.get_vocabulary(store, domain_id_2, vocab_name)

            @test v1_fetched.vocabulary_id == v1_id
            @test v1_fetched.domain_id == domain_id_1
            @test v1_fetched.items[1].code == "D1_A"

            @test v2_fetched.vocabulary_id == v2_id
            @test v2_fetched.domain_id == domain_id_2
            @test v2_fetched.items[1].code == "D2_A"

            # Trying to get by name alone should fail with error about non-unique name
            @test_throws ErrorException AHRI_TRE.get_vocabulary(store, vocab_name)
        end

        @testset "Get vocabulary by name alone (unique case)" begin
            vocab_name = "unique_name_$suffix"
            items = AHRI_TRE.VocabularyItem[
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=1, code="X", description="X value"),
            ]

            # Create in domain 1 only
            v_id = AHRI_TRE.ensure_vocabulary!(store, domain_id_1, vocab_name, "Unique vocabulary", items)

            # Should be retrievable by name alone since it's unique
            v_fetched = AHRI_TRE.get_vocabulary(store, vocab_name)
            @test v_fetched !== nothing
            @test v_fetched.vocabulary_id == v_id
            @test v_fetched.domain_id == domain_id_1
            @test v_fetched.name == vocab_name
        end

        @testset "Update vocabulary items" begin
            vocab_name = "mutable_vocab_$suffix"
            items_v1 = AHRI_TRE.VocabularyItem[
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=1, code="A", description="A"),
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=2, code="B", description="B"),
            ]

            vocab_id = AHRI_TRE.ensure_vocabulary!(store, domain_id_1, vocab_name, "v1", items_v1)
            fetched_v1 = AHRI_TRE.get_vocabulary(store, domain_id_1, vocab_name)
            @test length(fetched_v1.items) == 2

            # Update with new items
            items_v2 = AHRI_TRE.VocabularyItem[
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=10, code="X", description="X"),
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=20, code="Y", description="Y"),
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=30, code="Z", description="Z"),
            ]

            vocab_id_2 = AHRI_TRE.ensure_vocabulary!(store, domain_id_1, vocab_name, "v2", items_v2)
            @test vocab_id_2 == vocab_id  # Same vocabulary ID

            fetched_v2 = AHRI_TRE.get_vocabulary(store, domain_id_1, vocab_name)
            @test fetched_v2.description == "v2"
            @test length(fetched_v2.items) == 3
            @test Set([it.code for it in fetched_v2.items]) == Set(["X", "Y", "Z"])
        end

        @testset "Retrieve vocabulary by ID" begin
            vocab_name = "by_id_vocab_$suffix"
            items = AHRI_TRE.VocabularyItem[
                AHRI_TRE.VocabularyItem(vocabulary_id=0, value=1, code="ID", description="ID test"),
            ]

            vocab_id = AHRI_TRE.ensure_vocabulary!(store, domain_id_1, vocab_name, "For ID lookup", items)

            # Retrieve by vocabulary_id alone
            fetched = AHRI_TRE.get_vocabulary(store, vocab_id)
            @test fetched !== nothing
            @test fetched.vocabulary_id == vocab_id
            @test fetched.domain_id == domain_id_1
            @test fetched.name == vocab_name
        end

        @testset "Nonexistent vocabulary returns nothing" begin
            nonexistent = AHRI_TRE.get_vocabulary(store, domain_id_1, "this_vocab_does_not_exist_anywhere")
            @test isnothing(nonexistent)

            nonexistent_by_name = AHRI_TRE.get_vocabulary(store, "also_does_not_exist_$suffix")
            @test isnothing(nonexistent_by_name)
        end

        @testset "Get all vocabularies in a domain" begin
            # Create multiple vocabularies in domain 1
            vocab_names = ["list_test_a_$suffix", "list_test_b_$suffix", "list_test_c_$suffix"]
            created_vocab_ids = Int[]
            
            for (idx, name) in enumerate(vocab_names)
                items = AHRI_TRE.VocabularyItem[
                    AHRI_TRE.VocabularyItem(vocabulary_id=0, value=idx, code="CODE_$idx", description="Item $idx"),
                ]
                vocab_id = AHRI_TRE.ensure_vocabulary!(store, domain_id_1, name, "Test vocab $idx", items)
                push!(created_vocab_ids, vocab_id)
            end

            # Get all vocabularies in domain 1 using domain_id
            vocabs_d1 = AHRI_TRE.get_vocabularies(store, domain_id_1)
            @test !isempty(vocabs_d1)
            
            # Check that our created vocabularies are present
            vocab_names_fetched = [v.name for v in vocabs_d1]
            for name in vocab_names
                @test name in vocab_names_fetched
            end
            
            # Verify each vocabulary has its items populated
            for vocab in vocabs_d1
                if vocab.name in vocab_names
                    @test length(vocab.items) >= 1
                    @test !isnothing(vocab.vocabulary_id)
                    @test vocab.domain_id == domain_id_1
                end
            end

            # Get vocabularies using Domain struct
            domain1_obj = AHRI_TRE.Domain(domain_id=domain_id_1, name="test_domain_1")
            vocabs_from_struct = AHRI_TRE.get_vocabularies(store, domain1_obj)
            @test length(vocabs_from_struct) == length(vocabs_d1)
            
            # Domain 2 should have different (fewer) vocabularies
            vocabs_d2 = AHRI_TRE.get_vocabularies(store, domain_id_2)
            vocab_names_d2 = [v.name for v in vocabs_d2]
            
            # Our test vocabularies should not be in domain 2
            for name in vocab_names
                @test !(name in vocab_names_d2)
            end

            # Cleanup the extra vocabularies we created
            for vocab_id in created_vocab_ids
                try
                    DBInterface.execute(store.store, 
                        raw"DELETE FROM vocabulary_items WHERE vocabulary_id = $1;", (vocab_id,))
                    DBInterface.execute(store.store, 
                        raw"DELETE FROM vocabularies WHERE vocabulary_id = $1;", (vocab_id,))
                catch e
                    @warn "Failed to cleanup test vocabulary" vocab_id exception=e
                end
            end
        end

    finally
        # Cleanup
        try
            # Delete ALL vocabularies in both domains before deleting domains
            if !isnothing(domain_id_1)
                DBInterface.execute(store.store, 
                    raw"DELETE FROM vocabulary_items WHERE vocabulary_id IN (SELECT vocabulary_id FROM vocabularies WHERE domain_id = $1);", (domain_id_1,))
                DBInterface.execute(store.store, 
                    raw"DELETE FROM vocabularies WHERE domain_id = $1;", (domain_id_1,))
            end
            if !isnothing(domain_id_2)
                DBInterface.execute(store.store, 
                    raw"DELETE FROM vocabulary_items WHERE vocabulary_id IN (SELECT vocabulary_id FROM vocabularies WHERE domain_id = $1);", (domain_id_2,))
                DBInterface.execute(store.store, 
                    raw"DELETE FROM vocabularies WHERE domain_id = $1;", (domain_id_2,))
            end

            # Delete domains after all vocabularies are removed
            if !isnothing(domain_id_1)
                DBInterface.execute(store.store, 
                    raw"DELETE FROM domains WHERE domain_id = $1;", (domain_id_1,))
            end
            if !isnothing(domain_id_2)
                DBInterface.execute(store.store, 
                    raw"DELETE FROM domains WHERE domain_id = $1;", (domain_id_2,))
            end
        catch e
            @warn "Vocabulary CRUD test cleanup failed" exception=(e, catch_backtrace())
        end
    end
end

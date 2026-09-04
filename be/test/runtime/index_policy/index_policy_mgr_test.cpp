// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/index_policy/index_policy_mgr.h"

#include <gtest/gtest.h>

#include <exception>
#include <future>
#include <latch>
#include <thread>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "storage/index/inverted/analysis_factory_mgr.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/analyzer/segment_analyzer_context.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/inverted/common_grams/common_word_set.h"
#include "util/defer_op.h"

namespace doris {
namespace {

TIndexPolicy common_grams_policy(int64_t id, std::string name) {
    TIndexPolicy policy;
    policy.id = id;
    policy.name = std::move(name);
    policy.type = TIndexPolicyType::TOKEN_FILTER;
    policy.properties["type"] = "common_grams";
    return policy;
}

} // namespace

class IndexPolicyMgrTest : public testing::Test {
protected:
    void SetUp() override {
        // Create some test policies
        TIndexPolicy tokenizer1;
        tokenizer1.id = 1;
        tokenizer1.name = "tokenizer1";
        tokenizer1.type = TIndexPolicyType::TOKENIZER;
        tokenizer1.properties["type"] = "standard";
        tokenizer1.properties["max_token_length"] = "255";

        TIndexPolicy tokenizer2;
        tokenizer2.id = 2;
        tokenizer2.name = "tokenizer2";
        tokenizer2.type = TIndexPolicyType::TOKENIZER;
        tokenizer2.properties["type"] = "ngram";
        tokenizer2.properties["min_gram"] = "2";
        tokenizer2.properties["max_gram"] = "3";

        TIndexPolicy filter1;
        filter1.id = 3;
        filter1.name = "filter1";
        filter1.type = TIndexPolicyType::TOKEN_FILTER;
        filter1.properties["type"] = "lowercase";

        TIndexPolicy filter2;
        filter2.id = 4;
        filter2.name = "filter2";
        filter2.type = TIndexPolicyType::TOKEN_FILTER;
        filter2.properties["type"] = "asciifolding";

        TIndexPolicy analyzer1;
        analyzer1.id = 5;
        analyzer1.name = "analyzer1";
        analyzer1.type = TIndexPolicyType::ANALYZER;
        analyzer1.properties["tokenizer"] = "tokenizer1";
        analyzer1.properties["token_filter"] = "filter1,filter2";

        // Initialize the manager with some policies
        std::vector<TIndexPolicy> initial_policies = {tokenizer1, tokenizer2, filter1, filter2,
                                                      analyzer1};
        mgr.apply_policy_changes(initial_policies, {});
    }

    IndexPolicyMgr mgr;
};

TEST_F(IndexPolicyMgrTest, TestApplyPolicyChanges) {
    // Test initial state
    auto policies = mgr.get_index_policys();
    ASSERT_EQ(policies.size(), 5);

    // Test adding new policies
    TIndexPolicy newTokenizer;
    newTokenizer.id = 6;
    newTokenizer.name = "new_tokenizer";
    newTokenizer.type = TIndexPolicyType::TOKENIZER;
    newTokenizer.properties["type"] = "whitespace";

    TIndexPolicy newAnalyzer;
    newAnalyzer.id = 7;
    newAnalyzer.name = "new_analyzer";
    newAnalyzer.type = TIndexPolicyType::ANALYZER;
    newAnalyzer.properties["tokenizer"] = "new_tokenizer";

    mgr.apply_policy_changes({newTokenizer, newAnalyzer}, {});
    policies = mgr.get_index_policys();
    ASSERT_EQ(policies.size(), 7);

    // Test deleting policies
    mgr.apply_policy_changes({}, {1, 3}); // Delete tokenizer1 and filter1
    policies = mgr.get_index_policys();
    ASSERT_EQ(policies.size(), 5);
    ASSERT_FALSE(policies.contains(1));
    ASSERT_FALSE(policies.contains(3));

    // Test duplicate ID
    TIndexPolicy duplicateId;
    duplicateId.id = 2; // Same as tokenizer2
    duplicateId.name = "duplicate_id";
    mgr.apply_policy_changes({duplicateId}, {});
    policies = mgr.get_index_policys();
    if (policies.contains(duplicateId.id)) {
        ASSERT_NE(policies[duplicateId.id].name, "duplicate_id");
    }

    // Test duplicate name
    TIndexPolicy duplicateName;
    duplicateName.id = 8;
    duplicateName.name = "tokenizer2"; // Same as tokenizer2
    mgr.apply_policy_changes({duplicateName}, {});
    policies = mgr.get_index_policys();
    ASSERT_FALSE(policies.contains(duplicateName.id));
}

TEST_F(IndexPolicyMgrTest, TestGetPolicyByName) {
    // Test getting existing policy
    auto analyzer = mgr.get_policy_by_name("analyzer1");
    ASSERT_NE(analyzer, nullptr);

    // Test getting non-existent policy
    EXPECT_THROW(mgr.get_policy_by_name("nonexistent"), Exception);

    // Test policy with invalid tokenizer config
    TIndexPolicy invalidAnalyzer;
    invalidAnalyzer.id = 8;
    invalidAnalyzer.name = "invalid_analyzer";
    invalidAnalyzer.type = TIndexPolicyType::ANALYZER;
    // Missing tokenizer property
    mgr.apply_policy_changes({invalidAnalyzer}, {});
    EXPECT_THROW(mgr.get_policy_by_name("invalid_analyzer"), Exception);

    // Test policy with non-existent tokenizer reference
    TIndexPolicy badRefAnalyzer;
    badRefAnalyzer.id = 9;
    badRefAnalyzer.name = "bad_ref_analyzer";
    badRefAnalyzer.type = TIndexPolicyType::ANALYZER;
    badRefAnalyzer.properties["tokenizer"] = "nonexistent_tokenizer";
    mgr.apply_policy_changes({badRefAnalyzer}, {});
    EXPECT_THROW(mgr.get_policy_by_name("bad_ref_analyzer"), Exception);
}

TEST_F(IndexPolicyMgrTest, TestTokenFilterProcessing) {
    // Test analyzer with multiple token filters
    auto analyzer = mgr.get_policy_by_name("analyzer1");
    ASSERT_NE(analyzer, nullptr);

    // Test analyzer with simple token filter (not a policy reference)
    TIndexPolicy simpleFilterAnalyzer;
    simpleFilterAnalyzer.id = 10;
    simpleFilterAnalyzer.name = "simple_filter_analyzer";
    simpleFilterAnalyzer.type = TIndexPolicyType::ANALYZER;
    simpleFilterAnalyzer.properties["tokenizer"] = "tokenizer2";
    simpleFilterAnalyzer.properties["token_filter"] = "lowercase";
    mgr.apply_policy_changes({simpleFilterAnalyzer}, {});

    auto simpleAnalyzer = mgr.get_policy_by_name("simple_filter_analyzer");
    ASSERT_NE(simpleAnalyzer, nullptr);

    // Test empty token filter list
    TIndexPolicy emptyFilterAnalyzer;
    emptyFilterAnalyzer.id = 11;
    emptyFilterAnalyzer.name = "empty_filter_analyzer";
    emptyFilterAnalyzer.type = TIndexPolicyType::ANALYZER;
    emptyFilterAnalyzer.properties["tokenizer"] = "tokenizer1";
    emptyFilterAnalyzer.properties["token_filter"] = "   ";
    mgr.apply_policy_changes({emptyFilterAnalyzer}, {});

    auto emptyAnalyzer = mgr.get_policy_by_name("empty_filter_analyzer");
    ASSERT_NE(emptyAnalyzer, nullptr);
}

TEST_F(IndexPolicyMgrTest, CommonGramsProviderRetainsImmutablePurposeConfiguration) {
    TIndexPolicy tokenizer;
    tokenizer.id = 20;
    tokenizer.name = "cg_tokenizer";
    tokenizer.type = TIndexPolicyType::TOKENIZER;
    tokenizer.properties["type"] = "char_group";
    tokenizer.properties["tokenize_on_chars"] = "[whitespace]";

    auto common_grams = common_grams_policy(21, "cg_filter");

    TIndexPolicy analyzer_policy;
    analyzer_policy.id = 22;
    analyzer_policy.name = "cg_analyzer";
    analyzer_policy.type = TIndexPolicyType::ANALYZER;
    analyzer_policy.properties["tokenizer"] = "cg_tokenizer";
    analyzer_policy.properties["token_filter"] = "lowercase,cg_filter";
    mgr.apply_policy_changes({tokenizer, common_grams, analyzer_policy}, {});

    auto provider = mgr.get_analyzer_provider_by_name("cg_analyzer");
    ASSERT_NE(provider, nullptr);
    auto analyze = [](const segment_v2::inverted_index::AnalyzerProviderPtr& analyzer_provider,
                      segment_v2::inverted_index::AnalysisPurpose purpose, std::string_view text) {
        auto analyzer = analyzer_provider->get_analyzer(purpose);
        auto reader = std::make_shared<lucene::util::SStringReader<char>>();
        reader->init(text.data(), static_cast<int32_t>(text.size()), true);
        return segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                reader, analyzer.get());
    };

    auto index = analyze(provider, segment_v2::inverted_index::AnalysisPurpose::kIndex,
                         "Man of the Year");
    auto plain = analyze(provider, segment_v2::inverted_index::AnalysisPurpose::kPlainQuery,
                         "Man of the Year");
    auto exact = analyze(provider, segment_v2::inverted_index::AnalysisPurpose::kExactPhraseQuery,
                         "Man of the Year");
    auto prefix = analyze(provider, segment_v2::inverted_index::AnalysisPurpose::kPhrasePrefixQuery,
                          "the wo");
    EXPECT_EQ(index.size(), 7);
    EXPECT_EQ(plain.size(), 4);
    EXPECT_EQ(exact.size(), 3);
    EXPECT_EQ(prefix.size(), 1);
    auto prefix_gram = segment_v2::inverted_index::encode_common_gram("the", "wo");
    ASSERT_TRUE(prefix_gram.has_value()) << prefix_gram.error();
    EXPECT_EQ(prefix.front().get_single_term(), prefix_gram.value());

    auto single_index = mgr.get_analyzer_by_name(
            "cg_analyzer", segment_v2::inverted_index::AnalysisPurpose::kIndex);
    auto single_reader = std::make_shared<lucene::util::SStringReader<char>>();
    const std::string single_input = "Man of the Year";
    single_reader->init(single_input.data(), static_cast<int32_t>(single_input.size()), true);
    EXPECT_EQ(segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                      single_reader, single_index.get())
                      .size(),
              7);
}

TEST_F(IndexPolicyMgrTest, AnalyzerProviderPreservesPurposeInsensitiveNormalizers) {
    auto builtin = mgr.get_analyzer_provider_by_name("lowercase");
    auto builtin_analyzer =
            builtin->get_analyzer(segment_v2::inverted_index::AnalysisPurpose::kPlainQuery);
    EXPECT_EQ(builtin->get_analyzer(segment_v2::inverted_index::AnalysisPurpose::kIndex),
              builtin_analyzer);

    TIndexPolicy normalizer;
    normalizer.id = 23;
    normalizer.name = "test_normalizer";
    normalizer.type = TIndexPolicyType::NORMALIZER;
    normalizer.properties["token_filter"] = "lowercase";
    mgr.apply_policy_changes({normalizer}, {});

    auto configured = mgr.get_analyzer_provider_by_name("test_normalizer");
    auto configured_analyzer =
            configured->get_analyzer(segment_v2::inverted_index::AnalysisPurpose::kPlainQuery);
    EXPECT_EQ(configured->get_analyzer(segment_v2::inverted_index::AnalysisPurpose::kIndex),
              configured_analyzer);
}

TEST_F(IndexPolicyMgrTest, FindsFreshAnalyzerProviderBySegmentBaseFingerprint) {
    using segment_v2::inverted_index::AnalysisPurpose;
    using segment_v2::inverted_index::InvertedIndexAnalyzer;

    const std::map<std::string, std::string> slash_to_space = {
            {INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE},
            {INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "/"},
            {INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " "}};
    auto configured = mgr.get_analyzer_provider_by_name("analyzer1", slash_to_space);
    const std::string segment_fingerprint(configured->base_analyzer_fingerprint());

    auto first = mgr.get_analyzer_provider_by_base_fingerprint(segment_fingerprint, slash_to_space);
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->base_analyzer_fingerprint(), segment_fingerprint);
    EXPECT_EQ(mgr.get_analyzer_provider_by_base_fingerprint(segment_fingerprint), nullptr);
    EXPECT_EQ(mgr.get_analyzer_provider_by_base_fingerprint("unknown", slash_to_space), nullptr);

    auto second =
            mgr.get_analyzer_provider_by_base_fingerprint(segment_fingerprint, slash_to_space);
    ASSERT_NE(second, nullptr);
    EXPECT_NE(first, second);
    EXPECT_NE(first->get_analyzer(AnalysisPurpose::kPlainQuery),
              second->get_analyzer(AnalysisPurpose::kPlainQuery));

    mgr.apply_policy_changes({}, {5});
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    const std::string input = "ASCII TERM";
    reader->init(input.data(), static_cast<int32_t>(input.size()), true);
    const auto terms = InvertedIndexAnalyzer::get_analyse_result(
            reader, first->get_analyzer(AnalysisPurpose::kPlainQuery).get());
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].get_single_term(), "ascii");
    EXPECT_EQ(terms[1].get_single_term(), "term");
}

TEST_F(IndexPolicyMgrTest, RebuildsQueryContextForPersistedSegmentAnalyzer) {
    const std::map<std::string, std::string> slash_to_space = {
            {INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE},
            {INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "/"},
            {INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " "}};
    auto segment_provider = mgr.get_analyzer_provider_by_name("analyzer1", slash_to_space);
    const std::string segment_fingerprint(segment_provider->base_analyzer_fingerprint());

    segment_v2::inverted_index::Settings tokenizer_settings;
    tokenizer_settings.set("tokenize_on_chars", "[whitespace]");
    segment_v2::inverted_index::CustomAnalyzerConfig::Builder request_builder;
    request_builder.with_tokenizer_config("char_group", tokenizer_settings);
    auto request_provider = std::make_shared<segment_v2::inverted_index::CustomAnalyzerProvider>(
            request_builder.build());
    ASSERT_NE(request_provider->base_analyzer_fingerprint(), segment_fingerprint);

    InvertedIndexAnalyzerCtx request_context;
    request_context.analyzer_name = "request_analyzer";
    request_context.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    request_context.char_filter_map = {{"stale", "filter"}};
    request_context.analyzer = request_provider->get_analyzer(
            segment_v2::inverted_index::AnalysisPurpose::kPlainQuery);
    request_context.analyzer_provider = request_provider;
    request_context.common_grams_identity = segment_v2::inverted_index::CommonGramsQueryIdentity {
            .common_grams_dictionary_identity = "stale-dictionary",
            .base_analyzer_fingerprint = std::string(request_provider->base_analyzer_fingerprint()),
            .common_grams_fingerprint = "stale-common-grams"};

    const std::map<std::string, std::string> physical_properties = slash_to_space;
    auto rebuilt = segment_v2::inverted_index::maybe_rebuild_segment_analyzer_context(
            &request_context, segment_fingerprint, physical_properties, &mgr);
    ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error();
    ASSERT_TRUE(rebuilt->has_value());
    const auto& effective = rebuilt->value();
    EXPECT_EQ(effective.analyzer_name, request_context.analyzer_name);
    EXPECT_EQ(effective.parser_type, request_context.parser_type);
    EXPECT_EQ(effective.char_filter_map, slash_to_space);
    EXPECT_EQ(effective.analyzer, nullptr);
    ASSERT_NE(effective.analyzer_provider, nullptr);
    EXPECT_EQ(effective.analyzer_provider->base_analyzer_fingerprint(), segment_fingerprint);
    EXPECT_FALSE(effective.common_grams_identity.has_value());
    EXPECT_NE(effective.analyzer_provider, segment_provider);

    InvertedIndexAnalyzerCtx matching_context = request_context;
    matching_context.analyzer_provider = segment_provider;
    auto unchanged = segment_v2::inverted_index::maybe_rebuild_segment_analyzer_context(
            &matching_context, segment_fingerprint, physical_properties, &mgr);
    ASSERT_TRUE(unchanged.has_value()) << unchanged.error();
    EXPECT_FALSE(unchanged->has_value());

    auto unavailable = segment_v2::inverted_index::maybe_rebuild_segment_analyzer_context(
            &request_context, "missing-segment-fingerprint", physical_properties, &mgr);
    ASSERT_FALSE(unavailable.has_value());
    EXPECT_EQ(unavailable.error().code(), ErrorCode::INVERTED_INDEX_BYPASS);
}

TEST_F(IndexPolicyMgrTest, SegmentAnalyzerAdmissionKeepsLegacyRequestWithoutMetadata) {
    auto admitted = segment_v2::inverted_index::maybe_rebuild_segment_analyzer_context(
            nullptr, std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata> {}, {},
            nullptr);

    ASSERT_TRUE(admitted.has_value()) << admitted.error();
    EXPECT_FALSE(admitted->has_value());
}

TEST_F(IndexPolicyMgrTest, SegmentAnalyzerAdmissionBypassesTypedMetadataWithoutBaseFingerprint) {
    segment_v2::inverted_index::CommonGramsSegmentMetadata metadata;
    auto admitted = segment_v2::inverted_index::maybe_rebuild_segment_analyzer_context(
            nullptr, std::optional {metadata}, {}, &mgr);

    ASSERT_FALSE(admitted.has_value());
    EXPECT_EQ(admitted.error().code(), ErrorCode::INVERTED_INDEX_BYPASS);
}

TEST_F(IndexPolicyMgrTest, SegmentAnalyzerAdmissionRebuildsTypedMetadata) {
    const std::map<std::string, std::string> slash_to_space = {
            {INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE},
            {INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "/"},
            {INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " "}};
    auto segment_provider = mgr.get_analyzer_provider_by_name("analyzer1", slash_to_space);
    const std::string segment_fingerprint(segment_provider->base_analyzer_fingerprint());

    segment_v2::inverted_index::Settings tokenizer_settings;
    tokenizer_settings.set("tokenize_on_chars", "[whitespace]");
    segment_v2::inverted_index::CustomAnalyzerConfig::Builder request_builder;
    request_builder.with_tokenizer_config("char_group", tokenizer_settings);
    auto request_provider = std::make_shared<segment_v2::inverted_index::CustomAnalyzerProvider>(
            request_builder.build());

    InvertedIndexAnalyzerCtx request_context;
    request_context.analyzer_provider = request_provider;
    request_context.analyzer = request_provider->get_analyzer(
            segment_v2::inverted_index::AnalysisPurpose::kPlainQuery);
    segment_v2::inverted_index::CommonGramsSegmentMetadata metadata;
    metadata.base_analyzer_fingerprint = segment_fingerprint;

    auto rebuilt = segment_v2::inverted_index::maybe_rebuild_segment_analyzer_context(
            &request_context, std::optional {metadata}, slash_to_space, &mgr);

    ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error();
    ASSERT_TRUE(rebuilt->has_value());
    EXPECT_EQ(rebuilt->value().analyzer_provider->base_analyzer_fingerprint(), segment_fingerprint);
}

} // namespace doris
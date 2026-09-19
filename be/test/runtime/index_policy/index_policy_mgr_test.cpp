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
#include "storage/index/inverted/inverted_index_parser.h"
#include "util/defer_op.h"

namespace doris {
namespace {

TIndexPolicy make_analyzer_policy(int64_t id, std::string name, std::string tokenizer) {
    TIndexPolicy policy;
    policy.id = id;
    policy.name = std::move(name);
    policy.type = TIndexPolicyType::ANALYZER;
    policy.properties["tokenizer"] = std::move(tokenizer);
    return policy;
}

TIndexPolicy make_tokenizer_policy(int64_t id, std::string name, std::string type) {
    TIndexPolicy policy;
    policy.id = id;
    policy.name = std::move(name);
    policy.type = TIndexPolicyType::TOKENIZER;
    policy.properties["type"] = std::move(type);
    return policy;
}

size_t count_analyzer_terms(IndexPolicyMgr& manager, const std::string& name) {
    auto analyzer = manager.get_policy_by_name(name);
    auto reader = segment_v2::inverted_index::InvertedIndexAnalyzer::create_reader({});
    const std::string text = "one two";
    reader->init(text.data(), static_cast<int32_t>(text.size()), false);
    return segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(reader,
                                                                                 analyzer.get())
            .size();
}

void assert_collision_sequence(const std::vector<TIndexPolicy>& updates, int64_t older_id,
                               int64_t newer_id) {
    IndexPolicyMgr manager;
    manager.apply_policy_changes(updates, {});
    EXPECT_EQ(count_analyzer_terms(manager, "Colliding_Analyzer"), 2);
    EXPECT_EQ(manager.get_index_policys().size(), 2);

    manager.apply_policy_changes({}, {older_id});
    EXPECT_EQ(count_analyzer_terms(manager, "Colliding_Analyzer"), 2);
    manager.apply_policy_changes({}, {newer_id});
    EXPECT_THROW(manager.get_policy_by_name("colliding_analyzer"), Exception);
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

    // Legacy duplicate names are retained, with the higher ID authoritative.
    TIndexPolicy duplicateName;
    duplicateName.id = 8;
    duplicateName.name = "tokenizer2"; // Same as tokenizer2
    mgr.apply_policy_changes({duplicateName}, {});
    policies = mgr.get_index_policys();
    ASSERT_TRUE(policies.contains(duplicateName.id));
}

TEST_F(IndexPolicyMgrTest, NormalizedNameCollisionUsesHigherIdIndependentOfArrivalOrder) {
    TIndexPolicy older = make_analyzer_policy(100, "COLLIDING_ANALYZER", "keyword");
    TIndexPolicy newer = make_analyzer_policy(101, "colliding_analyzer", "standard");

    assert_collision_sequence({older, newer}, older.id, newer.id);
    assert_collision_sequence({newer, older}, older.id, newer.id);

    IndexPolicyMgr manager;
    manager.apply_policy_changes({older, newer}, {});
    manager.apply_policy_changes({}, {newer.id});
    EXPECT_EQ(count_analyzer_terms(manager, "COLLIDING_ANALYZER"), 1);
}

TEST_F(IndexPolicyMgrTest, LegacyExactNameCollisionPreservesDependentAnalyzerTerms) {
    TIndexPolicy historical = make_tokenizer_policy(100, "IK_SMART", "standard");
    TIndexPolicy newer = make_tokenizer_policy(101, "ik_smart", "keyword");
    TIndexPolicy upper_dependent = make_analyzer_policy(102, "legacy_exact_analyzer", "IK_SMART");
    TIndexPolicy lower_dependent =
            make_analyzer_policy(103, "normalized_exact_analyzer", "ik_smart");

    for (const std::vector<TIndexPolicy>& updates :
         {std::vector<TIndexPolicy> {historical, newer, upper_dependent, lower_dependent},
          std::vector<TIndexPolicy> {lower_dependent, upper_dependent, newer, historical}}) {
        IndexPolicyMgr manager;
        manager.apply_policy_changes(updates, {});

        EXPECT_EQ(count_analyzer_terms(manager, upper_dependent.name), 2);
        EXPECT_EQ(count_analyzer_terms(manager, lower_dependent.name), 1);
    }
}

TEST_F(IndexPolicyMgrTest, MatchDispatchPreservesReplayedExactIkTerms) {
    IndexPolicyMgr manager;
    const auto historical = make_analyzer_policy(110, "IK", "keyword");
    manager.apply_policy_changes({historical}, {});

    const auto config = AnalyzerConfigParser::parse("IK", "english");
    ASSERT_TRUE(config.uses_provider());
    EXPECT_EQ(config.provider_name, historical.name);
    EXPECT_EQ(config.analyzer_key, build_analyzer_key_from_properties({{"analyzer", "IK"}}));
    auto analyzer = manager.get_analyzer_provider_by_name(config.provider_name, {})->get_analyzer();
    auto reader = segment_v2::inverted_index::InvertedIndexAnalyzer::create_reader({});
    const std::string text = "abc def";
    reader->init(text.data(), static_cast<int32_t>(text.size()), false);
    const auto terms = segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(
            reader, analyzer.get());
    ASSERT_EQ(terms.size(), 1);
    EXPECT_EQ(terms.front().get_single_term(), text);
    EXPECT_EQ(count_analyzer_terms(manager, historical.name), 1);
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

TEST_F(IndexPolicyMgrTest, BuiltinTokenizerNamesAreCaseInsensitive) {
    TIndexPolicy analyzer;
    analyzer.id = 20;
    analyzer.name = "uppercase_ik_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties["tokenizer"] = "IK_SMART";
    mgr.apply_policy_changes({analyzer}, {});

    auto built = mgr.get_policy_by_name(analyzer.name);
    ASSERT_NE(built, nullptr);
}

TEST_F(IndexPolicyMgrTest, ExistingPolicyTakesPrecedenceOverNewBuiltinName) {
    TIndexPolicy legacy_tokenizer;
    legacy_tokenizer.id = 21;
    legacy_tokenizer.name = "ik_smart";
    legacy_tokenizer.type = TIndexPolicyType::TOKENIZER;
    legacy_tokenizer.properties["type"] = "ngram";
    legacy_tokenizer.properties["min_gram"] = "2";
    legacy_tokenizer.properties["max_gram"] = "2";

    TIndexPolicy analyzer;
    analyzer.id = 22;
    analyzer.name = "legacy_collision_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties["tokenizer"] = "IK_SMART";
    mgr.apply_policy_changes({legacy_tokenizer, analyzer}, {});

    auto built = mgr.get_policy_by_name(analyzer.name);
    ASSERT_NE(built, nullptr);
    auto reader = segment_v2::inverted_index::InvertedIndexAnalyzer::create_reader({});
    const std::string text = "abcd";
    reader->init(text.data(), static_cast<int32_t>(text.size()), false);
    auto terms = segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(reader,
                                                                                       built.get());
    ASSERT_EQ(terms.size(), 3);
    EXPECT_EQ(terms[0].get_single_term(), "ab");
    EXPECT_EQ(terms[1].get_single_term(), "bc");
    EXPECT_EQ(terms[2].get_single_term(), "cd");
}

TEST_F(IndexPolicyMgrTest, AnalyzerProviderPreservesPurposeInsensitiveNormalizers) {
    auto builtin = mgr.get_analyzer_provider_by_name("lowercase");
    auto builtin_analyzer = builtin->get_analyzer();
    EXPECT_EQ(builtin->get_analyzer(), builtin_analyzer);

    TIndexPolicy normalizer;
    normalizer.id = 23;
    normalizer.name = "test_normalizer";
    normalizer.type = TIndexPolicyType::NORMALIZER;
    normalizer.properties["token_filter"] = "lowercase";
    mgr.apply_policy_changes({normalizer}, {});

    auto configured = mgr.get_analyzer_provider_by_name("test_normalizer");
    auto configured_analyzer = configured->get_analyzer();
    EXPECT_EQ(configured->get_analyzer(), configured_analyzer);
}

} // namespace doris

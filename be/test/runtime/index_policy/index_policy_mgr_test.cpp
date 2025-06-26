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

#include "olap/rowset/segment_v2/inverted_index/analysis_factory_mgr.h"
#include "runtime/exec_env.h"

namespace doris {

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

} // namespace doris
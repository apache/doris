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

#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"

#include <gtest/gtest.h>

#include "gen_cpp/AgentService_types.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/inverted_index/util/reader.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"

namespace doris::segment_v2::inverted_index {

class AnalyzerTest : public ::testing::Test {
protected:
    void SetUp() override { _index_policy_mgr = std::make_unique<IndexPolicyMgr>(); }

    void TearDown() override { _index_policy_mgr.reset(); }

    void SetupCustomAnalyzerPolicies() {
        TIndexPolicy tokenizer;
        tokenizer.id = 1;
        tokenizer.name = "test_tokenizer";
        tokenizer.type = TIndexPolicyType::TOKENIZER;
        tokenizer.properties["type"] = "standard";

        TIndexPolicy filter;
        filter.id = 2;
        filter.name = "test_filter";
        filter.type = TIndexPolicyType::TOKEN_FILTER;
        filter.properties["type"] = "lowercase";

        TIndexPolicy analyzer;
        analyzer.id = 3;
        analyzer.name = "test_custom_analyzer";
        analyzer.type = TIndexPolicyType::ANALYZER;
        analyzer.properties["tokenizer"] = "test_tokenizer";
        analyzer.properties["token_filter"] = "test_filter";

        std::vector<TIndexPolicy> policies = {tokenizer, filter, analyzer};
        _index_policy_mgr->apply_policy_changes(policies, {});
    }

    std::unique_ptr<IndexPolicyMgr> _index_policy_mgr;
};

// ==================== Combined test for is_builtin_analyzer and create_builtin_analyzer ====================

TEST_F(AnalyzerTest, TestBuiltinAnalyzers) {
    // Test all builtin analyzer names with is_builtin_analyzer and create_builtin_analyzer together
    struct BuiltinAnalyzerTestCase {
        std::string name;
        InvertedIndexParserType parser_type;
        std::string parser_mode;
        bool requires_dict; // Flag to indicate if dictionary files are required
    };

    std::vector<BuiltinAnalyzerTestCase> builtin_cases = {
            {INVERTED_INDEX_PARSER_NONE, InvertedIndexParserType::PARSER_NONE, "", false},
            {INVERTED_INDEX_PARSER_STANDARD, InvertedIndexParserType::PARSER_STANDARD, "", false},
            {INVERTED_INDEX_PARSER_UNICODE, InvertedIndexParserType::PARSER_UNICODE, "", false},
            {INVERTED_INDEX_PARSER_ENGLISH, InvertedIndexParserType::PARSER_ENGLISH, "", false},
            {INVERTED_INDEX_PARSER_CHINESE, InvertedIndexParserType::PARSER_CHINESE,
             INVERTED_INDEX_PARSER_COARSE_GRANULARITY, true},
            {INVERTED_INDEX_PARSER_ICU, InvertedIndexParserType::PARSER_ICU, "", true},
            {INVERTED_INDEX_PARSER_BASIC, InvertedIndexParserType::PARSER_BASIC, "", false},
            {INVERTED_INDEX_PARSER_IK, InvertedIndexParserType::PARSER_IK,
             INVERTED_INDEX_PARSER_SMART, true}};

    // Test all builtin analyzers
    for (const auto& test_case : builtin_cases) {
        // Test is_builtin_analyzer returns true
        EXPECT_TRUE(InvertedIndexAnalyzer::is_builtin_analyzer(test_case.name))
                << "Failed for: " << test_case.name;

        // Test create_builtin_analyzer works
        // For analyzers that require dict files, allow exception
        if (test_case.requires_dict) {
            try {
                auto analyzer = InvertedIndexAnalyzer::create_builtin_analyzer(
                        test_case.parser_type, test_case.parser_mode, "", "");
                // If dict exists, analyzer should not be null
                EXPECT_NE(analyzer, nullptr)
                        << "Created analyzer for: " << test_case.name << " (dict available)";
            } catch (const std::exception& e) {
                // If dict doesn't exist, allow the exception and log it
                LOG(INFO) << "Skipped creating " << test_case.name
                          << " due to missing dict: " << e.what();
            }
        } else {
            auto analyzer = InvertedIndexAnalyzer::create_builtin_analyzer(
                    test_case.parser_type, test_case.parser_mode, "", "");
            EXPECT_NE(analyzer, nullptr) << "Failed to create analyzer for: " << test_case.name;
        }
    }

    // Test non-builtin names return false
    EXPECT_FALSE(InvertedIndexAnalyzer::is_builtin_analyzer("my_custom_analyzer"));
    EXPECT_FALSE(InvertedIndexAnalyzer::is_builtin_analyzer(""));
    EXPECT_FALSE(InvertedIndexAnalyzer::is_builtin_analyzer("Standard")); // case sensitive

    // Test different parser modes (Chinese with fine granularity)
    try {
        auto chinese_fine = InvertedIndexAnalyzer::create_builtin_analyzer(
                InvertedIndexParserType::PARSER_CHINESE, INVERTED_INDEX_PARSER_FINE_GRANULARITY, "",
                "");
        EXPECT_NE(chinese_fine, nullptr);
    } catch (const std::exception& e) {
        LOG(INFO) << "Skipped Chinese fine granularity test due to missing dict: " << e.what();
    }

    // Test IK with max word mode
    try {
        auto ik_maxword = InvertedIndexAnalyzer::create_builtin_analyzer(
                InvertedIndexParserType::PARSER_IK, INVERTED_INDEX_PARSER_MAX_WORD, "", "");
        EXPECT_NE(ik_maxword, nullptr);
    } catch (const std::exception& e) {
        LOG(INFO) << "Skipped IK max word test due to missing dict: " << e.what();
    }

    // Test lowercase and stopwords settings (using STANDARD which doesn't require dict)
    auto with_lower = InvertedIndexAnalyzer::create_builtin_analyzer(
            InvertedIndexParserType::PARSER_STANDARD, "", INVERTED_INDEX_PARSER_TRUE, "");
    EXPECT_NE(with_lower, nullptr);

    auto without_lower = InvertedIndexAnalyzer::create_builtin_analyzer(
            InvertedIndexParserType::PARSER_STANDARD, "", INVERTED_INDEX_PARSER_FALSE, "");
    EXPECT_NE(without_lower, nullptr);

    auto with_stopwords = InvertedIndexAnalyzer::create_builtin_analyzer(
            InvertedIndexParserType::PARSER_STANDARD, "", "", "none");
    EXPECT_NE(with_stopwords, nullptr);

    // Test unknown parser type falls back to default
    auto unknown = InvertedIndexAnalyzer::create_builtin_analyzer(
            InvertedIndexParserType::PARSER_UNKNOWN, "", "", "");
    EXPECT_NE(unknown, nullptr);
}

// ==================== Combined test for create_analyzer ====================

TEST_F(AnalyzerTest, TestCreateAnalyzer) {
    // Test Case 1: Empty custom_analyzer, use builtin parser_type
    {
        InvertedIndexCtx ctx;
        ctx.custom_analyzer = "";
        ctx.parser_type = InvertedIndexParserType::PARSER_STANDARD;
        ctx.parser_mode = "";
        ctx.lower_case = INVERTED_INDEX_PARSER_TRUE;
        ctx.stop_words = "none";

        auto analyzer = InvertedIndexAnalyzer::create_analyzer(&ctx);
        EXPECT_NE(analyzer, nullptr);
    }

    // Test Case 2: custom_analyzer is a builtin name (using one that doesn't need dict)
    {
        InvertedIndexCtx ctx;
        ctx.custom_analyzer = INVERTED_INDEX_PARSER_ENGLISH;
        ctx.parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
        ctx.parser_mode = "";
        ctx.lower_case = INVERTED_INDEX_PARSER_FALSE;
        ctx.stop_words = "";

        auto analyzer = InvertedIndexAnalyzer::create_analyzer(&ctx);
        EXPECT_NE(analyzer, nullptr);
    }

    // Test Case 3: Test all builtin names work through create_analyzer
    std::vector<std::pair<std::string, bool>> builtin_names = {
            {INVERTED_INDEX_PARSER_STANDARD, false},
            {INVERTED_INDEX_PARSER_UNICODE, false},
            {INVERTED_INDEX_PARSER_ENGLISH, false},
            {INVERTED_INDEX_PARSER_CHINESE, true}, // requires dict
            {INVERTED_INDEX_PARSER_ICU, true},     // requires dict
            {INVERTED_INDEX_PARSER_BASIC, false},
            {INVERTED_INDEX_PARSER_IK, true} // requires dict
    };

    for (const auto& [name, requires_dict] : builtin_names) {
        InvertedIndexCtx ctx;
        ctx.custom_analyzer = name;
        ctx.parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
        ctx.parser_mode = "";
        ctx.lower_case = "";
        ctx.stop_words = "";

        if (requires_dict) {
            try {
                auto analyzer = InvertedIndexAnalyzer::create_analyzer(&ctx);
                EXPECT_NE(analyzer, nullptr) << "Created analyzer for builtin name: " << name;
            } catch (const std::exception& e) {
                LOG(INFO) << "Skipped " << name << " due to missing dict: " << e.what();
            }
        } else {
            auto analyzer = InvertedIndexAnalyzer::create_analyzer(&ctx);
            EXPECT_NE(analyzer, nullptr) << "Failed for builtin name: " << name;
        }
    }

    // Test Case 4: Test with different parser types
    std::vector<std::pair<InvertedIndexParserType, bool>> parser_types = {
            {InvertedIndexParserType::PARSER_STANDARD, false},
            {InvertedIndexParserType::PARSER_UNICODE, false},
            {InvertedIndexParserType::PARSER_ENGLISH, false},
            {InvertedIndexParserType::PARSER_CHINESE, true}, // requires dict
            {InvertedIndexParserType::PARSER_ICU, true},     // requires dict
            {InvertedIndexParserType::PARSER_BASIC, false},
            {InvertedIndexParserType::PARSER_IK, true} // requires dict
    };

    for (const auto& [parser_type, requires_dict] : parser_types) {
        InvertedIndexCtx ctx;
        ctx.custom_analyzer = "";
        ctx.parser_type = parser_type;
        ctx.parser_mode = "";
        ctx.lower_case = "";
        ctx.stop_words = "";

        if (requires_dict) {
            try {
                auto analyzer = InvertedIndexAnalyzer::create_analyzer(&ctx);
                EXPECT_NE(analyzer, nullptr)
                        << "Created analyzer for parser_type: " << static_cast<int>(parser_type);
            } catch (const std::exception& e) {
                LOG(INFO) << "Skipped parser_type " << static_cast<int>(parser_type)
                          << " due to missing dict: " << e.what();
            }
        } else {
            auto analyzer = InvertedIndexAnalyzer::create_analyzer(&ctx);
            EXPECT_NE(analyzer, nullptr)
                    << "Failed for parser_type: " << static_cast<int>(parser_type);
        }
    }
}

// ==================== Test create_analyzer with index_policy_mgr ====================

TEST_F(AnalyzerTest, TestCreateAnalyzerWithCustomPolicy) {
    // Test when index_policy_mgr is null - should throw exception
    {
        InvertedIndexCtx ctx;
        ctx.custom_analyzer = "non_existent_custom";
        ctx.parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
        ctx.parser_mode = "";
        ctx.lower_case = "";
        ctx.stop_words = "";

        if (!doris::ExecEnv::GetInstance()->index_policy_mgr()) {
            EXPECT_THROW(
                    {
                        try {
                            InvertedIndexAnalyzer::create_analyzer(&ctx);
                        } catch (const Exception& e) {
                            EXPECT_EQ(e.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
                            EXPECT_TRUE(std::string(e.what()).find(
                                                "Index policy manager is not initialized") !=
                                        std::string::npos);
                            throw;
                        }
                    },
                    Exception);
        }
    }

    // Test with properly configured index_policy_mgr
    auto* mgr = doris::ExecEnv::GetInstance()->index_policy_mgr();
    if (mgr) {
        SetupCustomAnalyzerPolicies();

        // Test successful custom analyzer retrieval
        {
            InvertedIndexCtx ctx;
            ctx.custom_analyzer = "test_custom_analyzer";
            ctx.parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
            ctx.parser_mode = "";
            ctx.lower_case = "";
            ctx.stop_words = "";

            auto analyzer = InvertedIndexAnalyzer::create_analyzer(&ctx);
            EXPECT_NE(analyzer, nullptr);
        }

        // Test non-existent custom analyzer throws exception
        {
            InvertedIndexCtx ctx;
            ctx.custom_analyzer = "non_existent_analyzer";
            ctx.parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
            ctx.parser_mode = "";
            ctx.lower_case = "";
            ctx.stop_words = "";

            EXPECT_THROW(InvertedIndexAnalyzer::create_analyzer(&ctx), Exception);
        }
    }
}

// ==================== Integration test ====================

TEST_F(AnalyzerTest, TestAnalyzerFunctionality) {
    // Create an analyzer and test it can tokenize text properly
    InvertedIndexCtx ctx;
    ctx.custom_analyzer = "";
    ctx.parser_type = InvertedIndexParserType::PARSER_STANDARD;
    ctx.parser_mode = "";
    ctx.lower_case = INVERTED_INDEX_PARSER_TRUE;
    ctx.stop_words = "none";

    auto analyzer = InvertedIndexAnalyzer::create_analyzer(&ctx);
    ASSERT_NE(analyzer, nullptr);

    // Test tokenization
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    std::string text = "Hello World Test";
    reader->init(text.data(), static_cast<int32_t>(text.size()), true);

    auto result = InvertedIndexAnalyzer::get_analyse_result(reader, analyzer.get());
    EXPECT_GT(result.size(), 0);

    // Verify tokens are not empty
    for (const auto& term_info : result) {
        // term is a variant, need to check based on its actual type
        if (term_info.is_single_term()) {
            EXPECT_FALSE(term_info.get_single_term().empty());
        } else {
            EXPECT_FALSE(term_info.get_multi_terms().empty());
        }
        EXPECT_GE(term_info.position, 0);
    }
}

} // namespace doris::segment_v2::inverted_index
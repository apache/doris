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

#include "storage/index/inverted/analysis/analyzer/analyzer.h"

#include <gen_cpp/AgentService_types.h>
#include <gtest/gtest.h>

#include <map>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/analysis/reader.h"
#include "util/defer_op.h"

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
        InvertedIndexAnalyzerConfig config;
        config.analyzer_name = "";
        config.parser_type = InvertedIndexParserType::PARSER_STANDARD;
        config.parser_mode = "";
        config.lower_case = INVERTED_INDEX_PARSER_TRUE;
        config.stop_words = "none";

        auto analyzer = InvertedIndexAnalyzer::create_analyzer(&config);
        EXPECT_NE(analyzer, nullptr);
    }

    // Test Case 2: custom_analyzer is a builtin name (using one that doesn't need dict)
    {
        InvertedIndexAnalyzerConfig config;
        config.analyzer_name = INVERTED_INDEX_PARSER_ENGLISH;
        config.parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
        config.parser_mode = "";
        config.lower_case = INVERTED_INDEX_PARSER_FALSE;
        config.stop_words = "";

        auto analyzer = InvertedIndexAnalyzer::create_analyzer(&config);
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
        InvertedIndexAnalyzerConfig config;
        config.analyzer_name = name;
        config.parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
        config.parser_mode = "";
        config.lower_case = "";
        config.stop_words = "";

        if (requires_dict) {
            try {
                auto analyzer = InvertedIndexAnalyzer::create_analyzer(&config);
                EXPECT_NE(analyzer, nullptr) << "Created analyzer for builtin name: " << name;
            } catch (const std::exception& e) {
                LOG(INFO) << "Skipped " << name << " due to missing dict: " << e.what();
            }
        } else {
            auto analyzer = InvertedIndexAnalyzer::create_analyzer(&config);
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
        InvertedIndexAnalyzerConfig config;
        config.analyzer_name = "";
        config.parser_type = parser_type;
        config.parser_mode = "";
        config.lower_case = "";
        config.stop_words = "";

        if (requires_dict) {
            try {
                auto analyzer = InvertedIndexAnalyzer::create_analyzer(&config);
                EXPECT_NE(analyzer, nullptr)
                        << "Created analyzer for parser_type: " << static_cast<int>(parser_type);
            } catch (const std::exception& e) {
                LOG(INFO) << "Skipped parser_type " << static_cast<int>(parser_type)
                          << " due to missing dict: " << e.what();
            }
        } else {
            auto analyzer = InvertedIndexAnalyzer::create_analyzer(&config);
            EXPECT_NE(analyzer, nullptr)
                    << "Failed for parser_type: " << static_cast<int>(parser_type);
        }
    }
}

// ==================== Test create_analyzer with index_policy_mgr ====================

TEST_F(AnalyzerTest, TestCreateAnalyzerWithCustomPolicy) {
    // Test when index_policy_mgr is null - should throw exception
    {
        InvertedIndexAnalyzerConfig config;
        config.analyzer_name = "non_existent_custom";
        config.parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
        config.parser_mode = "";
        config.lower_case = "";
        config.stop_words = "";

        if (!doris::ExecEnv::GetInstance()->index_policy_mgr()) {
            EXPECT_THROW(
                    {
                        try {
                            InvertedIndexAnalyzer::create_analyzer(&config);
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
            InvertedIndexAnalyzerConfig config;
            config.analyzer_name = "test_custom_analyzer";
            config.parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
            config.parser_mode = "";
            config.lower_case = "";
            config.stop_words = "";

            auto analyzer = InvertedIndexAnalyzer::create_analyzer(&config);
            EXPECT_NE(analyzer, nullptr);
        }

        // Test non-existent custom analyzer throws exception
        {
            InvertedIndexAnalyzerConfig config;
            config.analyzer_name = "non_existent_analyzer";
            config.parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
            config.parser_mode = "";
            config.lower_case = "";
            config.stop_words = "";

            EXPECT_THROW(InvertedIndexAnalyzer::create_analyzer(&config), Exception);
        }
    }
}

// ==================== Integration test ====================

TEST_F(AnalyzerTest, TestAnalyzerFunctionality) {
    // Create an analyzer and test it can tokenize text properly
    InvertedIndexAnalyzerConfig config;
    config.analyzer_name = "";
    config.parser_type = InvertedIndexParserType::PARSER_STANDARD;
    config.parser_mode = "";
    config.lower_case = INVERTED_INDEX_PARSER_TRUE;
    config.stop_words = "none";

    auto analyzer = InvertedIndexAnalyzer::create_analyzer(&config);
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

// ==================== Query values ====================

TEST_F(AnalyzerTest, TestAnalyzeQueryValue) {
    const std::map<std::string, std::string> english = {{"parser", "english"},
                                                        {"lower_case", "true"}};
    const std::map<std::string, std::string> keyword;
    const auto terms = [](const std::vector<TermInfo>& infos) {
        std::vector<std::string> out;
        for (const auto& info : infos) {
            out.push_back(info.get_single_term());
        }
        return out;
    };
    std::vector<TermInfo> out;

    // Without a query context the index's properties decide.
    ASSERT_TRUE(InvertedIndexAnalyzer::analyze("Hello World", nullptr, english, &out).ok());
    EXPECT_EQ(terms(out), (std::vector<std::string> {"hello", "world"}));
    EXPECT_EQ(out[1].position, 2);
    ASSERT_TRUE(InvertedIndexAnalyzer::analyze("Hello World", nullptr, keyword, &out).ok());
    EXPECT_EQ(terms(out), std::vector<std::string> {"Hello World"});

    // A context that does not analyze keeps the value whole, even on a tokenized index.
    InvertedIndexAnalyzerCtx untokenized;
    untokenized.parser_type = InvertedIndexParserType::PARSER_NONE;
    ASSERT_TRUE(InvertedIndexAnalyzer::analyze("Hello World", &untokenized, english, &out).ok());
    EXPECT_EQ(terms(out), std::vector<std::string> {"Hello World"});

    // A context's analyzer comes before the index's.
    InvertedIndexAnalyzerCtx standard;
    standard.parser_type = InvertedIndexParserType::PARSER_STANDARD;
    standard.analyzer = InvertedIndexAnalyzer::create_builtin_analyzer(
            InvertedIndexParserType::PARSER_STANDARD, "", INVERTED_INDEX_PARSER_FALSE, "none");
    ASSERT_TRUE(InvertedIndexAnalyzer::analyze("Hello World", &standard, english, &out).ok());
    EXPECT_EQ(terms(out), (std::vector<std::string> {"Hello", "World"}));

    // A context without an analyzer leaves the choice to the index.
    InvertedIndexAnalyzerCtx without_analyzer;
    without_analyzer.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    ASSERT_TRUE(
            InvertedIndexAnalyzer::analyze("Hello World", &without_analyzer, english, &out).ok());
    EXPECT_EQ(terms(out), (std::vector<std::string> {"hello", "world"}));

    const std::map<std::string, std::string> missing = {{"analyzer", "no_such_analyzer"}};
    EXPECT_TRUE(InvertedIndexAnalyzer::analyze("Hello", nullptr, missing, &out)
                        .is<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>());
}

// A prefix or a wildcard normalizes its literal text into the term a builtin parser writes for it:
// through the character filter, the width folding of IK and kuromoji, and the parser's own
// lowercasing.
TEST_F(AnalyzerTest, TestNormalizeMatchesTheTermsOfBuiltinParsers) {
    const std::string saved_dict_path = config::inverted_index_dict_path;
    Defer restore_dict_path {[&] { config::inverted_index_dict_path = saved_dict_path; }};
    // Unit tests run from the source root, as in the ICU tokenizer tests.
    config::inverted_index_dict_path = "./be/dict";

    const std::string eclair =
            "\xC3\x89"
            "Clair";
    // "Clair" in full-width letters.
    const std::string full_width_clair =
            "\xEF\xBC\xA3\xEF\xBD\x8C\xEF\xBD\x81\xEF\xBD\x89\xEF\xBD\x92";
    // `properties` must analyze `value` into one term, which normalize must return too.
    const auto expect_term = [](std::map<std::string, std::string> properties,
                                const std::string& value) {
        for (const auto& lower_case : {INVERTED_INDEX_PARSER_TRUE, INVERTED_INDEX_PARSER_FALSE}) {
            properties[INVERTED_INDEX_PARSER_LOWERCASE_KEY] = lower_case;
            const auto terms = InvertedIndexAnalyzer::get_analyse_result(value, properties);
            ASSERT_EQ(terms.size(), 1) << properties[INVERTED_INDEX_PARSER_KEY] << " " << value;
            EXPECT_EQ(InvertedIndexAnalyzer::normalize(value, properties),
                      terms[0].get_single_term())
                    << properties[INVERTED_INDEX_PARSER_KEY] << " " << value
                    << " lower_case=" << lower_case;
        }
    };
    for (const auto& [parser, value] :
         std::vector<std::pair<std::string, std::string>> {{INVERTED_INDEX_PARSER_STANDARD, eclair},
                                                           {INVERTED_INDEX_PARSER_UNICODE, eclair},
                                                           {INVERTED_INDEX_PARSER_ENGLISH, "Clair"},
                                                           {INVERTED_INDEX_PARSER_BASIC, "Clair"},
                                                           {INVERTED_INDEX_PARSER_ICU, eclair}}) {
        expect_term({{INVERTED_INDEX_PARSER_KEY, parser}}, value);
    }
    expect_term({{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD},
                 {INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE},
                 {INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "-"},
                 {INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "x"}},
                "A-B");

    // The kuromoji dictionary is generated when the backend builds, and IK loads its dictionary
    // once per process while a test of a missing dictionary leaves it unusable, so the terms these
    // parsers write are spelled out: both fold full-width ASCII to ASCII, and kuromoji lowercases
    // every letter where IK lowercases ASCII letters only.
    const std::string eclair_lowered =
            "\xC3\xA9"
            "clair";
    for (const auto& [parser, value, lowered, kept] :
         std::vector<std::tuple<std::string, std::string, std::string, std::string>> {
                 {INVERTED_INDEX_PARSER_KUROMOJI, eclair, eclair_lowered, eclair},
                 {INVERTED_INDEX_PARSER_KUROMOJI, full_width_clair, "clair", "Clair"},
                 {INVERTED_INDEX_PARSER_IK, full_width_clair, "clair", "Clair"}}) {
        for (const auto& [lower_case, term] : std::vector<std::pair<std::string, std::string>> {
                     {INVERTED_INDEX_PARSER_TRUE, lowered}, {INVERTED_INDEX_PARSER_FALSE, kept}}) {
            EXPECT_EQ(InvertedIndexAnalyzer::normalize(
                              value, {{INVERTED_INDEX_PARSER_KEY, parser},
                                      {INVERTED_INDEX_PARSER_LOWERCASE_KEY, lower_case}}),
                      term)
                    << parser << " " << value << " lower_case=" << lower_case;
        }
    }
}

// A custom analyzer reads the text through the index's character filter too, before its own
// per-character part, and so does one without a per-character part: normalize returns the term the
// analyzer writes.
TEST_F(AnalyzerTest, TestNormalizeAppliesTheIndexCharacterFilterToCustomAnalyzers) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_policy_mgr = exec_env->index_policy_mgr();
    exec_env->_index_policy_mgr = _index_policy_mgr.get();
    Defer restore_policy_mgr {[&] { exec_env->_index_policy_mgr = previous_policy_mgr; }};
    TIndexPolicy lowercased;
    lowercased.id = 4101;
    lowercased.name = "char_filter_lowercase_analyzer";
    lowercased.type = TIndexPolicyType::ANALYZER;
    lowercased.properties = {{"tokenizer", "standard"}, {"token_filter", "lowercase"}};
    TIndexPolicy plain;
    plain.id = 4102;
    plain.name = "char_filter_plain_analyzer";
    plain.type = TIndexPolicyType::ANALYZER;
    plain.properties = {{"tokenizer", "standard"}};
    _index_policy_mgr->apply_policy_changes({lowercased, plain}, {});

    for (const auto& name : {lowercased.name, plain.name}) {
        const std::map<std::string, std::string> properties = {
                {INVERTED_INDEX_ANALYZER_NAME_KEY, name},
                {INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE},
                {INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "-"},
                {INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "x"}};
        const auto terms = InvertedIndexAnalyzer::get_analyse_result("A-B", properties);
        ASSERT_EQ(terms.size(), 1) << name;
        EXPECT_EQ(InvertedIndexAnalyzer::normalize("A-B", properties), terms[0].get_single_term())
                << name;
    }
}

} // namespace doris::segment_v2::inverted_index
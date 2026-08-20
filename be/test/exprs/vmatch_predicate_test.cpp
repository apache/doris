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

#include "exprs/vmatch_predicate.h"

#include <gtest/gtest.h>

#include <array>
#include <string>

#include "core/data_type/primitive_type.h"

namespace doris {
namespace {

TExprNode make_match_node(const std::string& analyzer_name, const std::string& parser_type) {
    TMatchPredicate match_predicate;
    match_predicate.__set_analyzer_name(analyzer_name);
    match_predicate.__set_parser_type(parser_type);
    match_predicate.__set_parser_mode("");

    TExprNode node;
    node.__set_node_type(TExprNodeType::MATCH_PRED);
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_num_children(2);
    node.__set_match_predicate(match_predicate);
    return node;
}

TEST(VMatchPredicateTest, ExplicitNoneKeepsSelectionKeyWithoutAnalyzer) {
    auto predicate = VMatchPredicate::create_shared(make_match_node("none", "english"));
    const auto* analyzer_ctx = predicate->query_analyzer_ctx();

    EXPECT_EQ(predicate->get_analyzer_key(), "none");
    EXPECT_FALSE(analyzer_ctx->requires_analysis());
    EXPECT_EQ(analyzer_ctx->analyzer, nullptr);
    EXPECT_EQ(analyzer_ctx->analyzer_provider, nullptr);
}

TEST(VMatchPredicateTest, ResolvesBuiltinAndFallbackExecutionModes) {
    struct TestCase {
        std::string analyzer_name;
        std::string parser_type;
        std::string expected_key;
        InvertedIndexParserType expected_parser;
        bool requires_analysis;
    };
    const std::array test_cases {
            TestCase {"english", "chinese", "english", InvertedIndexParserType::PARSER_ENGLISH,
                      true},
            TestCase {"", "standard", "", InvertedIndexParserType::PARSER_STANDARD, true},
            TestCase {"", "unknown", "", InvertedIndexParserType::PARSER_UNKNOWN, true},
            TestCase {"", "", "", InvertedIndexParserType::PARSER_UNKNOWN, true},
    };

    for (const auto& test_case : test_cases) {
        auto predicate = VMatchPredicate::create_shared(
                make_match_node(test_case.analyzer_name, test_case.parser_type));
        const auto* analyzer_ctx = predicate->query_analyzer_ctx();

        EXPECT_EQ(predicate->get_analyzer_key(), test_case.expected_key);
        EXPECT_TRUE(analyzer_ctx->analyzer_name.empty());
        EXPECT_EQ(analyzer_ctx->parser_type, test_case.expected_parser);
        EXPECT_EQ(analyzer_ctx->requires_analysis(), test_case.requires_analysis);
        EXPECT_EQ(analyzer_ctx->analyzer != nullptr, test_case.requires_analysis);
        EXPECT_EQ(analyzer_ctx->analyzer_provider != nullptr, test_case.requires_analysis);
    }
}

} // namespace
} // namespace doris

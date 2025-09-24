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

#include "vec/functions/function_search.h"

#include <gtest/gtest.h>

#include "gen_cpp/Exprs_types.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class FunctionSearchTest : public testing::Test {
public:
    void SetUp() override { function_search = std::make_shared<FunctionSearch>(); }

protected:
    std::shared_ptr<FunctionSearch> function_search;
};

TEST_F(FunctionSearchTest, TestGetName) {
    EXPECT_EQ("search", function_search->get_name());
}

TEST_F(FunctionSearchTest, TestClauseTypeCategory) {
    // Test NON_TOKENIZED types
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("TERM"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("PREFIX"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("WILDCARD"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("REGEXP"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("RANGE"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("LIST"));

    // Test TOKENIZED types
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::TOKENIZED,
              function_search->get_clause_type_category("PHRASE"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::TOKENIZED,
              function_search->get_clause_type_category("MATCH"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::TOKENIZED,
              function_search->get_clause_type_category("ANY"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::TOKENIZED,
              function_search->get_clause_type_category("ALL"));

    // Test COMPOUND types
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::COMPOUND,
              function_search->get_clause_type_category("AND"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::COMPOUND,
              function_search->get_clause_type_category("OR"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::COMPOUND,
              function_search->get_clause_type_category("NOT"));

    // Test unknown type - should default to NON_TOKENIZED
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("UNKNOWN"));
}

TEST_F(FunctionSearchTest, TestAnalyzeFieldQueryTypeSimpleLeaf) {
    // Test TERM query
    TSearchClause termClause;
    termClause.clause_type = "TERM";
    termClause.field_name = "title";
    termClause.value = "hello";

    auto query_type = function_search->analyze_field_query_type("title", termClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type);

    // Test PHRASE query
    TSearchClause phraseClause;
    phraseClause.clause_type = "PHRASE";
    phraseClause.field_name = "content";
    phraseClause.value = "machine learning";

    query_type = function_search->analyze_field_query_type("content", phraseClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, query_type);

    // Test PREFIX query
    TSearchClause prefixClause;
    prefixClause.clause_type = "PREFIX";
    prefixClause.field_name = "title";
    prefixClause.value = "hello*";

    query_type = function_search->analyze_field_query_type("title", prefixClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestAnalyzeFieldQueryTypeCompound) {
    // Test AND query with mixed children
    TSearchClause termChild;
    termChild.clause_type = "TERM";
    termChild.field_name = "title";
    termChild.value = "hello";

    TSearchClause phraseChild;
    phraseChild.clause_type = "PHRASE";
    phraseChild.field_name = "content";
    phraseChild.value = "machine learning";

    TSearchClause andClause;
    andClause.clause_type = "AND";
    andClause.children = {termChild, phraseChild};

    // Test field-specific query type analysis
    auto title_query_type = function_search->analyze_field_query_type("title", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto content_query_type = function_search->analyze_field_query_type("content", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, content_query_type);
}

TEST_F(FunctionSearchTest, TestAnalyzeFieldQueryTypeCompoundNonTokenized) {
    // Test AND query with only non-tokenized children
    TSearchClause termChild1;
    termChild1.clause_type = "TERM";
    termChild1.field_name = "title";
    termChild1.value = "hello";

    TSearchClause termChild2;
    termChild2.clause_type = "TERM";
    termChild2.field_name = "category";
    termChild2.value = "tech";

    TSearchClause andClause;
    andClause.clause_type = "AND";
    andClause.children = {termChild1, termChild2};

    // Test field-specific query type analysis
    auto title_query_type = function_search->analyze_field_query_type("title", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto category_query_type = function_search->analyze_field_query_type("category", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, category_query_type);
}

TEST_F(FunctionSearchTest, TestBuildSearchParam) {
    // Create test search param
    TSearchParam searchParam;
    searchParam.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "title";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    // Test successful creation
    EXPECT_EQ("title:hello", searchParam.original_dsl);
    EXPECT_EQ("TERM", searchParam.root.clause_type);
    EXPECT_EQ("title", searchParam.root.field_name);
    EXPECT_EQ("hello", searchParam.root.value);
    EXPECT_EQ(1, searchParam.field_bindings.size());
    EXPECT_EQ("title", searchParam.field_bindings[0].field_name);
    EXPECT_EQ(0, searchParam.field_bindings[0].slot_index);
}

TEST_F(FunctionSearchTest, TestComplexSearchParam) {
    // Create complex search param with AND clause
    TSearchParam searchParam;
    searchParam.original_dsl = "title:hello AND content:world";

    // Create child clauses
    TSearchClause titleClause;
    titleClause.clause_type = "TERM";
    titleClause.field_name = "title";
    titleClause.value = "hello";

    TSearchClause contentClause;
    contentClause.clause_type = "TERM";
    contentClause.field_name = "content";
    contentClause.value = "world";

    // Create root AND clause
    TSearchClause rootClause;
    rootClause.clause_type = "AND";
    rootClause.children = {titleClause, contentClause};
    searchParam.root = rootClause;

    // Create field bindings
    TSearchFieldBinding titleBinding;
    titleBinding.field_name = "title";
    titleBinding.slot_index = 0;

    TSearchFieldBinding contentBinding;
    contentBinding.field_name = "content";
    contentBinding.slot_index = 1;

    searchParam.field_bindings = {titleBinding, contentBinding};

    // Verify structure
    EXPECT_EQ("title:hello AND content:world", searchParam.original_dsl);
    EXPECT_EQ("AND", searchParam.root.clause_type);
    EXPECT_EQ(2, searchParam.root.children.size());
    EXPECT_EQ("TERM", searchParam.root.children[0].clause_type);
    EXPECT_EQ("title", searchParam.root.children[0].field_name);
    EXPECT_EQ("hello", searchParam.root.children[0].value);
    EXPECT_EQ("TERM", searchParam.root.children[1].clause_type);
    EXPECT_EQ("content", searchParam.root.children[1].field_name);
    EXPECT_EQ("world", searchParam.root.children[1].value);
    EXPECT_EQ(2, searchParam.field_bindings.size());
}

TEST_F(FunctionSearchTest, TestPhraseClause) {
    TSearchParam searchParam;
    searchParam.original_dsl = "content:\"machine learning\"";

    TSearchClause rootClause;
    rootClause.clause_type = "PHRASE";
    rootClause.field_name = "content";
    rootClause.value = "machine learning";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "content";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    // Verify phrase handling
    EXPECT_EQ("PHRASE", searchParam.root.clause_type);
    EXPECT_EQ("content", searchParam.root.field_name);
    EXPECT_EQ("machine learning", searchParam.root.value);

    auto query_type = function_search->analyze_field_query_type("content", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestRegexpClause) {
    TSearchParam searchParam;
    searchParam.original_dsl = "title:/[a-z]+/";

    TSearchClause rootClause;
    rootClause.clause_type = "REGEXP";
    rootClause.field_name = "title";
    rootClause.value = "[a-z]+"; // slashes should be removed by parser
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "title";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    // Verify regexp handling
    EXPECT_EQ("REGEXP", searchParam.root.clause_type);
    EXPECT_EQ("title", searchParam.root.field_name);
    EXPECT_EQ("[a-z]+", searchParam.root.value);

    auto query_type = function_search->analyze_field_query_type("title", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_REGEXP_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestRangeClause) {
    TSearchParam searchParam;
    searchParam.original_dsl = "age:[18 TO 65]";

    TSearchClause rootClause;
    rootClause.clause_type = "RANGE";
    rootClause.field_name = "age";
    rootClause.value = "[18 TO 65]";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "age";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    // Verify range handling
    EXPECT_EQ("RANGE", searchParam.root.clause_type);
    EXPECT_EQ("age", searchParam.root.field_name);
    EXPECT_EQ("[18 TO 65]", searchParam.root.value);

    auto query_type = function_search->analyze_field_query_type("age", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::RANGE_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestAnyAllClauses) {
    // Test ANY clause
    TSearchParam anyParam;
    anyParam.original_dsl = "tags:ANY(java python)";

    TSearchClause anyClause;
    anyClause.clause_type = "ANY";
    anyClause.field_name = "tags";
    anyClause.value = "java python";
    anyParam.root = anyClause;

    auto query_type = function_search->analyze_field_query_type("tags", anyParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, query_type);

    // Test ALL clause
    TSearchParam allParam;
    allParam.original_dsl = "tags:ALL(programming language)";

    TSearchClause allClause;
    allClause.clause_type = "ALL";
    allClause.field_name = "tags";
    allClause.value = "programming language";
    allParam.root = allClause;

    query_type = function_search->analyze_field_query_type("tags", allParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestAnalyzeFieldQueryType) {
    // Test compound query with different field types
    TSearchClause termChild;
    termChild.clause_type = "TERM";
    termChild.field_name = "title";
    termChild.value = "hello";

    TSearchClause phraseChild;
    phraseChild.clause_type = "PHRASE";
    phraseChild.field_name = "content";
    phraseChild.value = "machine learning";

    TSearchClause andClause;
    andClause.clause_type = "AND";
    andClause.children = {termChild, phraseChild};

    // Test field-specific query type analysis
    auto title_query_type = function_search->analyze_field_query_type("title", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto content_query_type = function_search->analyze_field_query_type("content", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, content_query_type);

    // Test field not in query
    auto other_query_type = function_search->analyze_field_query_type("other_field", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, other_query_type);

    // Test single field query
    auto single_field_type = function_search->analyze_field_query_type("title", termChild);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, single_field_type);

    auto single_phrase_type = function_search->analyze_field_query_type("content", phraseChild);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, single_phrase_type);
}

TEST_F(FunctionSearchTest, TestClauseTypeToQueryType) {
    // Test non-tokenized queries
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY,
              function_search->clause_type_to_query_type("TERM"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
              function_search->clause_type_to_query_type("PREFIX"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::WILDCARD_QUERY,
              function_search->clause_type_to_query_type("WILDCARD"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_REGEXP_QUERY,
              function_search->clause_type_to_query_type("REGEXP"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::RANGE_QUERY,
              function_search->clause_type_to_query_type("RANGE"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::LIST_QUERY,
              function_search->clause_type_to_query_type("LIST"));

    // Test tokenized queries
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY,
              function_search->clause_type_to_query_type("PHRASE"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY,
              function_search->clause_type_to_query_type("MATCH"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY,
              function_search->clause_type_to_query_type("ANY"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY,
              function_search->clause_type_to_query_type("ALL"));

    // Test boolean operations
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY,
              function_search->clause_type_to_query_type("AND"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY,
              function_search->clause_type_to_query_type("OR"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY,
              function_search->clause_type_to_query_type("NOT"));

    // Test unknown clause type
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY,
              function_search->clause_type_to_query_type("UNKNOWN"));
}

} // namespace doris::vectorized

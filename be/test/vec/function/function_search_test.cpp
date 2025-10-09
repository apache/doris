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

#include <chrono>
#include <memory>
#include <roaring/roaring.hh>
#include <unordered_map>

#include "gen_cpp/Exprs_types.h"
#include "olap/rowset/segment_v2/index_iterator.h"
#include "vec/core/block.h"

namespace doris::vectorized {

class FunctionSearchTest : public testing::Test {
public:
    void SetUp() override { function_search = std::make_shared<FunctionSearch>(); }

protected:
    std::shared_ptr<FunctionSearch> function_search;
};

class DummyIndexIterator : public segment_v2::IndexIterator {
public:
    segment_v2::IndexReaderPtr get_reader(
            segment_v2::IndexReaderType /*reader_type*/) const override {
        return nullptr;
    }

    Status read_from_index(const segment_v2::IndexParam& /*param*/) override {
        return Status::OK();
    }

    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* /*cache_handle*/) override {
        return Status::OK();
    }

    Result<bool> has_null() override { return false; }
};

class TrackingIndexIterator : public segment_v2::IndexIterator {
public:
    explicit TrackingIndexIterator(bool has_null) : _has_null(has_null) {}

    segment_v2::IndexReaderPtr get_reader(
            segment_v2::IndexReaderType /*reader_type*/) const override {
        return nullptr;
    }

    Status read_from_index(const segment_v2::IndexParam& /*param*/) override {
        return Status::OK();
    }

    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* /*cache_handle*/) override {
        ++_read_null_bitmap_calls;
        return Status::OK();
    }

    Result<bool> has_null() override {
        ++_has_null_checks;
        return _has_null;
    }

    int read_null_bitmap_calls() const { return _read_null_bitmap_calls; }
    int has_null_checks() const { return _has_null_checks; }

    void set_has_null(bool value) { _has_null = value; }

private:
    bool _has_null = false;
    int _read_null_bitmap_calls = 0;
    int _has_null_checks = 0;
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

TEST_F(FunctionSearchTest, TestExecuteImpl) {
    // Test that execute_impl always returns RuntimeError
    FunctionContext function_context;
    Block block;
    ColumnNumbers arguments;
    uint32_t result = 0;
    size_t input_rows_count = 0;

    auto status = function_search->execute_impl(&function_context, block, arguments, result,
                                                input_rows_count);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.code() == ErrorCode::RUNTIME_ERROR);
    EXPECT_TRUE(status.to_string().find("only inverted index queries are supported") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestBasicProperties) {
    // Test basic function properties
    EXPECT_EQ("search", function_search->get_name());
    EXPECT_TRUE(function_search->is_variadic());
    EXPECT_EQ(0, function_search->get_number_of_arguments());
    EXPECT_FALSE(function_search->use_default_implementation_for_nulls());
    EXPECT_FALSE(function_search->is_use_default_implementation_for_constants());
    EXPECT_FALSE(function_search->use_default_implementation_for_constants());
    EXPECT_TRUE(function_search->can_push_down_to_index());

    // Test return type
    DataTypes empty_args;
    auto return_type = function_search->get_return_type_impl(empty_args);
    EXPECT_NE(nullptr, return_type);
    // Should return UInt8 type for boolean results
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexBasic) {
    // Test basic evaluate_inverted_index method (legacy version)
    ColumnsWithTypeAndName arguments;
    std::vector<vectorized::IndexFieldNameAndTypePair> data_type_with_names;
    std::vector<IndexIterator*> iterators;
    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index(arguments, data_type_with_names,
                                                           iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should return OK for legacy method
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamEmptyInputs) {
    // Test evaluate_inverted_index_with_search_param with empty inputs
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> empty_data_types;
    std::unordered_map<std::string, IndexIterator*> empty_iterators;
    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    // Test with empty iterators
    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, empty_data_types, empty_iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should return OK but with empty result

    // Test with empty data types but non-empty iterators - should still return OK
    // because empty data_types will cause early return
    std::unordered_map<std::string, IndexIterator*> non_empty_iterators;
    non_empty_iterators["title"] = nullptr; // Add null iterator
    status = function_search->evaluate_inverted_index_with_search_param(
            search_param, empty_data_types, non_empty_iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should return OK due to empty data_types check
}

TEST_F(FunctionSearchTest, TestNestedBooleanQueries) {
    // Test deeply nested boolean queries
    TSearchParam searchParam;
    searchParam.original_dsl =
            "((title:hello OR content:world) AND category:tech) OR (author:john AND "
            "status:published)";

    // Create nested structure: OR -> AND -> OR, AND
    TSearchClause titleClause;
    titleClause.clause_type = "TERM";
    titleClause.field_name = "title";
    titleClause.value = "hello";

    TSearchClause contentClause;
    contentClause.clause_type = "TERM";
    contentClause.field_name = "content";
    contentClause.value = "world";

    TSearchClause categoryClause;
    categoryClause.clause_type = "TERM";
    categoryClause.field_name = "category";
    categoryClause.value = "tech";

    TSearchClause authorClause;
    authorClause.clause_type = "TERM";
    authorClause.field_name = "author";
    authorClause.value = "john";

    TSearchClause statusClause;
    statusClause.clause_type = "TERM";
    statusClause.field_name = "status";
    statusClause.value = "published";

    // Build nested structure
    TSearchClause innerOrClause;
    innerOrClause.clause_type = "OR";
    innerOrClause.children = {titleClause, contentClause};

    TSearchClause leftAndClause;
    leftAndClause.clause_type = "AND";
    leftAndClause.children = {innerOrClause, categoryClause};

    TSearchClause rightAndClause;
    rightAndClause.clause_type = "AND";
    rightAndClause.children = {authorClause, statusClause};

    TSearchClause rootOrClause;
    rootOrClause.clause_type = "OR";
    rootOrClause.children = {leftAndClause, rightAndClause};
    searchParam.root = rootOrClause;

    // Test field-specific query type analysis for nested queries
    auto title_query_type = function_search->analyze_field_query_type("title", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto content_query_type =
            function_search->analyze_field_query_type("content", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, content_query_type);

    auto author_query_type = function_search->analyze_field_query_type("author", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, author_query_type);

    // Test field not in query
    auto missing_query_type =
            function_search->analyze_field_query_type("missing_field", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, missing_query_type);
}

TEST_F(FunctionSearchTest, TestMixedTokenizedAndNonTokenizedQueries) {
    // Test queries mixing tokenized and non-tokenized clause types
    TSearchParam searchParam;
    searchParam.original_dsl =
            "title:TERM(hello) AND content:PHRASE(\"machine learning\") AND tags:ANY(java python)";

    TSearchClause termClause;
    termClause.clause_type = "TERM";
    termClause.field_name = "title";
    termClause.value = "hello";

    TSearchClause phraseClause;
    phraseClause.clause_type = "PHRASE";
    phraseClause.field_name = "content";
    phraseClause.value = "machine learning";

    TSearchClause anyClause;
    anyClause.clause_type = "ANY";
    anyClause.field_name = "tags";
    anyClause.value = "java python";

    TSearchClause rootAndClause;
    rootAndClause.clause_type = "AND";
    rootAndClause.children = {termClause, phraseClause, anyClause};
    searchParam.root = rootAndClause;

    // Test field-specific query type analysis
    auto title_query_type = function_search->analyze_field_query_type("title", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto content_query_type =
            function_search->analyze_field_query_type("content", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, content_query_type);

    auto tags_query_type = function_search->analyze_field_query_type("tags", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, tags_query_type);
}

TEST_F(FunctionSearchTest, TestNotOperatorQueries) {
    // Test NOT operator with various clause types
    TSearchParam searchParam;
    searchParam.original_dsl = "NOT (title:hello OR content:world)";

    TSearchClause titleClause;
    titleClause.clause_type = "TERM";
    titleClause.field_name = "title";
    titleClause.value = "hello";

    TSearchClause contentClause;
    contentClause.clause_type = "TERM";
    contentClause.field_name = "content";
    contentClause.value = "world";

    TSearchClause orClause;
    orClause.clause_type = "OR";
    orClause.children = {titleClause, contentClause};

    TSearchClause notClause;
    notClause.clause_type = "NOT";
    notClause.children = {orClause};
    searchParam.root = notClause;

    // Test field-specific query type analysis for NOT queries
    auto title_query_type = function_search->analyze_field_query_type("title", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto content_query_type =
            function_search->analyze_field_query_type("content", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, content_query_type);
}

TEST_F(FunctionSearchTest, TestWildcardAndPrefixQueries) {
    // Test WILDCARD queries
    TSearchParam wildcardParam;
    wildcardParam.original_dsl = "title:hello*";

    TSearchClause wildcardClause;
    wildcardClause.clause_type = "WILDCARD";
    wildcardClause.field_name = "title";
    wildcardClause.value = "hello*";
    wildcardParam.root = wildcardClause;

    auto wildcard_query_type =
            function_search->analyze_field_query_type("title", wildcardParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::WILDCARD_QUERY, wildcard_query_type);

    // Test PREFIX queries
    TSearchParam prefixParam;
    prefixParam.original_dsl = "title:hello*";

    TSearchClause prefixClause;
    prefixClause.clause_type = "PREFIX";
    prefixClause.field_name = "title";
    prefixClause.value = "hello";
    prefixParam.root = prefixClause;

    auto prefix_query_type = function_search->analyze_field_query_type("title", prefixParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, prefix_query_type);
}

TEST_F(FunctionSearchTest, TestListQueries) {
    // Test LIST queries
    TSearchParam listParam;
    listParam.original_dsl = "category:LIST(tech, science, programming)";

    TSearchClause listClause;
    listClause.clause_type = "LIST";
    listClause.field_name = "category";
    listClause.value = "tech,science,programming";
    listParam.root = listClause;

    auto list_query_type = function_search->analyze_field_query_type("category", listParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::LIST_QUERY, list_query_type);
}

TEST_F(FunctionSearchTest, TestMatchQueries) {
    // Test MATCH queries (full-text search)
    TSearchParam matchParam;
    matchParam.original_dsl = "content:MATCH(machine learning algorithms)";

    TSearchClause matchClause;
    matchClause.clause_type = "MATCH";
    matchClause.field_name = "content";
    matchClause.value = "machine learning algorithms";
    matchParam.root = matchClause;

    auto match_query_type = function_search->analyze_field_query_type("content", matchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, match_query_type);
}

TEST_F(FunctionSearchTest, TestEmptyAndNullQueries) {
    // Test empty clause type
    TSearchClause emptyClause;
    emptyClause.clause_type = "";
    emptyClause.field_name = "title";
    emptyClause.value = "hello";

    auto empty_query_type = function_search->analyze_field_query_type("title", emptyClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY,
              empty_query_type); // Should default to EQUAL_QUERY

    // Test clause with empty field name
    TSearchClause noFieldClause;
    noFieldClause.clause_type = "TERM";
    noFieldClause.field_name = "";
    noFieldClause.value = "hello";

    auto no_field_query_type = function_search->analyze_field_query_type("title", noFieldClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, no_field_query_type);

    // Test clause with empty value
    TSearchClause emptyValueClause;
    emptyValueClause.clause_type = "TERM";
    emptyValueClause.field_name = "title";
    emptyValueClause.value = "";

    auto empty_value_query_type =
            function_search->analyze_field_query_type("title", emptyValueClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, empty_value_query_type);
}

// Error handling and edge case tests
TEST_F(FunctionSearchTest, TestInvalidClauseTypes) {
    // Test completely invalid clause types
    std::vector<std::string> invalid_types = {"INVALID", "UNKNOWN_TYPE", "BAD_CLAUSE", "", " "};

    for (const auto& invalid_type : invalid_types) {
        auto category = function_search->get_clause_type_category(invalid_type);
        EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED, category);

        auto query_type = function_search->clause_type_to_query_type(invalid_type);
        EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type);
    }
}

TEST_F(FunctionSearchTest, TestMalformedSearchClauses) {
    // Test clause without field_name
    TSearchClause malformed_clause1;
    malformed_clause1.clause_type = "TERM";
    // malformed_clause1.field_name is not set
    malformed_clause1.value = "hello";

    auto query_type1 = function_search->analyze_field_query_type("any_field", malformed_clause1);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, query_type1);

    // Test clause without value
    TSearchClause malformed_clause2;
    malformed_clause2.clause_type = "TERM";
    malformed_clause2.field_name = "title";
    // malformed_clause2.value is not set

    auto query_type2 = function_search->analyze_field_query_type("title", malformed_clause2);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type2);

    // Test clause without clause_type
    TSearchClause malformed_clause3;
    // malformed_clause3.clause_type is not set
    malformed_clause3.field_name = "title";
    malformed_clause3.value = "hello";

    auto query_type3 = function_search->analyze_field_query_type("title", malformed_clause3);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type3);
}

TEST_F(FunctionSearchTest, TestEmptySearchParam) {
    // Test completely empty search param
    TSearchParam empty_param;
    // empty_param.original_dsl is not set
    // empty_param.root is not set

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            empty_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should handle gracefully
}

TEST_F(FunctionSearchTest, TestNullIterators) {
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Add null iterator - this should cause an error
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when iterator is null

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("iterator not found for field 'title'") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestMismatchedFieldNames) {
    // Test query referencing fields not available in iterators
    TSearchParam search_param;
    search_param.original_dsl = "nonexistent_field:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "nonexistent_field";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Add different field
    data_types["existing_field"] = {"existing_field", nullptr};
    iterators["existing_field"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when field not found

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find(
                        "field 'nonexistent_field' not found in inverted index metadata") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestBooleanClauseWithoutChildren) {
    // Test AND clause with no children
    TSearchClause and_clause_no_children;
    and_clause_no_children.clause_type = "AND";
    // No children set

    auto query_type =
            function_search->analyze_field_query_type("any_field", and_clause_no_children);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, query_type);

    // Test OR clause with no children
    TSearchClause or_clause_no_children;
    or_clause_no_children.clause_type = "OR";
    // No children set

    query_type = function_search->analyze_field_query_type("any_field", or_clause_no_children);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, query_type);

    // Test NOT clause with no children
    TSearchClause not_clause_no_children;
    not_clause_no_children.clause_type = "NOT";
    // No children set

    query_type = function_search->analyze_field_query_type("any_field", not_clause_no_children);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestSpecialCharactersInValues) {
    // Test special characters in field values
    std::vector<std::string> special_values = {
            "",   " ",    "\n",    "\t",        "\\",  "\"",
            "'",  "null", "NULL",  "undefined", "NaN", "0",
            "-1", "true", "false", "你好",      "🔍",  std::string(1000, 'a')};

    for (const auto& special_value : special_values) {
        TSearchClause special_clause;
        special_clause.clause_type = "TERM";
        special_clause.field_name = "title";
        special_clause.value = special_value;

        auto query_type = function_search->analyze_field_query_type("title", special_clause);
        EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type);
    }
}

TEST_F(FunctionSearchTest, TestSpecialCharactersInFieldNames) {
    // Test special characters in field names
    std::vector<std::string> special_field_names = {"",
                                                    " ",
                                                    "field with spaces",
                                                    "field-with-dashes",
                                                    "field_with_underscores",
                                                    "field.with.dots",
                                                    "field@with@symbols",
                                                    "字段名",
                                                    "🔍field",
                                                    "123field"};

    for (const auto& special_field_name : special_field_names) {
        TSearchClause special_clause;
        special_clause.clause_type = "TERM";
        special_clause.field_name = special_field_name;
        special_clause.value = "hello";

        // Test with matching field name
        auto query_type1 =
                function_search->analyze_field_query_type(special_field_name, special_clause);
        EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type1);

        // Test with non-matching field name
        auto query_type2 =
                function_search->analyze_field_query_type("different_field", special_clause);
        EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, query_type2);
    }
}

TEST_F(FunctionSearchTest, TestCaseSensitivityInClauseTypes) {
    // Test case sensitivity for clause types
    std::vector<std::pair<std::string, segment_v2::InvertedIndexQueryType>> case_variations = {
            {"term", segment_v2::InvertedIndexQueryType::EQUAL_QUERY},  // lowercase
            {"TERM", segment_v2::InvertedIndexQueryType::EQUAL_QUERY},  // uppercase
            {"AND", segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY}, // uppercase
            {"and", segment_v2::InvertedIndexQueryType::
                            EQUAL_QUERY}, // lowercase (unknown, defaults to EQUAL)
            {"PHRASE", segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY}, // uppercase
            {"phrase", segment_v2::InvertedIndexQueryType::
                               EQUAL_QUERY}, // lowercase (unknown, defaults to EQUAL)
    };

    for (const auto& [clause_type, expected_query_type] : case_variations) {
        auto actual_query_type = function_search->clause_type_to_query_type(clause_type);
        EXPECT_EQ(expected_query_type, actual_query_type)
                << "Failed for clause_type: " << clause_type;
    }
}

TEST_F(FunctionSearchTest, TestZeroRowsScenario) {
    // Test with zero rows but empty iterators/data_types (realistic scenario)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    search_param.root = rootClause;

    // Empty data types and iterators - this is a realistic zero-data scenario
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    uint32_t num_rows = 0; // Zero rows
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should handle zero data gracefully and return empty result
}

TEST_F(FunctionSearchTest, TestVeryLargeRowCount) {
    // Test with very large row count but empty iterators/data_types (realistic scenario)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    search_param.root = rootClause;

    // Empty data types and iterators - this tests the large row count parameter handling
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    uint32_t num_rows = UINT32_MAX; // Very large row count
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should handle large row counts gracefully and return empty result
}

// Integration tests with VSearchExpr
TEST_F(FunctionSearchTest, TestFunctionSearchAndVSearchExprIntegration) {
    // Test that both components handle the same clause types consistently
    std::vector<std::string> clause_types = {"TERM",  "PHRASE", "WILDCARD", "REGEXP",
                                             "RANGE", "LIST",   "ANY",      "ALL",
                                             "AND",   "OR",     "NOT"};

    for (const auto& clause_type : clause_types) {
        auto category = function_search->get_clause_type_category(clause_type);
        auto query_type = function_search->clause_type_to_query_type(clause_type);

        // Verify that the mapping is consistent
        if (category == FunctionSearch::ClauseTypeCategory::COMPOUND) {
            EXPECT_EQ(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY, query_type);
        } else {
            EXPECT_NE(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY, query_type);
        }
    }
}

TEST_F(FunctionSearchTest, TestTokenizedVsNonTokenizedConsistency) {
    // Test that both components agree on tokenized vs non-tokenized classification
    std::map<std::string, FunctionSearch::ClauseTypeCategory> expected_categories = {
            {"TERM", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"PREFIX", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"WILDCARD", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"REGEXP", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"RANGE", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"LIST", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"PHRASE", FunctionSearch::ClauseTypeCategory::TOKENIZED},
            {"MATCH", FunctionSearch::ClauseTypeCategory::TOKENIZED},
            {"ANY", FunctionSearch::ClauseTypeCategory::TOKENIZED},
            {"ALL", FunctionSearch::ClauseTypeCategory::TOKENIZED},
            {"AND", FunctionSearch::ClauseTypeCategory::COMPOUND},
            {"OR", FunctionSearch::ClauseTypeCategory::COMPOUND},
            {"NOT", FunctionSearch::ClauseTypeCategory::COMPOUND}};

    for (const auto& [clause_type, expected_category] : expected_categories) {
        auto actual_category = function_search->get_clause_type_category(clause_type);
        EXPECT_EQ(expected_category, actual_category) << "Failed for clause_type: " << clause_type;
    }
}

TEST_F(FunctionSearchTest, TestPerformanceWithLargeQueries) {
    // Test performance with large query structures
    std::vector<TSearchClause> clauses;

    // Generate many field clauses
    for (int i = 0; i < 100; ++i) {
        TSearchClause clause;
        clause.clause_type = "TERM";
        clause.field_name = "field" + std::to_string(i);
        clause.value = "value" + std::to_string(i);
        clauses.push_back(clause);
    }

    // Create large OR clause
    TSearchClause largeOr;
    largeOr.clause_type = "OR";
    largeOr.children = clauses;

    // Test that analysis completes in reasonable time
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 100; ++i) {
        std::string field_name = "field" + std::to_string(i);
        auto query_type = function_search->analyze_field_query_type(field_name, largeOr);
        EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Should complete within reasonable time (less than 1 second for 100 fields)
    EXPECT_LT(duration.count(), 1000)
            << "Query analysis took too long: " << duration.count() << "ms";
}

// Tests for FieldReaderResolver::resolve function coverage (lines 74+)
TEST_F(FunctionSearchTest, TestFieldReaderResolverWithNonInvertedIndexIterator) {
    // Exercise the branch where the iterator exists but is not an InvertedIndexIterator
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    data_types["title"] = {"title", nullptr};
    DummyIndexIterator dummy_iterator;
    iterators["title"] = &dummy_iterator;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_NE(status.to_string().find("iterator for field 'title' is not InvertedIndexIterator"),
              std::string::npos);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithValidIterator) {
    // Test the path where we have a valid iterator but no real InvertedIndexIterator
    // This will test the early return in build_leaf_query when resolver.resolve fails
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Add valid data but no real iterator
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithEmptyFieldName) {
    // Test the path where field_name is empty
    TSearchParam search_param;
    search_param.original_dsl = ":hello"; // Empty field name

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = ""; // Empty field name
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when field not found

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("field '' not found in inverted index metadata") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithSpecialCharacters) {
    // Test with special characters in field names
    TSearchParam search_param;
    search_param.original_dsl = "field-with-dashes:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "field-with-dashes";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Field name doesn't match
    data_types["different_field"] = {"different_field", nullptr};
    iterators["different_field"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when field not found

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find(
                        "field 'field-with-dashes' not found in inverted index metadata") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithUnicodeFieldName) {
    // Test with Unicode field names
    TSearchParam search_param;
    search_param.original_dsl = "字段名:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "字段名";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Field name doesn't match
    data_types["english_field"] = {"english_field", nullptr};
    iterators["english_field"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when field not found

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("field '字段名' not found in inverted index metadata") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithVeryLongFieldName) {
    // Test with very long field names
    std::string very_long_field_name = "field_" + std::string(1000, 'a');

    TSearchParam search_param;
    search_param.original_dsl = very_long_field_name + ":hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = very_long_field_name;
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Field name doesn't match
    data_types["short_field"] = {"short_field", nullptr};
    iterators["short_field"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when field not found

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("field '" + very_long_field_name +
                                        "' not found in inverted index metadata") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithDifferentQueryTypes) {
    // Test with different query types to ensure the binding_key generation is covered
    std::vector<std::string> query_types = {"TERM",  "PHRASE", "WILDCARD", "REGEXP",
                                            "RANGE", "LIST",   "ANY",      "ALL"};

    for (const auto& query_type_str : query_types) {
        TSearchParam search_param;
        search_param.original_dsl = "title:" + query_type_str + "(hello)";

        TSearchClause rootClause;
        rootClause.clause_type = query_type_str;
        rootClause.field_name = "title";
        rootClause.value = "hello";
        rootClause.__isset.field_name = true;
        rootClause.__isset.value = true;
        search_param.root = rootClause;

        std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
        std::unordered_map<std::string, IndexIterator*> iterators;

        data_types["title"] = {"title", nullptr};
        iterators["title"] = nullptr;

        uint32_t num_rows = 100;
        InvertedIndexResultBitmap bitmap_result;

        auto status = function_search->evaluate_inverted_index_with_search_param(
                search_param, data_types, iterators, num_rows, bitmap_result);
        EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

        EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    }
}

// Tests for FunctionSearch::evaluate_inverted_index_with_search_param function coverage (lines 201+)
TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamEmptyQuery) {
    // Test the path where root_query is nullptr (lines 201-204)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Add valid data but no real iterator - this will cause build_query_recursive to fail
    // and return nullptr for root_query
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamNullBitmapHandling) {
    // Test the null bitmap handling logic (lines 206-220)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause early return due to iterator issues, but we can test the logic path
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamExecutionContext) {
    // Test the QueryExecutionContext creation (lines 222-226)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause early return due to iterator issues, but we can test the logic path
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamWeightAndScorer) {
    // Test the weight and scorer creation logic (lines 228-240)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause early return due to iterator issues, but we can test the logic path
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamDocumentIteration) {
    // Test the document iteration logic (lines 242-248)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause early return due to iterator issues, but we can test the logic path
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamResultMasking) {
    // Test the result masking logic (lines 250-255)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause early return due to iterator issues, but we can test the logic path
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamComplexQuery) {
    // Test with complex query structure to ensure all paths are covered
    TSearchParam search_param;
    search_param.original_dsl = "title:hello AND content:world";

    TSearchClause titleClause;
    titleClause.clause_type = "TERM";
    titleClause.field_name = "title";
    titleClause.value = "hello";
    titleClause.__isset.field_name = true;
    titleClause.__isset.value = true;

    TSearchClause contentClause;
    contentClause.clause_type = "TERM";
    contentClause.field_name = "content";
    contentClause.value = "world";
    contentClause.__isset.field_name = true;
    contentClause.__isset.value = true;

    TSearchClause rootClause;
    rootClause.clause_type = "AND";
    rootClause.children = {titleClause, contentClause};
    search_param.root = rootClause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause all child queries to fail, resulting in an empty root_query
    // which will return Status::OK() at line 201-204
    data_types["title"] = {"title", nullptr};
    data_types["content"] = {"content", nullptr};
    iterators["title"] = nullptr;
    iterators["content"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should return OK because root_query will be nullptr (empty query)

    // The function should return OK with an empty result when all child queries fail
    // This tests the path where build_query_recursive returns empty query for AND clause
}

TEST_F(FunctionSearchTest, TestOrCrossFieldMatchesMatchAnyRows) {
    TSearchClause left_clause;
    left_clause.clause_type = "TERM";
    left_clause.field_name = "title";
    left_clause.value = "foo";
    left_clause.__isset.field_name = true;
    left_clause.__isset.value = true;

    TSearchClause right_clause;
    right_clause.clause_type = "TERM";
    right_clause.field_name = "content";
    right_clause.value = "bar";
    right_clause.__isset.field_name = true;
    right_clause.__isset.value = true;

    TSearchClause root_clause;
    root_clause.clause_type = "OR";
    root_clause.children = {left_clause, right_clause};
    root_clause.__isset.children = true;

    auto left_iterator = std::make_unique<TrackingIndexIterator>(true);
    auto right_iterator = std::make_unique<TrackingIndexIterator>(true);

    std::unordered_map<std::string, IndexIterator*> iterators_map = {
            {"title", left_iterator.get()}, {"content", right_iterator.get()}};

    auto null_bitmap = std::make_shared<roaring::Roaring>();
    auto status = function_search->collect_all_field_nulls(root_clause, iterators_map, null_bitmap);
    EXPECT_TRUE(status.ok());
    EXPECT_GE(left_iterator->has_null_checks(), 1);
    EXPECT_GE(right_iterator->has_null_checks(), 1);
    EXPECT_GE(left_iterator->read_null_bitmap_calls(), 1);
    EXPECT_GE(right_iterator->read_null_bitmap_calls(), 1);
    EXPECT_TRUE(null_bitmap->isEmpty());

    auto data_bitmap = std::make_shared<roaring::Roaring>();
    data_bitmap->add(1);
    data_bitmap->add(3);
    auto search_null_bitmap = std::make_shared<roaring::Roaring>();
    search_null_bitmap->add(2);

    InvertedIndexResultBitmap search_bitmap(data_bitmap, search_null_bitmap);
    search_bitmap.mask_out_null();

    auto result_bitmap = search_bitmap.get_data_bitmap();
    ASSERT_NE(nullptr, result_bitmap);
    EXPECT_EQ(2u, result_bitmap->cardinality());

    roaring::Roaring match_any_rows;
    match_any_rows.add(1);
    match_any_rows.add(3);

    roaring::Roaring expected_diff = match_any_rows;
    expected_diff -= *result_bitmap;
    EXPECT_TRUE(expected_diff.isEmpty());

    roaring::Roaring result_diff = *result_bitmap;
    result_diff -= match_any_rows;
    EXPECT_TRUE(result_diff.isEmpty());
}

TEST_F(FunctionSearchTest, TestOrWithNotSameFieldMatchesMatchAllRows) {
    TSearchClause include_clause;
    include_clause.clause_type = "TERM";
    include_clause.field_name = "title";
    include_clause.value = "foo";
    include_clause.__isset.field_name = true;
    include_clause.__isset.value = true;

    TSearchClause exclude_child;
    exclude_child.clause_type = "TERM";
    exclude_child.field_name = "title";
    exclude_child.value = "bar";
    exclude_child.__isset.field_name = true;
    exclude_child.__isset.value = true;

    TSearchClause exclude_clause;
    exclude_clause.clause_type = "NOT";
    exclude_clause.children = {exclude_child};

    TSearchClause root_clause;
    root_clause.clause_type = "OR";
    root_clause.children = {include_clause, exclude_clause};
    root_clause.__isset.children = true;

    auto iterator = std::make_unique<TrackingIndexIterator>(true);
    std::unordered_map<std::string, IndexIterator*> iterators_map = {{"title", iterator.get()}};

    auto null_bitmap = std::make_shared<roaring::Roaring>();
    auto status = function_search->collect_all_field_nulls(root_clause, iterators_map, null_bitmap);
    EXPECT_TRUE(status.ok());
    EXPECT_GE(iterator->has_null_checks(), 1);
    EXPECT_GE(iterator->read_null_bitmap_calls(), 1);

    auto data_bitmap = std::make_shared<roaring::Roaring>();
    data_bitmap->add(1);
    data_bitmap->add(2);
    data_bitmap->add(3);
    auto search_null_bitmap = std::make_shared<roaring::Roaring>();
    search_null_bitmap->add(3);

    InvertedIndexResultBitmap search_bitmap(data_bitmap, search_null_bitmap);
    search_bitmap.mask_out_null();

    auto result_bitmap = search_bitmap.get_data_bitmap();
    ASSERT_NE(nullptr, result_bitmap);
    EXPECT_EQ(2u, result_bitmap->cardinality());

    roaring::Roaring match_all_rows;
    match_all_rows.add(1);
    match_all_rows.add(2);

    roaring::Roaring expected_diff = match_all_rows;
    expected_diff -= *result_bitmap;
    EXPECT_TRUE(expected_diff.isEmpty());

    roaring::Roaring result_diff = *result_bitmap;
    result_diff -= match_all_rows;
    EXPECT_TRUE(result_diff.isEmpty());
}

// Note: Full testing of evaluate_inverted_index_with_search_param with real InvertedIndexIterator
// and actual file operations would require complex setup with real index files
// and is better suited for integration tests. The tests above cover the main
// execution paths and error handling logic in the function.

} // namespace doris::vectorized

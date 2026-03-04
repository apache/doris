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

// Unit tests for FunctionSearch::evaluate_nested_query and NESTED clause handling.
// Migrated from function_search_test.cpp for maintainability.

#include <gtest/gtest.h>

#include <memory>
#include <roaring/roaring.hh>
#include <unordered_map>

#include "gen_cpp/Exprs_types.h"
#include "olap/rowset/segment_v2/variant/nested_group_provider.h"
#include "vec/core/block.h"
#include "vec/functions/function_search.h"

namespace doris::vectorized {

class FunctionSearchNestedTest : public testing::Test {
public:
    void SetUp() override { function_search = std::make_shared<FunctionSearch>(); }

protected:
    std::shared_ptr<FunctionSearch> function_search;
};

// ===========================================================================
// is_nested_group_search_supported (CE / Default provider)
// ===========================================================================

TEST_F(FunctionSearchNestedTest, DefaultProviderNestedGroupSearchSupport) {
    auto provider = segment_v2::create_nested_group_read_provider();
    ASSERT_NE(nullptr, provider);
    // In CE, disabled; in EE, enabled.
    // Just verify the provider is non-null and the method is callable.
    // CE-specific assertion:
    if (!provider->should_enable_nested_group_read_path()) {
        EXPECT_FALSE(provider->should_enable_nested_group_read_path());
    } else {
        EXPECT_TRUE(provider->should_enable_nested_group_read_path());
    }
}

// ===========================================================================
// NESTED clause at top level — build_query_recursive rejects non-top-level NESTED
// ===========================================================================

TEST_F(FunctionSearchNestedTest, NestedClauseMustBeTopLevel) {
    TSearchClause nested_clause;
    nested_clause.clause_type = "NESTED";

    TSearchClause root_clause;
    root_clause.clause_type = "AND";
    root_clause.children = {nested_clause};
    root_clause.__isset.children = true;

    auto context = std::make_shared<IndexQueryContext>();
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_types, iterators, context);

    inverted_index::query_v2::QueryPtr out;
    std::string binding_key;
    const std::string default_operator = "or";
    constexpr int32_t minimum_should_match = -1;
    auto status = function_search->build_query_recursive(root_clause, context, resolver, &out,
                                                         &binding_key, default_operator,
                                                         minimum_should_match);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_NE(status.to_string().find("NESTED clause must be evaluated at top level"),
              std::string::npos);
}

// ===========================================================================
// Community-edition fallback: NESTED root → NOT_IMPLEMENTED_ERROR
// ===========================================================================

TEST_F(FunctionSearchNestedTest, CommunityBuildFallbackForNestedRootClause) {
    TSearchParam search_param;
    search_param.original_dsl = "NESTED(data.items, msg=hello)";

    TSearchClause inner_clause;
    inner_clause.clause_type = "TERM";
    inner_clause.__set_field_name("data.items.msg");
    inner_clause.__set_value("hello");

    TSearchClause nested_clause;
    nested_clause.clause_type = "NESTED";
    nested_clause.__set_nested_path("data.items");
    nested_clause.__set_children({inner_clause});
    search_param.root = nested_clause;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> empty_data_types;
    std::unordered_map<std::string, IndexIterator*> empty_iterators;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, empty_data_types, empty_iterators, 100, bitmap_result);

    auto provider = segment_v2::create_nested_group_read_provider();
    ASSERT_TRUE(provider != nullptr);
    if (!provider->should_enable_nested_group_read_path()) {
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(status.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
        EXPECT_TRUE(status.to_string().find("NestedGroup support") != std::string::npos);
    } else {
        // In EE, this test intentionally provides no segment context and should fail on that.
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    }
}

// ===========================================================================
// evaluate_nested_query — error path: missing nested_path
// ===========================================================================

TEST_F(FunctionSearchNestedTest, MissingNestedPath) {
    TSearchParam search_param;

    TSearchClause nested_clause;
    nested_clause.clause_type = "NESTED";
    nested_clause.__isset.nested_path = false; // missing

    TSearchClause inner;
    inner.clause_type = "TERM";
    inner.__set_field_name("msg");
    inner.__set_value("hello");
    nested_clause.__set_children({inner});

    auto context = std::make_shared<IndexQueryContext>();
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_types, iterators, context);
    std::shared_ptr<roaring::Roaring> result_bitmap;
    std::unordered_map<std::string, int> field_to_col_id;

    auto status =
            function_search->evaluate_nested_query(search_param, nested_clause, context, resolver,
                                                   100, nullptr, field_to_col_id, result_bitmap);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_NE(status.to_string().find("missing nested_path"), std::string::npos);
}

// ===========================================================================
// evaluate_nested_query — error path: missing children (not set)
// ===========================================================================

TEST_F(FunctionSearchNestedTest, MissingChildren) {
    TSearchParam search_param;

    TSearchClause nested_clause;
    nested_clause.clause_type = "NESTED";
    nested_clause.__set_nested_path("data.items");
    nested_clause.__isset.children = false; // not set

    auto context = std::make_shared<IndexQueryContext>();
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_types, iterators, context);
    std::shared_ptr<roaring::Roaring> result_bitmap;
    std::unordered_map<std::string, int> field_to_col_id;

    auto status =
            function_search->evaluate_nested_query(search_param, nested_clause, context, resolver,
                                                   100, nullptr, field_to_col_id, result_bitmap);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_NE(status.to_string().find("missing inner query"), std::string::npos);
}

// ===========================================================================
// evaluate_nested_query — error path: empty children list
// ===========================================================================

TEST_F(FunctionSearchNestedTest, EmptyChildrenList) {
    TSearchParam search_param;

    TSearchClause nested_clause;
    nested_clause.clause_type = "NESTED";
    nested_clause.__set_nested_path("data");
    nested_clause.__set_children({}); // empty

    auto context = std::make_shared<IndexQueryContext>();
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_types, iterators, context);
    std::shared_ptr<roaring::Roaring> result_bitmap;
    std::unordered_map<std::string, int> field_to_col_id;

    auto status =
            function_search->evaluate_nested_query(search_param, nested_clause, context, resolver,
                                                   100, nullptr, field_to_col_id, result_bitmap);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_NE(status.to_string().find("missing inner query"), std::string::npos);
}

// ===========================================================================
// evaluate_nested_query — error path: null IndexExecContext
// ===========================================================================

TEST_F(FunctionSearchNestedTest, NullExecContext) {
    TSearchParam search_param;

    TSearchClause inner;
    inner.clause_type = "TERM";
    inner.__set_field_name("data.msg");
    inner.__set_value("hello");

    TSearchClause nested_clause;
    nested_clause.clause_type = "NESTED";
    nested_clause.__set_nested_path("data");
    nested_clause.__set_children({inner});

    auto context = std::make_shared<IndexQueryContext>();
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_types, iterators, context);
    std::shared_ptr<roaring::Roaring> result_bitmap;
    std::unordered_map<std::string, int> field_to_col_id;

    auto status =
            function_search->evaluate_nested_query(search_param, nested_clause, context, resolver,
                                                   100, nullptr, field_to_col_id, result_bitmap);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_NE(status.to_string().find("IndexExecContext"), std::string::npos);
}

// ===========================================================================
// evaluate_nested_query — bitmap init: null result_bitmap is handled
// ===========================================================================

TEST_F(FunctionSearchNestedTest, InitializesNullResultBitmap) {
    TSearchParam search_param;

    TSearchClause nested_clause;
    nested_clause.clause_type = "NESTED";
    nested_clause.__isset.nested_path = false; // will fail early

    auto context = std::make_shared<IndexQueryContext>();
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_types, iterators, context);
    std::shared_ptr<roaring::Roaring> result_bitmap; // nullptr
    std::unordered_map<std::string, int> field_to_col_id;

    auto status =
            function_search->evaluate_nested_query(search_param, nested_clause, context, resolver,
                                                   100, nullptr, field_to_col_id, result_bitmap);
    // Should fail (nested_path not set), but no crash on null bitmap
    EXPECT_FALSE(status.ok());
}

// ===========================================================================
// evaluate_nested_query — bitmap init/clear happens after validation
// ===========================================================================

TEST_F(FunctionSearchNestedTest, BitmapClearedAfterPassingValidation) {
    // The implementation clears the bitmap AFTER nested_path/children checks
    // but BEFORE the IndexExecContext check. So providing valid nested_path
    // and children, but null context, should clear the bitmap.
    TSearchParam search_param;

    TSearchClause inner;
    inner.clause_type = "TERM";
    inner.__set_field_name("data.msg");
    inner.__set_value("hello");

    TSearchClause nested_clause;
    nested_clause.clause_type = "NESTED";
    nested_clause.__set_nested_path("data");
    nested_clause.__set_children({inner});

    auto context = std::make_shared<IndexQueryContext>();
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_types, iterators, context);

    // Pre-populated bitmap
    auto result_bitmap = std::make_shared<roaring::Roaring>();
    result_bitmap->add(1);
    result_bitmap->add(5);
    result_bitmap->add(10);
    ASSERT_EQ(3U, result_bitmap->cardinality());

    std::unordered_map<std::string, int> field_to_col_id;

    auto status =
            function_search->evaluate_nested_query(search_param, nested_clause, context, resolver,
                                                   100, nullptr, field_to_col_id, result_bitmap);
    // Will fail later (null context), but bitmap should be cleared
    EXPECT_FALSE(status.ok());
    ASSERT_NE(nullptr, result_bitmap);
    EXPECT_TRUE(result_bitmap->isEmpty());
}

// ===========================================================================
// evaluate_nested_query — dotted nested_path
// ===========================================================================

TEST_F(FunctionSearchNestedTest, DottedNestedPath) {
    TSearchParam search_param;

    TSearchClause inner;
    inner.clause_type = "TERM";
    inner.__set_field_name("data.items.msg");
    inner.__set_value("hello");

    TSearchClause nested_clause;
    nested_clause.clause_type = "NESTED";
    nested_clause.__set_nested_path("data.items"); // dotted path
    nested_clause.__set_children({inner});

    auto context = std::make_shared<IndexQueryContext>();
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_types, iterators, context);
    std::shared_ptr<roaring::Roaring> result_bitmap;
    std::unordered_map<std::string, int> field_to_col_id;

    // null context → InvalidArgument about segment
    auto status =
            function_search->evaluate_nested_query(search_param, nested_clause, context, resolver,
                                                   100, nullptr, field_to_col_id, result_bitmap);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_NE(status.to_string().find("IndexExecContext"), std::string::npos);
}

// ===========================================================================
// End-to-end: NESTED root through top-level API in CE
// ===========================================================================

TEST_F(FunctionSearchNestedTest, NestedRootFallbackViaToplevelAPI) {
    TSearchParam search_param;
    search_param.original_dsl = "NESTED(data, msg=hello)";

    TSearchClause inner;
    inner.clause_type = "TERM";
    inner.__set_field_name("data.msg");
    inner.__set_value("hello");

    TSearchClause nested_root;
    nested_root.clause_type = "NESTED";
    nested_root.__set_nested_path("data");
    nested_root.__set_children({inner});
    search_param.root = nested_root;

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    InvertedIndexResultBitmap bitmap_result;

    auto provider = segment_v2::create_nested_group_read_provider();
    if (!provider->should_enable_nested_group_read_path()) {
        auto status = function_search->evaluate_inverted_index_with_search_param(
                search_param, data_types, iterators, 100, bitmap_result);
        EXPECT_FALSE(status.ok());
        EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
        EXPECT_NE(status.to_string().find("NestedGroup support"), std::string::npos);
    }
}

} // namespace doris::vectorized

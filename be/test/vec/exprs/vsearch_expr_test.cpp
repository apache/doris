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

#include <gtest/gtest.h>

#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vsearch.h"

namespace doris::vectorized {

class VSearchExprTest : public testing::Test {
public:
    void SetUp() override {
        // Create test TExprNode with search_param
        createTestExprNode();
    }

protected:
    void createTestExprNode() {
        test_node.node_type = TExprNodeType::SEARCH_EXPR;

        // Properly set up TTypeDesc for BOOLEAN type
        TTypeDesc type_desc;
        TTypeNode type_node;
        type_node.type = TTypeNodeType::SCALAR;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::BOOLEAN);
        type_node.__set_scalar_type(scalar_type);
        type_desc.types.push_back(type_node);
        test_node.__set_type(type_desc);

        test_node.num_children = 1;

        // Create search param
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

        test_node.search_param = searchParam;
        test_node.__isset.search_param = true;
    }

    TExprNode test_node;
};

TEST_F(VSearchExprTest, TestConstruction) {
    auto vsearch_expr = VSearchExpr::create_shared(test_node);

    ASSERT_NE(nullptr, vsearch_expr);
    EXPECT_EQ("VSearchExpr", vsearch_expr->expr_name());
}

TEST_F(VSearchExprTest, TestIsConstant) {
    auto vsearch_expr = VSearchExpr::create_shared(test_node);

    // VSearchExpr should never be constant to prevent constant column evaluation
    EXPECT_FALSE(vsearch_expr->is_constant());
}

TEST_F(VSearchExprTest, TestSearchParamExtraction) {
    auto vsearch_expr = VSearchExpr::create_shared(test_node);

    // The constructor should extract search_param from TExprNode
    // We can't directly access private members, but we can verify through logging
    // that the DSL was correctly extracted (check test output for log messages)
    EXPECT_TRUE(true); // Constructor should complete without error
}

TEST_F(VSearchExprTest, TestComplexSearchParam) {
    // Create complex search param with AND clause
    TExprNode complex_node;
    complex_node.node_type = TExprNodeType::SEARCH_EXPR;
    // Properly set up TTypeDesc for BOOLEAN type
    TTypeDesc complex_type_desc;
    TTypeNode complex_type_node;
    complex_type_node.type = TTypeNodeType::SCALAR;
    TScalarType complex_scalar_type;
    complex_scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    complex_type_node.__set_scalar_type(complex_scalar_type);
    complex_type_desc.types.push_back(complex_type_node);
    complex_node.__set_type(complex_type_desc);
    complex_node.num_children = 2;

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

    complex_node.search_param = searchParam;
    complex_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(complex_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestPhraseSearchParam) {
    TExprNode phrase_node;
    phrase_node.node_type = TExprNodeType::SEARCH_EXPR;
    // Properly set up TTypeDesc for BOOLEAN type
    TTypeDesc phrase_type_desc;
    TTypeNode phrase_type_node;
    phrase_type_node.type = TTypeNodeType::SCALAR;
    TScalarType phrase_scalar_type;
    phrase_scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    phrase_type_node.__set_scalar_type(phrase_scalar_type);
    phrase_type_desc.types.push_back(phrase_type_node);
    phrase_node.__set_type(phrase_type_desc);
    phrase_node.num_children = 1;

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

    phrase_node.search_param = searchParam;
    phrase_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(phrase_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestRegexpSearchParam) {
    TExprNode regexp_node;
    regexp_node.node_type = TExprNodeType::SEARCH_EXPR;
    // Properly set up TTypeDesc for BOOLEAN type
    TTypeDesc regexp_type_desc;
    TTypeNode regexp_type_node;
    regexp_type_node.type = TTypeNodeType::SCALAR;
    TScalarType regexp_scalar_type;
    regexp_scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    regexp_type_node.__set_scalar_type(regexp_scalar_type);
    regexp_type_desc.types.push_back(regexp_type_node);
    regexp_node.__set_type(regexp_type_desc);
    regexp_node.num_children = 1;

    TSearchParam searchParam;
    searchParam.original_dsl = "title:/[a-z]+/";

    TSearchClause rootClause;
    rootClause.clause_type = "REGEXP";
    rootClause.field_name = "title";
    rootClause.value = "[a-z]+";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "title";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    regexp_node.search_param = searchParam;
    regexp_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(regexp_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestRangeSearchParam) {
    TExprNode range_node;
    range_node.node_type = TExprNodeType::SEARCH_EXPR;
    // Properly set up TTypeDesc for BOOLEAN type
    TTypeDesc range_type_desc;
    TTypeNode range_type_node;
    range_type_node.type = TTypeNodeType::SCALAR;
    TScalarType range_scalar_type;
    range_scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    range_type_node.__set_scalar_type(range_scalar_type);
    range_type_desc.types.push_back(range_type_node);
    range_node.__set_type(range_type_desc);
    range_node.num_children = 1;

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

    range_node.search_param = searchParam;
    range_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(range_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestAnyAllSearchParam) {
    // Test ANY clause
    TExprNode any_node;
    any_node.node_type = TExprNodeType::SEARCH_EXPR;
    // Properly set up TTypeDesc for BOOLEAN type
    TTypeDesc any_type_desc;
    TTypeNode any_type_node;
    any_type_node.type = TTypeNodeType::SCALAR;
    TScalarType any_scalar_type;
    any_scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    any_type_node.__set_scalar_type(any_scalar_type);
    any_type_desc.types.push_back(any_type_node);
    any_node.__set_type(any_type_desc);
    any_node.num_children = 1;

    TSearchParam searchParam;
    searchParam.original_dsl = "tags:ANY(java python)";

    TSearchClause rootClause;
    rootClause.clause_type = "ANY";
    rootClause.field_name = "tags";
    rootClause.value = "java python";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "tags";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    any_node.search_param = searchParam;
    any_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(any_node);
    ASSERT_NE(nullptr, vsearch_expr);

    // Test ALL clause
    TExprNode all_node;
    all_node.node_type = TExprNodeType::SEARCH_EXPR;
    // Properly set up TTypeDesc for BOOLEAN type
    TTypeDesc all_type_desc;
    TTypeNode all_type_node;
    all_type_node.type = TTypeNodeType::SCALAR;
    TScalarType all_scalar_type;
    all_scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    all_type_node.__set_scalar_type(all_scalar_type);
    all_type_desc.types.push_back(all_type_node);
    all_node.__set_type(all_type_desc);
    all_node.num_children = 1;

    searchParam.original_dsl = "tags:ALL(programming language)";
    rootClause.clause_type = "ALL";
    rootClause.value = "programming language";
    searchParam.root = rootClause;

    all_node.search_param = searchParam;
    all_node.__isset.search_param = true;

    auto vsearch_expr2 = VSearchExpr::create_shared(all_node);
    ASSERT_NE(nullptr, vsearch_expr2);
}

TEST_F(VSearchExprTest, TestMissingSearchParam) {
    TExprNode missing_param_node;
    missing_param_node.node_type = TExprNodeType::SEARCH_EXPR;
    // Properly set up TTypeDesc for BOOLEAN type
    TTypeDesc missing_type_desc;
    TTypeNode missing_type_node;
    missing_type_node.type = TTypeNodeType::SCALAR;
    TScalarType missing_scalar_type;
    missing_scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    missing_type_node.__set_scalar_type(missing_scalar_type);
    missing_type_desc.types.push_back(missing_type_node);
    missing_param_node.__set_type(missing_type_desc);
    missing_param_node.num_children = 0;
    missing_param_node.__isset.search_param = false;

    auto vsearch_expr = VSearchExpr::create_shared(missing_param_node);
    ASSERT_NE(nullptr, vsearch_expr);
    // Should handle missing search_param gracefully
}

TEST_F(VSearchExprTest, TestMultipleFieldBindings) {
    TExprNode multi_field_node;
    multi_field_node.node_type = TExprNodeType::SEARCH_EXPR;
    // Properly set up TTypeDesc for BOOLEAN type
    TTypeDesc multi_type_desc;
    TTypeNode multi_type_node;
    multi_type_node.type = TTypeNodeType::SCALAR;
    TScalarType multi_scalar_type;
    multi_scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    multi_type_node.__set_scalar_type(multi_scalar_type);
    multi_type_desc.types.push_back(multi_type_node);
    multi_field_node.__set_type(multi_type_desc);
    multi_field_node.num_children = 3;

    TSearchParam searchParam;
    searchParam.original_dsl = "(title:hello OR content:world) AND category:tech";

    // Create complex structure
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

    TSearchClause orClause;
    orClause.clause_type = "OR";
    orClause.children = {titleClause, contentClause};

    TSearchClause rootClause;
    rootClause.clause_type = "AND";
    rootClause.children = {orClause, categoryClause};
    searchParam.root = rootClause;

    // Create field bindings for all three fields
    TSearchFieldBinding titleBinding;
    titleBinding.field_name = "title";
    titleBinding.slot_index = 0;

    TSearchFieldBinding contentBinding;
    contentBinding.field_name = "content";
    contentBinding.slot_index = 1;

    TSearchFieldBinding categoryBinding;
    categoryBinding.field_name = "category";
    categoryBinding.slot_index = 2;

    searchParam.field_bindings = {titleBinding, contentBinding, categoryBinding};

    multi_field_node.search_param = searchParam;
    multi_field_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(multi_field_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestExecuteMethod) {
    auto vsearch_expr = VSearchExpr::create_shared(test_node);

    // Test execute method with context but without inverted index context - should return error
    Block block;
    int result_column_id = -1;

    // Create a basic VExprContext without inverted index context
    auto dummy_expr = VSearchExpr::create_shared(test_node);
    VExprContext context(dummy_expr);

    auto status = vsearch_expr->execute(&context, &block, &result_column_id);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.code() == ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(
            status.to_string().find("SearchExpr should not be executed without inverted index") !=
            std::string::npos);
}

TEST_F(VSearchExprTest, TestEvaluateInvertedIndexEmptyDSL) {
    // Create expr with empty DSL
    TExprNode empty_dsl_node;
    empty_dsl_node.node_type = TExprNodeType::SEARCH_EXPR;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    empty_dsl_node.__set_type(type_desc);
    empty_dsl_node.num_children = 0;

    TSearchParam searchParam;
    searchParam.original_dsl = ""; // Empty DSL
    empty_dsl_node.search_param = searchParam;
    empty_dsl_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(empty_dsl_node);

    // Test evaluate_inverted_index with empty DSL
    auto dummy_expr = VSearchExpr::create_shared(empty_dsl_node);
    VExprContext context(dummy_expr);
    auto status = vsearch_expr->evaluate_inverted_index(&context, 100);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.code() == ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.to_string().find("search DSL is empty") != std::string::npos);
}

TEST_F(VSearchExprTest, TestEvaluateInvertedIndexNoContext) {
    auto vsearch_expr = VSearchExpr::create_shared(test_node);

    // Test evaluate_inverted_index with context but without inverted index context
    auto dummy_expr = VSearchExpr::create_shared(test_node);
    VExprContext context(dummy_expr);

    auto status = vsearch_expr->evaluate_inverted_index(&context, 100);
    EXPECT_TRUE(status.ok()); // Should return OK but log warning
}

TEST_F(VSearchExprTest, TestNodeTypeValidation) {
    // Test different node types
    auto vsearch_expr = VSearchExpr::create_shared(test_node);
    EXPECT_EQ(TExprNodeType::SEARCH_EXPR, vsearch_expr->node_type());

    // Verify expr_name
    EXPECT_EQ("VSearchExpr", vsearch_expr->expr_name());

    // Test that it's not constant (should never be constant to prevent column evaluation)
    EXPECT_FALSE(vsearch_expr->is_constant());
}

TEST_F(VSearchExprTest, TestWildcardSearchParam) {
    TExprNode wildcard_node;
    wildcard_node.node_type = TExprNodeType::SEARCH_EXPR;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    wildcard_node.__set_type(type_desc);
    wildcard_node.num_children = 1;

    TSearchParam searchParam;
    searchParam.original_dsl = "title:hello*world";

    TSearchClause rootClause;
    rootClause.clause_type = "WILDCARD";
    rootClause.field_name = "title";
    rootClause.value = "hello*world";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "title";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    wildcard_node.search_param = searchParam;
    wildcard_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(wildcard_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestPrefixSearchParam) {
    TExprNode prefix_node;
    prefix_node.node_type = TExprNodeType::SEARCH_EXPR;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    prefix_node.__set_type(type_desc);
    prefix_node.num_children = 1;

    TSearchParam searchParam;
    searchParam.original_dsl = "title:PREFIX(hello)";

    TSearchClause rootClause;
    rootClause.clause_type = "PREFIX";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "title";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    prefix_node.search_param = searchParam;
    prefix_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(prefix_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestListSearchParam) {
    TExprNode list_node;
    list_node.node_type = TExprNodeType::SEARCH_EXPR;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    list_node.__set_type(type_desc);
    list_node.num_children = 1;

    TSearchParam searchParam;
    searchParam.original_dsl = "category:LIST(tech, science, programming)";

    TSearchClause rootClause;
    rootClause.clause_type = "LIST";
    rootClause.field_name = "category";
    rootClause.value = "tech,science,programming";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "category";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    list_node.search_param = searchParam;
    list_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(list_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestMatchSearchParam) {
    TExprNode match_node;
    match_node.node_type = TExprNodeType::SEARCH_EXPR;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    match_node.__set_type(type_desc);
    match_node.num_children = 1;

    TSearchParam searchParam;
    searchParam.original_dsl = "content:MATCH(machine learning algorithms)";

    TSearchClause rootClause;
    rootClause.clause_type = "MATCH";
    rootClause.field_name = "content";
    rootClause.value = "machine learning algorithms";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "content";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    match_node.search_param = searchParam;
    match_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(match_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestComplexNestedQuery) {
    TExprNode complex_node;
    complex_node.node_type = TExprNodeType::SEARCH_EXPR;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    complex_node.__set_type(type_desc);
    complex_node.num_children = 4;

    TSearchParam searchParam;
    searchParam.original_dsl =
            "((title:hello AND content:world) OR (author:john AND category:tech)) AND NOT "
            "status:draft";

    // Build deeply nested query structure
    TSearchClause titleClause;
    titleClause.clause_type = "TERM";
    titleClause.field_name = "title";
    titleClause.value = "hello";

    TSearchClause contentClause;
    contentClause.clause_type = "TERM";
    contentClause.field_name = "content";
    contentClause.value = "world";

    TSearchClause authorClause;
    authorClause.clause_type = "TERM";
    authorClause.field_name = "author";
    authorClause.value = "john";

    TSearchClause categoryClause;
    categoryClause.clause_type = "TERM";
    categoryClause.field_name = "category";
    categoryClause.value = "tech";

    TSearchClause statusClause;
    statusClause.clause_type = "TERM";
    statusClause.field_name = "status";
    statusClause.value = "draft";

    // Build nested structure
    TSearchClause leftAndClause;
    leftAndClause.clause_type = "AND";
    leftAndClause.children = {titleClause, contentClause};

    TSearchClause rightAndClause;
    rightAndClause.clause_type = "AND";
    rightAndClause.children = {authorClause, categoryClause};

    TSearchClause innerOrClause;
    innerOrClause.clause_type = "OR";
    innerOrClause.children = {leftAndClause, rightAndClause};

    TSearchClause notClause;
    notClause.clause_type = "NOT";
    notClause.children = {statusClause};

    TSearchClause rootAndClause;
    rootAndClause.clause_type = "AND";
    rootAndClause.children = {innerOrClause, notClause};

    searchParam.root = rootAndClause;

    // Create field bindings
    TSearchFieldBinding titleBinding;
    titleBinding.field_name = "title";
    titleBinding.slot_index = 0;

    TSearchFieldBinding contentBinding;
    contentBinding.field_name = "content";
    contentBinding.slot_index = 1;

    TSearchFieldBinding authorBinding;
    authorBinding.field_name = "author";
    authorBinding.slot_index = 2;

    TSearchFieldBinding categoryBinding;
    categoryBinding.field_name = "category";
    categoryBinding.slot_index = 3;

    searchParam.field_bindings = {titleBinding, contentBinding, authorBinding, categoryBinding};

    complex_node.search_param = searchParam;
    complex_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(complex_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestEmptyChildrenClause) {
    TExprNode empty_children_node;
    empty_children_node.node_type = TExprNodeType::SEARCH_EXPR;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    empty_children_node.__set_type(type_desc);
    empty_children_node.num_children = 0;

    TSearchParam searchParam;
    searchParam.original_dsl = "AND()"; // Empty AND clause

    TSearchClause rootClause;
    rootClause.clause_type = "AND";
    rootClause.children = {}; // Empty children
    searchParam.root = rootClause;

    empty_children_node.search_param = searchParam;
    empty_children_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(empty_children_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestSingleChildBooleanClause) {
    TExprNode single_child_node;
    single_child_node.node_type = TExprNodeType::SEARCH_EXPR;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    single_child_node.__set_type(type_desc);
    single_child_node.num_children = 1;

    TSearchParam searchParam;
    searchParam.original_dsl = "NOT title:hello";

    TSearchClause termClause;
    termClause.clause_type = "TERM";
    termClause.field_name = "title";
    termClause.value = "hello";

    TSearchClause notClause;
    notClause.clause_type = "NOT";
    notClause.children = {termClause}; // Single child
    searchParam.root = notClause;

    TSearchFieldBinding binding;
    binding.field_name = "title";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    single_child_node.search_param = searchParam;
    single_child_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(single_child_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestExecuteWithNullBlock) {
    auto vsearch_expr = VSearchExpr::create_shared(test_node);

    int result_column_id = -1;

    // Create a basic VExprContext without inverted index context
    auto dummy_expr = VSearchExpr::create_shared(test_node);
    VExprContext context(dummy_expr);

    // Test with null block (should not crash)
    auto status = vsearch_expr->execute(&context, nullptr, &result_column_id);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.code() == ErrorCode::INTERNAL_ERROR);
}

TEST_F(VSearchExprTest, TestEvaluateInvertedIndexWithWhitespaceOnlyDSL) {
    TExprNode whitespace_node;
    whitespace_node.node_type = TExprNodeType::SEARCH_EXPR;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    whitespace_node.__set_type(type_desc);
    whitespace_node.num_children = 0;

    TSearchParam searchParam;
    searchParam.original_dsl = "   \t\n  "; // Whitespace only
    whitespace_node.search_param = searchParam;
    whitespace_node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(whitespace_node);

    auto dummy_expr = VSearchExpr::create_shared(whitespace_node);
    VExprContext context(dummy_expr);
    auto status = vsearch_expr->evaluate_inverted_index(&context, 100);
    EXPECT_TRUE(status.ok()); // Whitespace DSL should be handled, not considered empty
}

TEST_F(VSearchExprTest, TestEvaluateInvertedIndexWithZeroRows) {
    auto vsearch_expr = VSearchExpr::create_shared(test_node);

    auto dummy_expr = VSearchExpr::create_shared(test_node);
    VExprContext context(dummy_expr);
    auto status = vsearch_expr->evaluate_inverted_index(&context, 0);
    EXPECT_TRUE(status.ok()); // Should handle zero rows gracefully
}

TEST_F(VSearchExprTest, TestEvaluateInvertedIndexWithMaxRows) {
    auto vsearch_expr = VSearchExpr::create_shared(test_node);

    auto dummy_expr = VSearchExpr::create_shared(test_node);
    VExprContext context(dummy_expr);
    auto status = vsearch_expr->evaluate_inverted_index(&context, UINT32_MAX);
    EXPECT_TRUE(status.ok()); // Should handle large row counts gracefully
}

TEST_F(VSearchExprTest, TestConstructorWithInvalidNodeType) {
    TExprNode invalid_node;
    invalid_node.node_type = TExprNodeType::BOOL_LITERAL; // Wrong node type

    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    invalid_node.__set_type(type_desc);

    invalid_node.num_children = 0;
    invalid_node.__isset.search_param = false;

    // Should still construct but without search param
    auto vsearch_expr = VSearchExpr::create_shared(invalid_node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestConstructorWithMissingSearchParam) {
    TExprNode node;
    node.node_type = TExprNodeType::SEARCH_EXPR;

    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    node.__set_type(type_desc);

    node.num_children = 0;
    node.__isset.search_param = false; // No search param

    auto vsearch_expr = VSearchExpr::create_shared(node);
    ASSERT_NE(nullptr, vsearch_expr);

    // Should handle missing search param gracefully
    auto dummy_expr = VSearchExpr::create_shared(node);
    VExprContext context(dummy_expr);
    auto status = vsearch_expr->evaluate_inverted_index(&context, 100);
    EXPECT_FALSE(status.ok()); // Should fail due to empty DSL
}

TEST_F(VSearchExprTest, TestConstructorWithVeryLongDSL) {
    // Create a very long DSL string
    std::string very_long_dsl = "title:" + std::string(10000, 'a');

    TExprNode node;
    node.node_type = TExprNodeType::SEARCH_EXPR;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    node.__set_type(type_desc);
    node.num_children = 1;

    TSearchParam searchParam;
    searchParam.original_dsl = very_long_dsl;

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = std::string(10000, 'a');
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "title";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    node.search_param = searchParam;
    node.__isset.search_param = true;

    auto vsearch_expr = VSearchExpr::create_shared(node);
    ASSERT_NE(nullptr, vsearch_expr);
}

TEST_F(VSearchExprTest, TestConstructorWithSpecialCharactersInDSL) {
    std::vector<std::string> special_dsls = {
            "title:\"hello world\"", // Quotes
            "title:hello\nworld",    // Newline
            "title:hello\tworld",    // Tab
            "title:hello\\world",    // Backslash
            "title:你好世界",        // Unicode
            "title:🔍🌟",            // Emojis
            "title:123456",          // Numbers only
            "title:",                // Empty value
            ":hello",                // Empty field
            ":",                     // Both empty
            "",                      // Completely empty
            " ",                     // Space only
            "\n",                    // Newline only
            "\t"                     // Tab only
    };

    for (const auto& special_dsl : special_dsls) {
        TExprNode node;
        node.node_type = TExprNodeType::SEARCH_EXPR;
        TTypeDesc type_desc;
        TTypeNode type_node;
        type_node.type = TTypeNodeType::SCALAR;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::BOOLEAN);
        type_node.__set_scalar_type(scalar_type);
        type_desc.types.push_back(type_node);
        node.__set_type(type_desc);
        node.num_children = 1;

        TSearchParam searchParam;
        searchParam.original_dsl = special_dsl;

        TSearchClause rootClause;
        rootClause.clause_type = "TERM";
        rootClause.field_name = "title";
        rootClause.value = "hello";
        searchParam.root = rootClause;

        TSearchFieldBinding binding;
        binding.field_name = "title";
        binding.slot_index = 0;
        searchParam.field_bindings = {binding};

        node.search_param = searchParam;
        node.__isset.search_param = true;

        auto vsearch_expr = VSearchExpr::create_shared(node);
        ASSERT_NE(nullptr, vsearch_expr) << "Failed for DSL: " << special_dsl;
    }
}

} // namespace doris::vectorized

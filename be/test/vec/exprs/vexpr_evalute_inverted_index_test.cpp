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

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/Types_types.h>

#include "gtest/gtest.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vcast_expr.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"

// test expr is: v not in (ipv4_literal, casttoipv4(varchar))
TEST(TExprInvertedIndexTest, test_expr_evaluate_inverted_index) {
    doris::TExprNode in_expr_node;
    in_expr_node.__set_node_type(doris::TExprNodeType::IN_PRED);
    doris::TTypeDesc type_desc;

    doris::TTypeNode type_node;
    type_node.__set_type(doris::TTypeNodeType::SCALAR);
    doris::TScalarType result_scalar_type;
    result_scalar_type.__set_type(doris::TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(result_scalar_type);
    type_desc.types.push_back(type_node);
    in_expr_node.__set_type(type_desc);
    in_expr_node.__set_opcode(TExprOpcode::FILTER_NOT_IN);
    in_expr_node.__set_num_children(3);
    doris::TInPredicate in_predicate;
    in_predicate.__set_is_not_in(true);
    in_expr_node.__set_in_predicate(in_predicate);
    in_expr_node.__set_output_scale(-1);
    in_expr_node.__set_is_nullable(true);

    doris::TExprNode ipv4_literal_node;
    ipv4_literal_node.node_type = doris::TExprNodeType::IPV4_LITERAL;
    doris::TTypeDesc child2_type;

    doris::TTypeNode child2_type_node;
    child2_type_node.type = doris::TTypeNodeType::SCALAR;
    doris::TScalarType child2_scalar_type;
    child2_scalar_type.__set_type(doris::TPrimitiveType::IPV4);
    child2_type_node.__set_scalar_type(child2_scalar_type);
    child2_type.types.push_back(child2_type_node);
    ipv4_literal_node.__set_type(child2_type);
    ipv4_literal_node.__set_num_children(0);
    ipv4_literal_node.__set_output_scale(-1);
    ipv4_literal_node.__set_is_nullable(false);
    doris::TIPv4Literal ipv4_literal;
    ipv4_literal.__set_value(555819297);
    ipv4_literal_node.__set_ipv4_literal(ipv4_literal);

    doris::TExprNode cast_ipv4_node;
    cast_ipv4_node.__set_node_type(doris::TExprNodeType::CAST_EXPR);
    doris::TTypeDesc child3_type;

    doris::TTypeNode child3_type_node;
    child3_type_node.type = doris::TTypeNodeType::SCALAR;
    doris::TScalarType child3_scalar_type;
    child3_scalar_type.__set_type(doris::TPrimitiveType::IPV4);
    child3_type_node.__set_scalar_type(child3_scalar_type);
    child3_type.types.push_back(child3_type_node);
    cast_ipv4_node.__set_type(child3_type);
    cast_ipv4_node.__set_opcode(TExprOpcode::CAST);
    cast_ipv4_node.__set_num_children(1);
    cast_ipv4_node.__set_output_scale(-1);
    cast_ipv4_node.__set_is_nullable(false);

    doris::TFunction function;
    doris::TFunctionName function_name;
    function_name.__set_function_name("casttoipv4");
    function.__set_name(function_name);
    function.__set_binary_type(doris::TFunctionBinaryType::BUILTIN);

    doris::TTypeNode arg_type_node;
    arg_type_node.__set_type(doris::TTypeNodeType::SCALAR);
    doris::TScalarType arg_scalar_type;
    arg_scalar_type.__set_type(doris::TPrimitiveType::VARCHAR);
    arg_scalar_type.__set_len(65533);
    arg_type_node.__set_scalar_type(arg_scalar_type);
    doris::TTypeDesc arg_type;
    arg_type.types.push_back(arg_type_node);
    function.arg_types.push_back(arg_type);

    doris::TTypeNode ret_type_node;
    ret_type_node.__set_type(doris::TTypeNodeType::SCALAR);
    doris::TScalarType ret_scalar_type;
    ret_scalar_type.__set_type(doris::TPrimitiveType::IPV4);
    ret_type_node.__set_scalar_type(ret_scalar_type);
    doris::TTypeDesc ret_type;
    ret_type.types.push_back(ret_type_node);
    function.__set_ret_type(ret_type);
    function.__set_has_var_args(false);
    function.__set_signature("casttoipv4(varchar(65533))");
    function.__set_id(0);
    function.__set_vectorized(true);
    function.__set_is_udtf_function(false);
    function.__set_is_static_load(false);
    function.__set_expiration_time(360);
    cast_ipv4_node.__set_fn(function);
    cast_ipv4_node.__set_is_nullable(true);

    doris::TExprNode string_literal_node;
    string_literal_node.__set_node_type(doris::TExprNodeType::STRING_LITERAL);
    doris::TTypeDesc child4_type;
    doris::TTypeNode child4_type_node;
    child4_type_node.type = doris::TTypeNodeType::SCALAR;
    doris::TScalarType child4_scalar_type;
    child4_scalar_type.__set_type(doris::TPrimitiveType::VARCHAR);
    child4_scalar_type.__set_len(65533);
    child4_type_node.__set_scalar_type(child4_scalar_type);
    child4_type.types.push_back(child4_type_node);
    string_literal_node.__set_type(child4_type);
    string_literal_node.__set_num_children(0);
    string_literal_node.__set_output_scale(-1);
    string_literal_node.__set_is_nullable(false);
    doris::TStringLiteral string_literal;
    string_literal.__set_value("abc");
    string_literal_node.__set_string_literal(string_literal);

    doris::vectorized::VExprSPtr in_expr;
    EXPECT_TRUE(doris::vectorized::VExpr::create_expr(in_expr_node, in_expr).ok());
    doris::vectorized::VExprSPtr ipv4_literal_expr;
    EXPECT_TRUE(doris::vectorized::VExpr::create_expr(ipv4_literal_node, ipv4_literal_expr).ok());

    doris::vectorized::VExprSPtr cast_ipv4_expr;
    EXPECT_TRUE(doris::vectorized::VExpr::create_expr(cast_ipv4_node, cast_ipv4_expr).ok());

    doris::vectorized::VExprSPtr string_literal_expr;
    EXPECT_TRUE(
            doris::vectorized::VExpr::create_expr(string_literal_node, string_literal_expr).ok());

    cast_ipv4_expr->add_child(string_literal_expr);

    in_expr->add_child(ipv4_literal_expr);
    in_expr->add_child(cast_ipv4_expr);

    doris::vectorized::VExprContext expr_ctx(in_expr);

    std::unordered_map<doris::ColumnId, std::unordered_map<const doris::vectorized::VExpr*, bool>>
            common_expr_inverted_index_status;
    auto inverted_index_context = std::make_shared<doris::vectorized::InvertedIndexContext>(
            std::vector<doris::ColumnId>(),
            std::vector<std::unique_ptr<doris::segment_v2::IndexIterator>>(),
            std::vector<doris::vectorized::IndexFieldNameAndTypePair>(),
            common_expr_inverted_index_status);
    expr_ctx.set_inverted_index_context(inverted_index_context);
    doris::RuntimeState state;
    doris::RowDescriptor row_desc;
    EXPECT_TRUE(in_expr->prepare(&state, row_desc, &expr_ctx).ok());
    EXPECT_TRUE(in_expr->evaluate_inverted_index(&expr_ctx, 100).ok());
    EXPECT_FALSE(expr_ctx.get_inverted_index_context()->has_inverted_index_result_for_expr(
            in_expr.get()));
    EXPECT_TRUE(expr_ctx.get_inverted_index_context()->_inverted_index_result_bitmap.empty());
    EXPECT_TRUE(expr_ctx.get_inverted_index_context()->_expr_inverted_index_status.empty());
}
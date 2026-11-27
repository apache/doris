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

#include "runtime/fold_constant_executor.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <array>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "core/data_type/primitive_type.h"
#include "runtime/descriptor_helper.h"

namespace doris {
namespace {

TExpr timestamp_ns_literal_expr(std::string value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::DATE_LITERAL);
    node.__set_num_children(0);
    node.__set_is_nullable(false);

    TDateLiteral literal;
    literal.__set_value(std::move(value));
    node.__set_date_literal(literal);

    TTypeDesc type_desc = create_type_desc(TYPE_TIMESTAMP_NS);
    type_desc.__set_is_nullable(false);
    node.__set_type(type_desc);

    TExpr expr;
    expr.__set_nodes({std::move(node)});
    return expr;
}

TEST(FoldConstantExecutorTest, LegacyTimeStampNsResult) {
    const std::vector<std::string> values {
            "1677-09-21 00:12:43.145224192", "1969-12-31 23:59:59.999999999",
            "1970-01-01 00:00:00.000000000", "1970-01-01 00:00:00.000000001",
            "2262-04-11 23:47:16.854775807"};

    std::map<std::string, TExpr> expressions;
    for (size_t i = 0; i < values.size(); ++i) {
        expressions.emplace(std::to_string(i), timestamp_ns_literal_expr(values[i]));
    }

    TFoldConstantParams params;
    params.expr_map.emplace("timestamp_ns", std::move(expressions));
    params.query_globals.__set_now_string("1970-01-01 00:00:00");
    params.query_globals.__set_time_zone("UTC");
    params.__set_is_nereids(false);

    PConstantExprResult response;
    FoldConstantExecutor executor;
    const auto status = executor.fold_constant_vexpr(params, &response);
    ASSERT_TRUE(status.ok()) << status;

    const auto& results = response.expr_result_map().at("timestamp_ns").map();
    ASSERT_EQ(values.size(), results.size());
    for (size_t i = 0; i < values.size(); ++i) {
        const auto& result = results.at(std::to_string(i));
        EXPECT_TRUE(result.success());
        EXPECT_EQ(values[i], result.content());
    }
}

TEST(FoldConstantExecutorTest, LocalizedDateNames) {
    const std::array<std::array<const char*, 3>, 3> locales {{{"ru_RU", "воскресенье", "сентября"},
                                                              {"zh_CN", "星期日", "九月"},
                                                              {"en_US", "Sunday", "September"}}};
    for (const auto& locale : locales) {
        TFoldConstantParams params;
        params.query_globals.__set_now_string("2026-09-20 00:00:00");
        params.query_globals.__set_time_zone("UTC");
        params.query_globals.__set_lc_time_names(locale[0]);
        // Decode strings directly so FE evaluation cannot hide a missing BE locale.
        params.__set_is_nereids(false);
        for (const auto* name : {"dayname", "monthname"}) {
            TFunctionName function_name;
            function_name.__set_function_name(name);
            TFunction function;
            function.__set_name(function_name);
            function.__set_binary_type(TFunctionBinaryType::BUILTIN);
            function.__set_arg_types({create_type_desc(TYPE_TIMESTAMP_NS)});
            function.__set_ret_type(create_type_desc(TYPE_STRING));
            TExprNode call;
            call.__set_node_type(TExprNodeType::FUNCTION_CALL);
            call.__set_type(create_type_desc(TYPE_STRING));
            call.__set_is_nullable(false);
            call.__set_num_children(1);
            call.__set_fn(function);
            auto expression = timestamp_ns_literal_expr("2026-09-20 12:34:56.123456789");
            expression.nodes.insert(expression.nodes.begin(), std::move(call));
            params.expr_map["names"].emplace(name, std::move(expression));
        }
        PConstantExprResult response;
        FoldConstantExecutor executor;
        const auto status = executor.fold_constant_vexpr(params, &response);
        ASSERT_TRUE(status.ok()) << status;
        const auto& results = response.expr_result_map().at("names").map();
        ASSERT_EQ(results.size(), 2);
        EXPECT_TRUE(results.at("dayname").success());
        EXPECT_TRUE(results.at("monthname").success());
        EXPECT_EQ(results.at("dayname").content(), locale[1]);
        EXPECT_EQ(results.at("monthname").content(), locale[2]);
    }
}

} // namespace
} // namespace doris

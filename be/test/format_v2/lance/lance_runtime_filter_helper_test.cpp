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

#include "format_v2/lance/lance_runtime_filter_helper.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vbloom_predicate.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format/format_common.h"
#include "runtime/runtime_profile.h"

namespace doris::format::lance {
namespace {

TExprNode runtime_in_node() {
    TExprNode node;
    node.__set_type(std::make_shared<DataTypeUInt8>()->to_thrift());
    node.__set_node_type(TExprNodeType::IN_PRED);
    node.in_predicate.__set_is_not_in(false);
    node.__set_opcode(TExprOpcode::FILTER_IN);
    node.__set_is_nullable(false);
    return node;
}

VExprContextSPtr wrap_runtime_filter(VExprSPtr impl, const TExprNode& node, int filter_id) {
    return VExprContext::create_shared(
            RuntimeFilterExpr::create_shared(node, std::move(impl), 0.0, false, filter_id));
}

VExprContextSPtr int64_runtime_in(std::string column_name, std::vector<int64_t> values,
                                  int filter_id) {
    std::shared_ptr<HybridSetBase> filter(create_set(TYPE_BIGINT, false));
    for (const auto value : values) {
        filter->insert(&value);
    }
    auto node = runtime_in_node();
    auto predicate = VDirectInPredicate::create_shared(node, std::move(filter), true);
    predicate->add_child(VSlotRef::create_shared(
            0, 0, -1, make_nullable(std::make_shared<DataTypeInt64>()), std::move(column_name)));
    return wrap_runtime_filter(std::move(predicate), node, filter_id);
}

VExprContextSPtr string_runtime_in(std::string column_name, const std::string& value,
                                   int filter_id) {
    std::shared_ptr<HybridSetBase> filter(create_set(TYPE_STRING, false));
    StringRef value_ref(value.data(), value.size());
    filter->insert(&value_ref);
    auto node = runtime_in_node();
    auto predicate = VDirectInPredicate::create_shared(node, std::move(filter), true);
    predicate->add_child(VSlotRef::create_shared(
            0, 0, -1, make_nullable(std::make_shared<DataTypeString>()), std::move(column_name)));
    return wrap_runtime_filter(std::move(predicate), node, filter_id);
}

VExprContextSPtr int64_runtime_range(std::string column_name, TExprOpcode::type opcode,
                                     int64_t value, int filter_id) {
    const auto value_type = std::make_shared<DataTypeInt64>();
    const auto nullable_value_type = make_nullable(value_type);
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());

    TFunctionName function_name;
    function_name.__set_function_name(opcode == TExprOpcode::GE ? "ge" : "le");
    TFunction function;
    function.__set_name(function_name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types({nullable_value_type->to_thrift(), value_type->to_thrift()});
    function.__set_ret_type(result_type->to_thrift());
    function.__set_has_var_args(false);

    TExprNode predicate_node;
    predicate_node.__set_node_type(TExprNodeType::BINARY_PRED);
    predicate_node.__set_opcode(opcode);
    predicate_node.__set_type(result_type->to_thrift());
    predicate_node.__set_fn(function);
    predicate_node.__set_num_children(2);
    predicate_node.__set_is_nullable(true);
    auto predicate = VectorizedFnCall::create_shared(predicate_node);
    predicate->add_child(
            VSlotRef::create_shared(0, 0, -1, nullable_value_type, std::move(column_name)));
    predicate->add_child(
            VLiteral::create_shared(value_type, Field::create_field<TYPE_BIGINT>(value)));

    TExprNode wrapper_node;
    wrapper_node.__set_type(std::make_shared<DataTypeUInt8>()->to_thrift());
    wrapper_node.__set_is_nullable(false);
    return wrap_runtime_filter(std::move(predicate), wrapper_node, filter_id);
}

VExprContextSPtr unsupported_bloom_runtime_filter(std::string column_name, int filter_id) {
    auto node = runtime_in_node();
    node.__set_node_type(TExprNodeType::BLOOM_PRED);
    node.__set_opcode(TExprOpcode::RT_FILTER);
    auto predicate = VBloomPredicate::create_shared(node);
    predicate->add_child(VSlotRef::create_shared(
            0, 0, -1, make_nullable(std::make_shared<DataTypeInt64>()), std::move(column_name)));
    return wrap_runtime_filter(std::move(predicate), node, filter_id);
}

TEST(LanceRuntimeFilterHelperTest, ConvertsSupportedFiltersToLanceSql) {
    const VExprContextSPtrs conjuncts {
            int64_runtime_in("order`key", {7}, 3),
            string_runtime_in("author", "O'Reilly", 5),
            int64_runtime_range("score", TExprOpcode::GE, 10, 7),
            int64_runtime_range("score", TExprOpcode::LE, 20, 7),
    };

    const auto result = get_or_create_lance_runtime_filter_sql(conjuncts, nullptr);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(
            "(`order``key` IN (7)) AND (`author` IN ('O''Reilly')) AND "
            "(`score` >= 10) AND (`score` <= 20)",
            result->expression);
    EXPECT_EQ((std::vector<int> {3, 5, 7}), result->pushable_filter_ids);
    EXPECT_TRUE(result->skipped_filter_ids.empty());
}

TEST(LanceRuntimeFilterHelperTest, RecordsUnsupportedRuntimeFilters) {
    const VExprContextSPtrs conjuncts {
            int64_runtime_in("id", {2}, 3),
            unsupported_bloom_runtime_filter("id", 8),
    };

    const auto result = get_or_create_lance_runtime_filter_sql(conjuncts, nullptr);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ("(`id` IN (2))", result->expression);
    EXPECT_EQ((std::vector<int> {3}), result->pushable_filter_ids);
    EXPECT_EQ((std::vector<int> {8}), result->skipped_filter_ids);

    RuntimeProfile profile("lance_runtime_filter_profile");
    record_lance_runtime_filter_pushdown(&profile, *result);
    ASSERT_NE(profile.get_info_string("LanceRuntimeFilterPushedIds"), nullptr);
    EXPECT_EQ("3", *profile.get_info_string("LanceRuntimeFilterPushedIds"));
    ASSERT_NE(profile.get_info_string("LanceRuntimeFilterSkippedIds"), nullptr);
    EXPECT_EQ("8", *profile.get_info_string("LanceRuntimeFilterSkippedIds"));
}

TEST(LanceRuntimeFilterHelperTest, IgnoresNonRuntimeFilterConjuncts) {
    const VExprContextSPtrs conjuncts {VExprContext::create_shared(VSlotRef::create_shared(
            0, 0, -1, std::make_shared<DataTypeInt64>(), "ordinary_column"))};

    EXPECT_EQ(nullptr, get_or_create_lance_runtime_filter_sql(conjuncts, nullptr));
}

TEST(LanceRuntimeFilterHelperTest, ReusesSnapshotAcrossParallelReaders) {
    ShardedKVCache cache(2);
    const VExprContextSPtrs first_conjuncts {
            int64_runtime_in("id", {2}, 12),
            int64_runtime_range("score", TExprOpcode::GE, 10, 13),
    };
    const VExprContextSPtrs reordered_conjuncts {
            int64_runtime_range("score", TExprOpcode::GE, 10, 13),
            int64_runtime_in("id", {2}, 12),
    };

    const auto first = get_or_create_lance_runtime_filter_sql(first_conjuncts, &cache);
    const auto reused = get_or_create_lance_runtime_filter_sql(reordered_conjuncts, &cache);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(reused, nullptr);
    EXPECT_EQ(first.get(), reused.get());
    EXPECT_EQ("(`id` IN (2)) AND (`score` >= 10)", reused->expression);

    const auto different =
            get_or_create_lance_runtime_filter_sql({int64_runtime_in("id", {2}, 14)}, &cache);
    ASSERT_NE(different, nullptr);
    EXPECT_NE(first.get(), different.get());
}

} // namespace
} // namespace doris::format::lance

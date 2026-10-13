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

#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/object_pool.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/aggregate_function_sort.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_agg_fn_evaluator.h"

namespace doris {
namespace {

TExpr create_group_concat_expr(const std::string& function_name, const DataTypes& argument_types) {
    TExpr expr;
    TExprNode root;
    root.node_type = TExprNodeType::AGG_EXPR;
    root.num_children = argument_types.size();
    root.fn.name.function_name = function_name;
    root.fn.ret_type = argument_types[0]->to_thrift();
    root.__set_is_nullable(false);
    std::vector<TTypeDesc> param_types;
    for (const auto& type : argument_types) {
        auto param_type = type->to_thrift();
        param_type.__set_is_nullable(type->is_nullable());
        param_types.push_back(param_type);
    }
    root.agg_expr.__set_param_types(param_types);
    expr.nodes.push_back(root);
    for (int i = 0; i < argument_types.size(); ++i) {
        TExprNode child;
        child.node_type = TExprNodeType::SLOT_REF;
        child.num_children = 0;
        child.type = param_types[i];
        child.__set_is_nullable(argument_types[i]->is_nullable());
        child.slot_ref.slot_id = i;
        child.slot_ref.tuple_id = 0;
        expr.nodes.push_back(child);
    }
    return expr;
}

void add_group_concat_rows(const IAggregateFunction& function, AggregateDataPtr result,
                           const IColumn** columns, bool merge, Arena& arena) {
    if (!merge) {
        function.add_batch_single_place(columns[0]->size(), result, columns, arena);
        return;
    }
    for (size_t offset : {0, 2}) {
        AggregateFunctionGuard partial(&function);
        for (size_t row = offset; row < offset + 2; ++row) {
            function.add(partial.data(), columns, row, arena);
        }
        auto serialized = function.create_serialize_column();
        function.serialize_without_key_to_column(partial.data(), *serialized);
        function.deserialize_and_merge_from_column(result, *serialized, arena);
    }
}

} // namespace

TEST(AggFnEvaluatorTest, test_single) {
    ObjectPool pool;
    [[maybe_unused]] auto* agg_fn = create_mock_agg_fn_evaluator(pool);
    Arena arena;

    Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});

    // init place
    AggregateDataPtr place =
            reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->function()->size_of_data()));

    agg_fn->create(place);

    // execute agg fn
    auto st = agg_fn->execute_single_add(&block, place, arena);

    // get result
    ColumnInt64 result_column;
    agg_fn->insert_result_info(place, &result_column);
    EXPECT_EQ(result_column.get_element(0), 6);

    // reset place
    agg_fn->reset(place);
}

TEST(AggFnEvaluatorTest, test_batch) {
    ObjectPool pool;
    [[maybe_unused]] auto* agg_fn = create_mock_agg_fn_evaluator(pool);
    Arena arena;

    Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});

    // init place
    AggregateDataPtr place1 =
            reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->function()->size_of_data()));

    AggregateDataPtr place2 =
            reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->function()->size_of_data()));

    agg_fn->create(place1);
    agg_fn->create(place2);

    std::vector<AggregateDataPtr> places;
    places.push_back(place1);
    places.push_back(place2);
    places.push_back(place2);

    // execute agg fn
    auto st = agg_fn->execute_batch_add(&block, 0, places.data(), arena);

    // get result
    {
        ColumnInt64 result_column;
        agg_fn->insert_result_info(place1, &result_column);
        EXPECT_EQ(result_column.get_element(0), 1);
    }

    {
        ColumnInt64 result_column;
        agg_fn->insert_result_info(place2, &result_column);
        EXPECT_EQ(result_column.get_element(0), 5);
    }

    // reset place
    agg_fn->reset(place1);
    agg_fn->reset(place2);
}

TEST(AggFnEvaluatorTest, test_clone) {
    ObjectPool pool;
    [[maybe_unused]] auto* old_agg_fn = create_mock_agg_fn_evaluator(pool);

    auto* agg_fn = old_agg_fn->clone(nullptr, &pool);
    Arena arena;

    Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});

    // init place
    AggregateDataPtr place1 =
            reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->function()->size_of_data()));

    AggregateDataPtr place2 =
            reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->function()->size_of_data()));

    agg_fn->create(place1);
    agg_fn->create(place2);

    std::vector<AggregateDataPtr> places;
    places.push_back(place1);
    places.push_back(place2);
    places.push_back(place2);

    // execute agg fn
    auto st = agg_fn->execute_batch_add(&block, 0, places.data(), arena);

    // get result
    {
        ColumnInt64 result_column;
        agg_fn->insert_result_info(place1, &result_column);
        EXPECT_EQ(result_column.get_element(0), 1);
    }

    {
        ColumnInt64 result_column;
        agg_fn->insert_result_info(place2, &result_column);
        EXPECT_EQ(result_column.get_element(0), 5);
    }

    // reset place
    agg_fn->reset(place1);
    agg_fn->reset(place2);
}

TEST(AggFnEvaluatorTest, group_concat_order_by_null_placement) {
    struct TestCase {
        bool ascending;
        bool nulls_first;
        std::string expected;
        std::string distinct_expected;
    };
    const std::vector<TestCase> test_cases = {{.ascending = true,
                                               .nulls_first = true,
                                               .expected = "a|a|b|c",
                                               .distinct_expected = "a|b|c"},
                                              {.ascending = true,
                                               .nulls_first = false,
                                               .expected = "b|c|a|a",
                                               .distinct_expected = "b|c|a"},
                                              {.ascending = false,
                                               .nulls_first = true,
                                               .expected = "a|a|c|b",
                                               .distinct_expected = "a|c|b"},
                                              {.ascending = false,
                                               .nulls_first = false,
                                               .expected = "c|b|a|a",
                                               .distinct_expected = "c|b|a"}};
    const DataTypes argument_types = {std::make_shared<DataTypeString>(),
                                      std::make_shared<DataTypeString>(),
                                      make_nullable(std::make_shared<DataTypeInt32>())};
    for (const auto& test_case : test_cases) {
        for (bool distinct : {false, true}) {
            for (bool merge : {false, true}) {
                SCOPED_TRACE(testing::Message() << "ascending=" << test_case.ascending
                                                << " nulls_first=" << test_case.nulls_first
                                                << " distinct=" << distinct << " merge=" << merge);
                const std::string function_name =
                        distinct ? "multi_distinct_group_concat" : "group_concat";
                auto expr = create_group_concat_expr(function_name, argument_types);
                TSortInfo sort_info;
                sort_info.ordering_exprs.resize(1);
                sort_info.is_asc_order = {test_case.ascending};
                sort_info.nulls_first = {test_case.nulls_first};
                ObjectPool pool;
                AggFnEvaluator* evaluator = nullptr;
                ASSERT_TRUE(AggFnEvaluator::create(&pool, expr, sort_info, true, false, &evaluator)
                                    .ok());
                RuntimeState state;
                state._query_options.__set_be_exec_version(
                        BeExecVersionManager::get_newest_version());
                auto nested_function = AggregateFunctionSimpleFactory::instance().get(
                        function_name, evaluator->_real_argument_types, argument_types[0], false,
                        BeExecVersionManager::get_newest_version(), {.column_names = {}});
                ASSERT_NE(nested_function, nullptr);
                auto function = transform_to_sort_agg_function(
                        nested_function, evaluator->_argument_types_with_sort,
                        evaluator->_sort_description, &state);
                auto values = ColumnHelper::create_column<DataTypeString>({"a", "b", "c", "a"});
                auto separators = ColumnHelper::create_column<DataTypeString>({"|", "|", "|", "|"});
                auto keys = ColumnHelper::create_nullable_column<DataTypeInt32>({0, 1, 2, 0},
                                                                                {1, 0, 0, 1});
                const IColumn* columns[] = {values.get(), separators.get(), keys.get()};
                Arena arena;
                AggregateFunctionGuard result(function.get());
                add_group_concat_rows(*function, result.data(), columns, merge, arena);
                auto result_column = ColumnString::create();
                function->insert_result_into(result.data(), *result_column);
                EXPECT_EQ(result_column->get_data_at(0).to_string(),
                          distinct ? test_case.distinct_expected : test_case.expected);
            }
        }
    }
}

} // namespace doris

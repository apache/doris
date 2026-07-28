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

#include <string>
#include <string_view>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/exception.h"
#include "common/object_pool.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_agg_fn_evaluator.h"

namespace doris {
namespace {

template <typename Action>
void expect_exception_message_contains(Action&& action, const std::string& expected_message) {
    try {
        action();
        FAIL() << "Expected doris::Exception";
    } catch (const Exception& e) {
        EXPECT_NE(e.to_string().find(expected_message), std::string::npos) << e.to_string();
    }
}

MutableColumnPtr create_string64_column() {
    auto column = ColumnString64::create();
    column->insert_data("a", 1);
    column->insert_data("b", 1);
    column->insert_data("c", 1);
    return column;
}

MutableColumnPtr create_nullable_string_column() {
    auto nested_column = ColumnString::create();
    nested_column->insert_data("a", 1);
    nested_column->insert_data("b", 1);
    nested_column->insert_data("c", 1);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(0);
    null_map->insert_value(0);
    return ColumnNullable::create(std::move(nested_column), std::move(null_map));
}

MutableColumnPtr create_nullable_string64_column() {
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(0);
    null_map->insert_value(0);
    return ColumnNullable::create(create_string64_column(), std::move(null_map));
}

MutableColumnPtr create_nullable_int64_column() {
    auto nested_column = ColumnInt64::create();
    nested_column->insert_value(1);
    nested_column->insert_value(2);
    nested_column->insert_value(3);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(0);
    null_map->insert_value(0);
    return ColumnNullable::create(std::move(nested_column), std::move(null_map));
}

MutableColumnPtr create_int64_column() {
    auto column = ColumnInt64::create();
    column->insert_value(1);
    column->insert_value(2);
    column->insert_value(3);
    return column;
}

MutableColumnPtr create_array_nullable_string64_column() {
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);
    return ColumnArray::create(create_nullable_string64_column(), std::move(offsets));
}

MutableColumnPtr create_map_nullable_string64_key_column(bool nullable_values) {
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);
    auto value_column = nullable_values ? create_nullable_int64_column() : create_int64_column();
    return ColumnMap::create(create_nullable_string64_column(), std::move(value_column),
                             std::move(offsets));
}

} // namespace

TEST(AggFnEvaluatorTest, test_single) {
    ObjectPool pool;
    [[maybe_unused]] auto* agg_fn = create_mock_agg_fn_evaluator(pool);
    Arena arena;

    Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});

    // init place
    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));

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
    auto* place1 = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));

    auto* place2 = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));

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
    auto* place1 = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));

    auto* place2 = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));

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

TEST(AggFnEvaluatorTest, check_input_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_mock_agg_fn_evaluator(pool);
    Arena arena;

    Block block = ColumnHelper::create_block<DataTypeString>({"a", "b", "c"});

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    EXPECT_THROW({ static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
                 Exception);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, check_result_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_mock_agg_fn_evaluator(pool);
    Arena arena;

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    ColumnString result_column;
    EXPECT_THROW(agg_fn->insert_result_info(place, &result_column), Exception);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, group_concat_check_input_physical_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(pool, "group_concat", {std::make_shared<DataTypeString>()},
                                 nullptr, false);
    Arena arena;

    auto string64_column = ColumnString64::create();
    string64_column->insert_data("a", 1);
    string64_column->insert_data("b", 1);
    string64_column->insert_data("c", 1);
    Block block({ColumnWithTypeAndName(std::move(string64_column),
                                       std::make_shared<DataTypeString>(), "bad_string")});

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    EXPECT_THROW({ static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
                 Exception);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, group_concat_check_fixed_input_physical_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(pool, "group_concat", {std::make_shared<DataTypeInt64>()}, nullptr,
                                 false);
    Arena arena;

    Block block({ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeInt64>({1, 2, 3}),
                                       std::make_shared<DataTypeInt64>(), "bad_string")});

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    EXPECT_THROW({ static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
                 Exception);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, group_concat_check_result_physical_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(pool, "group_concat", {std::make_shared<DataTypeString>()},
                                 nullptr, false);
    Arena arena;

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    auto result_column = ColumnString64::create();
    EXPECT_THROW(agg_fn->insert_result_info(place, result_column.get()), Exception);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, min_max_check_string_input_physical_column_type) {
    for (const auto* function_name : {"min", "max", "any"}) {
        SCOPED_TRACE(function_name);
        ObjectPool pool;
        auto* agg_fn = create_agg_fn(pool, function_name, {std::make_shared<DataTypeString>()},
                                     nullptr, false);
        Arena arena;

        auto string64_column = ColumnString64::create();
        string64_column->insert_data("a", 1);
        string64_column->insert_data("b", 1);
        string64_column->insert_data("c", 1);
        Block block({ColumnWithTypeAndName(std::move(string64_column),
                                           std::make_shared<DataTypeString>(), "bad_string")});

        auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
        agg_fn->create(place);

        EXPECT_THROW({ static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
                     Exception);

        agg_fn->destroy(place);
    }
}

TEST(AggFnEvaluatorTest, min_max_check_string_result_physical_column_type) {
    for (const auto* function_name : {"min", "max", "any"}) {
        SCOPED_TRACE(function_name);
        ObjectPool pool;
        auto* agg_fn = create_agg_fn(pool, function_name, {std::make_shared<DataTypeString>()},
                                     nullptr, false);
        Arena arena;

        auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
        agg_fn->create(place);

        auto result_column = ColumnString64::create();
        EXPECT_THROW(agg_fn->insert_result_info(place, result_column.get()), Exception);

        agg_fn->destroy(place);
    }
}

TEST(AggFnEvaluatorTest, count_not_null_check_nullable_result_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(pool, "count", {make_nullable(std::make_shared<DataTypeInt64>())},
                                 std::make_shared<DataTypeInt64>(), false);
    Arena arena;

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    auto result_column = make_nullable(std::make_shared<DataTypeInt64>())->create_column();
    EXPECT_NO_THROW(agg_fn->insert_result_info(place, result_column.get()));
    EXPECT_EQ(result_column->size(), 1);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, count_by_enum_check_input_physical_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(pool, "count_by_enum", {std::make_shared<DataTypeString>()},
                                 nullptr, false);
    Arena arena;

    auto string64_column = ColumnString64::create();
    string64_column->insert_data("a", 1);
    string64_column->insert_data("b", 1);
    string64_column->insert_data("c", 1);
    Block block({ColumnWithTypeAndName(std::move(string64_column),
                                       std::make_shared<DataTypeString>(), "bad_string")});

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    EXPECT_THROW({ static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
                 Exception);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, count_by_enum_check_nullable_input_physical_column_type) {
    ObjectPool pool;
    auto input_type = make_nullable(std::make_shared<DataTypeString>());
    auto* agg_fn = create_agg_fn(pool, "count_by_enum", {input_type}, nullptr, false);
    Arena arena;

    auto string64_column = ColumnString64::create();
    string64_column->insert_data("a", 1);
    string64_column->insert_data("b", 1);
    string64_column->insert_data("c", 1);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(0);
    null_map->insert_value(0);
    auto nullable_column = ColumnNullable::create(std::move(string64_column), std::move(null_map));
    Block block({ColumnWithTypeAndName(std::move(nullable_column), input_type, "bad_string")});

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    EXPECT_THROW({ static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
                 Exception);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, count_by_enum_check_result_physical_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(pool, "count_by_enum", {std::make_shared<DataTypeString>()},
                                 nullptr, false);
    Arena arena;

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    auto result_column = ColumnString64::create();
    EXPECT_THROW(agg_fn->insert_result_info(place, result_column.get()), Exception);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, string_result_aggregates_check_physical_column_type) {
    struct TestCase {
        std::string function_name;
        DataTypes argument_types;
    };
    std::vector<TestCase> test_cases = {
            {"ai_agg",
             {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
              std::make_shared<DataTypeString>()}},
            {"histogram", {std::make_shared<DataTypeString>()}},
            {"linear_histogram",
             {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeFloat64>()}}};

    for (const auto& test_case : test_cases) {
        SCOPED_TRACE(test_case.function_name);
        ObjectPool pool;
        auto* agg_fn = create_agg_fn(pool, test_case.function_name, test_case.argument_types,
                                     nullptr, false);
        Arena arena;

        auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
        agg_fn->create(place);

        auto result_column = ColumnString64::create();
        expect_exception_message_contains(
                [&]() { agg_fn->insert_result_info(place, result_column.get()); },
                "result type check failed");

        agg_fn->destroy(place);
    }
}

TEST(AggFnEvaluatorTest, array_agg_check_string_physical_column_type) {
    ObjectPool pool;
    auto input_type = make_nullable(std::make_shared<DataTypeString>());
    auto* agg_fn = create_agg_fn(pool, "array_agg", {input_type}, nullptr, false);
    Arena arena;

    Block bad_input_block(
            {ColumnWithTypeAndName(create_nullable_string64_column(), input_type, "bad_string")});

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    expect_exception_message_contains(
            [&]() {
                static_cast<void>(agg_fn->execute_single_add(&bad_input_block, place, arena));
            },
            "argument type check failed");

    auto bad_result_column = create_array_nullable_string64_column();
    expect_exception_message_contains(
            [&]() { agg_fn->insert_result_info(place, bad_result_column.get()); },
            "result type check failed");

    Block good_input_block(
            {ColumnWithTypeAndName(create_nullable_string_column(), input_type, "good_string")});
    auto bad_serialize_column = create_array_nullable_string64_column();
    expect_exception_message_contains(
            [&]() {
                static_cast<void>(agg_fn->streaming_agg_serialize_to_column(
                        &good_input_block, bad_serialize_column, good_input_block.rows(), arena));
            },
            "result type check failed");

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, collect_check_string_physical_column_type) {
    for (const auto* function_name : {"collect_list", "collect_set"}) {
        SCOPED_TRACE(function_name);
        ObjectPool pool;
        auto* agg_fn = create_agg_fn(pool, function_name, {std::make_shared<DataTypeString>()},
                                     nullptr, false);
        Arena arena;

        auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
        agg_fn->create(place);

        if (std::string_view(function_name) == "collect_list") {
            Block block({ColumnWithTypeAndName(create_string64_column(),
                                               std::make_shared<DataTypeString>(), "bad_string")});
            expect_exception_message_contains(
                    [&]() { static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
                    "argument type check failed");
        }

        auto bad_result_column = create_array_nullable_string64_column();
        expect_exception_message_contains(
                [&]() { agg_fn->insert_result_info(place, bad_result_column.get()); },
                "result type check failed");

        agg_fn->destroy(place);
    }
}

TEST(AggFnEvaluatorTest, datasketches_hll_union_agg_check_string_input_physical_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(pool, "datasketches_hll_union_agg",
                                 {std::make_shared<DataTypeString>()}, nullptr, false);
    Arena arena;

    Block block({ColumnWithTypeAndName(create_string64_column(), std::make_shared<DataTypeString>(),
                                       "bad_string")});

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    expect_exception_message_contains(
            [&]() { static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
            "argument type check failed");

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, map_agg_check_string_key_input_physical_column_type) {
    for (bool nullable_key : {false, true}) {
        SCOPED_TRACE(nullable_key);
        ObjectPool pool;
        DataTypePtr key_type = std::make_shared<DataTypeString>();
        if (nullable_key) {
            key_type = make_nullable(key_type);
        }
        auto* agg_fn = create_agg_fn(pool, "map_agg_v1",
                                     {key_type, std::make_shared<DataTypeInt64>()}, nullptr, false);
        Arena arena;

        MutableColumnPtr key_column =
                nullable_key ? create_nullable_string64_column() : create_string64_column();
        Block block({ColumnWithTypeAndName(std::move(key_column), key_type, "bad_key"),
                     ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeInt64>({1, 2, 3}),
                                           std::make_shared<DataTypeInt64>(), "value")});

        auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
        agg_fn->create(place);

        expect_exception_message_contains(
                [&]() { static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
                "argument type check failed");

        agg_fn->destroy(place);
    }
}

TEST(AggFnEvaluatorTest, map_combinator_check_string_key_physical_column_type) {
    ObjectPool pool;
    auto map_type = std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                                  std::make_shared<DataTypeInt64>());
    auto* agg_fn = create_agg_fn(pool, "sum_map", {map_type}, nullptr, false);
    Arena arena;

    Block block({ColumnWithTypeAndName(create_map_nullable_string64_key_column(false), map_type,
                                       "bad_map")});

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    expect_exception_message_contains(
            [&]() { static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
            "argument type check failed");

    auto bad_result_column = create_map_nullable_string64_key_column(true);
    expect_exception_message_contains(
            [&]() { agg_fn->insert_result_info(place, bad_result_column.get()); },
            "result type check failed");

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, topn_check_input_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(
            pool, "topn", {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()},
            nullptr, false);
    Arena arena;

    Block block({ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeInt64>({1, 2, 3}),
                                       std::make_shared<DataTypeInt64>(), "bad_value"),
                 ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeInt32>({2, 2, 2}),
                                       std::make_shared<DataTypeInt32>(), "top")});

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    EXPECT_THROW({ static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
                 Exception);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, topn_check_fixed_parameter_physical_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(
            pool, "topn", {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt64>()},
            nullptr, false);
    Arena arena;

    Block block({ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>({"a", "b", "c"}),
                                       std::make_shared<DataTypeString>(), "value"),
                 ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeInt64>({2, 2, 2}),
                                       std::make_shared<DataTypeInt64>(), "bad_top")});

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    EXPECT_THROW({ static_cast<void>(agg_fn->execute_single_add(&block, place, arena)); },
                 Exception);

    agg_fn->destroy(place);
}

TEST(AggFnEvaluatorTest, topn_check_result_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(
            pool, "topn", {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()},
            nullptr, false);
    Arena arena;

    std::vector<AggregateDataPtr> places(2);
    for (auto& place : places) {
        place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
        agg_fn->create(place);
    }

    ColumnInt64 result_column;
    EXPECT_THROW(agg_fn->insert_result_info_vec(places, 0, &result_column, places.size()),
                 Exception);

    for (auto place : places) {
        agg_fn->destroy(place);
    }
}

TEST(AggFnEvaluatorTest, group_array_intersect_check_input_physical_column_type) {
    auto agg_function = AggregateFunctionSimpleFactory::instance().get(
            "group_array_intersect",
            {std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>())}, nullptr, false,
            BeExecVersionManager::get_newest_version(), {.column_names = {}});
    ASSERT_NE(agg_function, nullptr);

    auto data_column = ColumnInt64::create();
    data_column->insert_value(1);
    data_column->insert_value(2);
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(2);
    auto array_column = ColumnArray::create(std::move(data_column), std::move(offsets));

    const IColumn* columns[1] = {array_column.get()};
    EXPECT_THROW(agg_function->check_input_columns_type(columns), Exception);
}

TEST(AggFnEvaluatorTest, group_array_intersect_check_result_physical_column_type) {
    ObjectPool pool;
    auto* agg_fn = create_agg_fn(
            pool, "group_array_intersect",
            {std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>())}, nullptr, false);
    Arena arena;

    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(agg_fn->size_of_data()));
    agg_fn->create(place);

    auto result_column =
            ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
    EXPECT_THROW(agg_fn->insert_result_info(place, result_column.get()), Exception);

    agg_fn->destroy(place);
}

} // namespace doris

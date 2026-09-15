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

#include <memory>
#include <vector>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "testutil/column_helper.h"

namespace doris {
namespace {

AggregateFunctionPtr get_agg_function(const std::string& name, const DataTypes& argument_types,
                                      bool result_is_nullable, AggregateFunctionAttr attr = {}) {
    auto function = AggregateFunctionSimpleFactory::instance().get(name, argument_types, nullptr,
                                                                   result_is_nullable, -1, attr);
    if (function == nullptr) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "failed to create aggregate function {}", name);
    }
    return function;
}

AggregateDataPtr create_state(const AggregateFunctionPtr& function, Arena& arena) {
    auto* place = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
    function->create(place);
    return place;
}

void destroy_states(const AggregateFunctionPtr& function,
                    const std::vector<AggregateDataPtr>& places) {
    for (auto* place : places) {
        function->destroy(place);
    }
}

void destroy_states(const AggregateFunctionPtr& function,
                    const std::vector<AggregateDataPtr>& places, size_t offset) {
    for (auto* place : places) {
        function->destroy(place + offset);
    }
}

TEST(AggregateFunctionRepeatTest, CountRepeatOutput) {
    Arena arena;
    auto function = get_agg_function("count", {}, false);
    std::vector<AggregateDataPtr> places {create_state(function, arena),
                                          create_state(function, arena)};

    function->add(places[0], nullptr, 0, arena);
    function->add(places[0], nullptr, 0, arena);
    for (int i = 0; i < 5; ++i) {
        function->add(places[1], nullptr, 0, arena);
    }

    ColumnInt64 single_result;
    function->insert_result_into_repeat(places[0], 3, single_result, arena);
    ASSERT_EQ(single_result.size(), 1);
    EXPECT_EQ(single_result.get_element(0), 6);

    ColumnInt64 result;
    std::vector<uint64_t> repeats {3, 4};
    function->insert_result_into_repeat_vec(places, 0, repeats, result, places.size(), arena);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result.get_element(0), 6);
    EXPECT_EQ(result.get_element(1), 20);
    destroy_states(function, places);
}

TEST(AggregateFunctionRepeatTest, CountRepeatZeroOutput) {
    Arena arena;
    auto function = get_agg_function("count", {}, false);
    std::vector<AggregateDataPtr> places {create_state(function, arena)};

    function->add(places[0], nullptr, 0, arena);
    function->add(places[0], nullptr, 0, arena);
    function->add(places[0], nullptr, 0, arena);

    ColumnInt64 single_result;
    function->insert_result_into_repeat(places[0], 0, single_result, arena);
    ASSERT_EQ(single_result.size(), 1);
    EXPECT_EQ(single_result.get_element(0), 0);

    ColumnInt64 result;
    std::vector<uint64_t> repeats {0};
    function->insert_result_into_repeat_vec(places, 0, repeats, result, places.size(), arena);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result.get_element(0), 0);

    destroy_states(function, places);
}

TEST(AggregateFunctionRepeatTest, CountNullableRepeatOutput) {
    Arena arena;
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto function = get_agg_function("count", {nullable_type}, false);
    std::vector<AggregateDataPtr> places {create_state(function, arena),
                                          create_state(function, arena)};

    auto input =
            ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    const IColumn* columns[] = {input.get()};
    function->add(places[0], columns, 0, arena);
    function->add(places[0], columns, 1, arena);
    function->add(places[0], columns, 2, arena);
    function->add(places[1], columns, 3, arena);
    function->add(places[1], columns, 4, arena);

    ColumnInt64 result;
    std::vector<uint64_t> repeats {5, 7};
    function->insert_result_into_repeat_vec(places, 0, repeats, result, places.size(), arena);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result.get_element(0), 10);
    EXPECT_EQ(result.get_element(1), 7);
    destroy_states(function, places);
}

TEST(AggregateFunctionRepeatTest, RepeatOutputHonorsStateOffset) {
    Arena arena;
    auto function = get_agg_function("sum", {std::make_shared<DataTypeInt32>()}, true);
    const size_t offset = function->align_of_data();
    std::vector<AggregateDataPtr> places {
            arena.aligned_alloc(offset + function->size_of_data(), function->align_of_data()),
            arena.aligned_alloc(offset + function->size_of_data(), function->align_of_data())};
    for (auto* place : places) {
        function->create(place + offset);
    }

    auto input = ColumnHelper::create_column<DataTypeInt32>({2, 3, 5, 7});
    const IColumn* columns[] = {input.get()};
    function->add(places[0] + offset, columns, 0, arena);
    function->add(places[0] + offset, columns, 1, arena);
    function->add(places[1] + offset, columns, 2, arena);
    function->add(places[1] + offset, columns, 3, arena);

    ColumnInt64 result;
    std::vector<uint64_t> repeats {4, 6};
    function->insert_result_into_repeat_vec(places, offset, repeats, result, places.size(), arena);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result.get_element(0), 20);
    EXPECT_EQ(result.get_element(1), 72);
    destroy_states(function, places, offset);
}

TEST(AggregateFunctionRepeatTest, SumRepeatOutput) {
    Arena arena;
    auto function = get_agg_function("sum", {std::make_shared<DataTypeInt32>()}, true);
    std::vector<AggregateDataPtr> places {create_state(function, arena),
                                          create_state(function, arena)};

    auto input = ColumnHelper::create_column<DataTypeInt32>({1, 2, 4, 6});
    const IColumn* columns[] = {input.get()};
    function->add(places[0], columns, 0, arena);
    function->add(places[0], columns, 1, arena);
    function->add(places[1], columns, 2, arena);
    function->add(places[1], columns, 3, arena);

    ColumnInt64 result;
    std::vector<uint64_t> repeats {3, 2};
    function->insert_result_into_repeat_vec(places, 0, repeats, result, places.size(), arena);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result.get_element(0), 9);
    EXPECT_EQ(result.get_element(1), 20);
    destroy_states(function, places);
}

TEST(AggregateFunctionRepeatTest, MinMaxRepeatOutput) {
    Arena arena;
    auto min_function = get_agg_function("min", {std::make_shared<DataTypeInt32>()}, true);
    auto max_function = get_agg_function("max", {std::make_shared<DataTypeInt32>()}, true);

    auto input = ColumnHelper::create_column<DataTypeInt32>({4, 2, 1, 7});
    const IColumn* columns[] = {input.get()};
    std::vector<uint64_t> repeats {10, 20};

    std::vector<AggregateDataPtr> min_places {create_state(min_function, arena),
                                              create_state(min_function, arena)};
    min_function->add(min_places[0], columns, 0, arena);
    min_function->add(min_places[0], columns, 1, arena);
    min_function->add(min_places[1], columns, 2, arena);
    min_function->add(min_places[1], columns, 3, arena);
    ColumnInt32 min_result;
    min_function->insert_result_into_repeat_vec(min_places, 0, repeats, min_result,
                                                min_places.size(), arena);

    ASSERT_EQ(min_result.size(), 2);
    EXPECT_EQ(min_result.get_element(0), 2);
    EXPECT_EQ(min_result.get_element(1), 1);
    destroy_states(min_function, min_places);

    std::vector<AggregateDataPtr> max_places {create_state(max_function, arena),
                                              create_state(max_function, arena)};
    max_function->add(max_places[0], columns, 0, arena);
    max_function->add(max_places[0], columns, 1, arena);
    max_function->add(max_places[1], columns, 2, arena);
    max_function->add(max_places[1], columns, 3, arena);
    ColumnInt32 max_result;
    max_function->insert_result_into_repeat_vec(max_places, 0, repeats, max_result,
                                                max_places.size(), arena);

    ASSERT_EQ(max_result.size(), 2);
    EXPECT_EQ(max_result.get_element(0), 4);
    EXPECT_EQ(max_result.get_element(1), 7);
    destroy_states(max_function, max_places);
}

TEST(AggregateFunctionRepeatTest, NullableV2SumRepeatOutput) {
    Arena arena;
    AggregateFunctionAttr attr;
    attr.enable_aggregate_function_null_v2 = true;
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto function = get_agg_function("sum", {nullable_type}, true, attr);
    std::vector<AggregateDataPtr> places {create_state(function, arena),
                                          create_state(function, arena),
                                          create_state(function, arena)};

    auto input = ColumnHelper::create_nullable_column<DataTypeInt32>({5, 0, 3}, {0, 1, 0});
    const IColumn* columns[] = {input.get()};
    function->add(places[0], columns, 0, arena);
    function->add(places[1], columns, 1, arena);
    function->add(places[2], columns, 2, arena);

    auto nested_result = ColumnInt64::create();
    auto null_map = ColumnUInt8::create();
    auto result = ColumnNullable::create(std::move(nested_result), std::move(null_map));
    std::vector<uint64_t> repeats {4, 7, 6};
    function->insert_result_into_repeat_vec(places, 0, repeats, *result, places.size(), arena);

    ASSERT_EQ(result->size(), 3);
    EXPECT_FALSE(result->is_null_at(0));
    EXPECT_TRUE(result->is_null_at(1));
    EXPECT_FALSE(result->is_null_at(2));
    EXPECT_EQ(assert_cast<const ColumnInt64&>(result->get_nested_column()).get_element(0), 20);
    EXPECT_EQ(assert_cast<const ColumnInt64&>(result->get_nested_column()).get_element(2), 18);

    auto single_nested_result = ColumnInt64::create();
    auto single_null_map = ColumnUInt8::create();
    auto single_result =
            ColumnNullable::create(std::move(single_nested_result), std::move(single_null_map));
    function->insert_result_into_repeat(places[0], 4, *single_result, arena);
    function->insert_result_into_repeat(places[1], 7, *single_result, arena);
    function->insert_result_into_repeat(places[2], 6, *single_result, arena);
    ASSERT_EQ(single_result->size(), 3);
    EXPECT_FALSE(single_result->is_null_at(0));
    EXPECT_TRUE(single_result->is_null_at(1));
    EXPECT_FALSE(single_result->is_null_at(2));
    EXPECT_EQ(assert_cast<const ColumnInt64&>(single_result->get_nested_column()).get_element(0),
              20);
    EXPECT_EQ(assert_cast<const ColumnInt64&>(single_result->get_nested_column()).get_element(2),
              18);

    destroy_states(function, places);
}

TEST(AggregateFunctionRepeatTest, NullableV2RepeatOutputAppendsAfterExistingRows) {
    Arena arena;
    AggregateFunctionAttr attr;
    attr.enable_aggregate_function_null_v2 = true;
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto function = get_agg_function("sum", {nullable_type}, true, attr);
    std::vector<AggregateDataPtr> places {create_state(function, arena),
                                          create_state(function, arena),
                                          create_state(function, arena)};

    auto input = ColumnHelper::create_nullable_column<DataTypeInt32>({5, 0, 3}, {0, 1, 0});
    const IColumn* columns[] = {input.get()};
    function->add(places[0], columns, 0, arena);
    function->add(places[1], columns, 1, arena);
    function->add(places[2], columns, 2, arena);

    auto nested_result = ColumnInt64::create();
    auto null_map = ColumnUInt8::create();
    auto result = ColumnNullable::create(std::move(nested_result), std::move(null_map));
    result->insert_default();

    std::vector<uint64_t> repeats {4, 7, 6};
    function->insert_result_into_repeat_vec(places, 0, repeats, *result, places.size(), arena);

    ASSERT_EQ(result->size(), 4);
    EXPECT_TRUE(result->is_null_at(0));
    EXPECT_FALSE(result->is_null_at(1));
    EXPECT_TRUE(result->is_null_at(2));
    EXPECT_FALSE(result->is_null_at(3));
    EXPECT_EQ(result->get_nested_column().size(), result->get_null_map_column().size());
    EXPECT_EQ(assert_cast<const ColumnInt64&>(result->get_nested_column()).get_element(1), 20);
    EXPECT_EQ(assert_cast<const ColumnInt64&>(result->get_nested_column()).get_element(3), 18);

    destroy_states(function, places);
}

TEST(AggregateFunctionRepeatTest, NullableV2SumRepeatOutputAllValidRun) {
    Arena arena;
    AggregateFunctionAttr attr;
    attr.enable_aggregate_function_null_v2 = true;
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto function = get_agg_function("sum", {nullable_type}, true, attr);
    std::vector<AggregateDataPtr> places {create_state(function, arena),
                                          create_state(function, arena),
                                          create_state(function, arena)};

    auto input = ColumnHelper::create_nullable_column<DataTypeInt32>({2, 4, 6}, {0, 0, 0});
    const IColumn* columns[] = {input.get()};
    function->add(places[0], columns, 0, arena);
    function->add(places[1], columns, 1, arena);
    function->add(places[2], columns, 2, arena);

    auto nested_result = ColumnInt64::create();
    auto null_map = ColumnUInt8::create();
    auto result = ColumnNullable::create(std::move(nested_result), std::move(null_map));
    std::vector<uint64_t> repeats {3, 5, 7};
    function->insert_result_into_repeat_vec(places, 0, repeats, *result, places.size(), arena);

    ASSERT_EQ(result->size(), 3);
    EXPECT_FALSE(result->is_null_at(0));
    EXPECT_FALSE(result->is_null_at(1));
    EXPECT_FALSE(result->is_null_at(2));
    const auto& nested_column = assert_cast<const ColumnInt64&>(result->get_nested_column());
    EXPECT_EQ(nested_column.get_element(0), 6);
    EXPECT_EQ(nested_column.get_element(1), 20);
    EXPECT_EQ(nested_column.get_element(2), 42);

    destroy_states(function, places);
}

TEST(AggregateFunctionRepeatTest, NullableV2SumRepeatOutputAllNull) {
    Arena arena;
    AggregateFunctionAttr attr;
    attr.enable_aggregate_function_null_v2 = true;
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto function = get_agg_function("sum", {nullable_type}, true, attr);
    std::vector<AggregateDataPtr> places {create_state(function, arena),
                                          create_state(function, arena)};

    auto input = ColumnHelper::create_nullable_column<DataTypeInt32>({0, 0}, {1, 1});
    const IColumn* columns[] = {input.get()};
    function->add(places[0], columns, 0, arena);
    function->add(places[1], columns, 1, arena);

    auto nested_result = ColumnInt64::create();
    auto null_map = ColumnUInt8::create();
    auto result = ColumnNullable::create(std::move(nested_result), std::move(null_map));
    std::vector<uint64_t> repeats {3, 5};
    function->insert_result_into_repeat_vec(places, 0, repeats, *result, places.size(), arena);

    ASSERT_EQ(result->size(), 2);
    EXPECT_TRUE(result->is_null_at(0));
    EXPECT_TRUE(result->is_null_at(1));
    EXPECT_EQ(result->get_nested_column().size(), result->get_null_map_column().size());

    destroy_states(function, places);
}

TEST(AggregateFunctionRepeatTest, NullableV2MinMaxRepeatOutput) {
    Arena arena;
    AggregateFunctionAttr attr;
    attr.enable_aggregate_function_null_v2 = true;
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto min_function = get_agg_function("min", {nullable_type}, true, attr);
    auto max_function = get_agg_function("max", {nullable_type}, true, attr);

    auto input =
            ColumnHelper::create_nullable_column<DataTypeInt32>({5, 0, 9, 1, 0}, {0, 1, 0, 0, 1});
    const IColumn* columns[] = {input.get()};
    std::vector<uint64_t> repeats {3, 7, 11};

    std::vector<AggregateDataPtr> min_places {create_state(min_function, arena),
                                              create_state(min_function, arena),
                                              create_state(min_function, arena)};
    min_function->add(min_places[0], columns, 0, arena);
    min_function->add(min_places[1], columns, 1, arena);
    min_function->add(min_places[2], columns, 2, arena);
    min_function->add(min_places[2], columns, 3, arena);

    auto min_nested_result = ColumnInt32::create();
    auto min_null_map = ColumnUInt8::create();
    auto min_result = ColumnNullable::create(std::move(min_nested_result), std::move(min_null_map));
    min_function->insert_result_into_repeat_vec(min_places, 0, repeats, *min_result,
                                                min_places.size(), arena);

    ASSERT_EQ(min_result->size(), 3);
    EXPECT_FALSE(min_result->is_null_at(0));
    EXPECT_TRUE(min_result->is_null_at(1));
    EXPECT_FALSE(min_result->is_null_at(2));
    EXPECT_EQ(assert_cast<const ColumnInt32&>(min_result->get_nested_column()).get_element(0), 5);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(min_result->get_nested_column()).get_element(2), 1);
    destroy_states(min_function, min_places);

    std::vector<AggregateDataPtr> max_places {create_state(max_function, arena),
                                              create_state(max_function, arena),
                                              create_state(max_function, arena)};
    max_function->add(max_places[0], columns, 0, arena);
    max_function->add(max_places[1], columns, 4, arena);
    max_function->add(max_places[2], columns, 2, arena);
    max_function->add(max_places[2], columns, 3, arena);

    auto max_nested_result = ColumnInt32::create();
    auto max_null_map = ColumnUInt8::create();
    auto max_result = ColumnNullable::create(std::move(max_nested_result), std::move(max_null_map));
    max_function->insert_result_into_repeat_vec(max_places, 0, repeats, *max_result,
                                                max_places.size(), arena);

    ASSERT_EQ(max_result->size(), 3);
    EXPECT_FALSE(max_result->is_null_at(0));
    EXPECT_TRUE(max_result->is_null_at(1));
    EXPECT_FALSE(max_result->is_null_at(2));
    EXPECT_EQ(assert_cast<const ColumnInt32&>(max_result->get_nested_column()).get_element(0), 5);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(max_result->get_nested_column()).get_element(2), 9);
    destroy_states(max_function, max_places);
}

} // namespace
} // namespace doris

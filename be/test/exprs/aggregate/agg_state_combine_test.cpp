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

#include "agent/be_exec_version_manager.h"
#include "core/column/column_array.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/aggregate_function_state_combine.h"
#include "exprs/aggregate/aggregate_function_state_merge.h"
#include "exprs/aggregate/aggregate_function_state_union.h"
#include "testutil/column_helper.h"

namespace doris {

class AggregateStateCombineTest : public testing::Test {
protected:
    static DataTypePtr create_avg_state_type(const DataTypePtr& argument_type) {
        return std::make_shared<DataTypeAggState>(DataTypes {argument_type}, true, "avg",
                                                  BeExecVersionManager::get_newest_version());
    }

    static ColumnPtr combine(AggregateFunctionPtr combine_function, const DataTypePtr& state_type,
                             const ColumnPtr& input_column, Arena& arena) {
        auto* place =
                reinterpret_cast<AggregateDataPtr>(arena.alloc(combine_function->size_of_data()));
        combine_function->create(place);

        const IColumn* columns[] = {input_column.get()};
        combine_function->add_batch_single_place(input_column->size(), place, columns, arena);

        auto result = state_type->create_column();
        combine_function->insert_result_into(place, *result);
        combine_function->destroy(place);
        return result;
    }

    static ColumnPtr merge(AggregateFunctionPtr nested_function, const DataTypePtr& state_type,
                           const ColumnPtr& state_column, Arena& arena) {
        auto merge_function = AggregateStateMerge::create(nested_function, DataTypes {state_type},
                                                          nested_function->get_return_type());
        auto* place =
                reinterpret_cast<AggregateDataPtr>(arena.alloc(merge_function->size_of_data()));
        merge_function->create(place);

        const IColumn* columns[] = {state_column.get()};
        merge_function->add_batch_single_place(state_column->size(), place, columns, arena);

        auto result = nested_function->get_return_type()->create_column();
        merge_function->insert_result_into(place, *result);
        merge_function->destroy(place);
        return result;
    }
};

TEST_F(AggregateStateCombineTest, AvgStateCompatibleWithStateUnion) {
    auto argument_type = std::make_shared<DataTypeInt64>();
    auto state_type = create_avg_state_type(argument_type);
    auto nested_function =
            assert_cast<const DataTypeAggState*>(state_type.get())->get_nested_function();
    auto combine_function =
            AggregateStateCombine::create(nested_function, DataTypes {argument_type}, state_type);
    auto input_column = ColumnHelper::create_column<DataTypeInt64>({1, 2, 3});

    Arena arena;
    auto combined_state = combine(combine_function, state_type, input_column, arena);

    auto partial_states = combine_function->create_serialize_column();
    EXPECT_NE(check_and_get_column<ColumnFixedLengthObject>(*partial_states), nullptr);
    EXPECT_TRUE(combine_function->get_serialized_type()->equals(
            *nested_function->get_serialized_type()));
    const IColumn* partial_input_columns[] = {input_column.get()};
    combine_function->streaming_agg_serialize_to_column(partial_input_columns, partial_states,
                                                        input_column->size(), arena);
    auto* partial_place =
            reinterpret_cast<AggregateDataPtr>(arena.alloc(combine_function->size_of_data()));
    combine_function->create(partial_place);
    combine_function->deserialize_and_merge_from_column_range(partial_place, *partial_states, 0,
                                                              partial_states->size() - 1, arena);
    auto partial_state = state_type->create_column();
    combine_function->insert_result_into(partial_place, *partial_state);
    auto partial_state_without_key = state_type->create_column();
    combine_function->serialize_without_key_to_column(partial_place, *partial_state_without_key);
    combine_function->destroy(partial_place);

    auto row_states = nested_function->create_serialize_column();
    const IColumn* input_columns[] = {input_column.get()};
    nested_function->streaming_agg_serialize_to_column(input_columns, row_states,
                                                       input_column->size(), arena);
    auto union_function =
            AggregateStateUnion::create(nested_function, DataTypes {state_type}, state_type);
    auto* union_place =
            reinterpret_cast<AggregateDataPtr>(arena.alloc(union_function->size_of_data()));
    union_function->create(union_place);
    const IColumn* state_columns[] = {row_states.get()};
    union_function->add_batch_single_place(row_states->size(), union_place, state_columns, arena);
    auto union_state = state_type->create_column();
    union_function->insert_result_into(union_place, *union_state);
    union_function->destroy(union_place);

    ASSERT_EQ(combined_state->size(), 1);
    ASSERT_EQ(partial_state->size(), 1);
    ASSERT_EQ(partial_state_without_key->size(), 1);
    ASSERT_EQ(union_state->size(), 1);
    EXPECT_EQ(combined_state->get_data_at(0), partial_state->get_data_at(0));
    EXPECT_EQ(combined_state->get_data_at(0), partial_state_without_key->get_data_at(0));
    EXPECT_EQ(combined_state->get_data_at(0), union_state->get_data_at(0));

    auto result = merge(nested_function, state_type, combined_state, arena);
    const auto& result_column = assert_cast<const ColumnFloat64&>(*result);
    ASSERT_EQ(result_column.size(), 1);
    EXPECT_DOUBLE_EQ(result_column.get_data()[0], 2.0);
}

TEST_F(AggregateStateCombineTest, AvgNullableInput) {
    auto argument_type = make_nullable(std::make_shared<DataTypeInt64>());
    auto state_type = create_avg_state_type(argument_type);
    auto nested_function =
            assert_cast<const DataTypeAggState*>(state_type.get())->get_nested_function();
    auto combine_function =
            AggregateStateCombine::create(nested_function, DataTypes {argument_type}, state_type);
    auto input_column = ColumnHelper::create_nullable_column<DataTypeInt64>({1, 0, 3}, {0, 1, 0});

    Arena arena;
    auto combined_state = combine(combine_function, state_type, input_column, arena);
    auto result = merge(nested_function, state_type, combined_state, arena);

    const auto& nullable_result = assert_cast<const ColumnNullable&>(*result);
    ASSERT_EQ(nullable_result.size(), 1);
    ASSERT_FALSE(nullable_result.is_null_at(0));
    const auto& result_column =
            assert_cast<const ColumnFloat64&>(nullable_result.get_nested_column());
    EXPECT_DOUBLE_EQ(result_column.get_data()[0], 2.0);

    auto all_null_input = ColumnHelper::create_nullable_column<DataTypeInt64>({0, 0}, {1, 1});
    auto all_null_state = combine(combine_function, state_type, all_null_input, arena);
    auto all_null_result = merge(nested_function, state_type, all_null_state, arena);
    const auto& nullable_all_null_result = assert_cast<const ColumnNullable&>(*all_null_result);
    ASSERT_EQ(nullable_all_null_result.size(), 1);
    EXPECT_TRUE(nullable_all_null_result.is_null_at(0));
}

TEST_F(AggregateStateCombineTest, CountNullableInputPreservesGroupedStates) {
    auto argument_type = make_nullable(std::make_shared<DataTypeInt64>());
    auto state_type = std::make_shared<DataTypeAggState>(
            DataTypes {argument_type}, false, "count", BeExecVersionManager::get_newest_version());
    auto nested_function = state_type->get_nested_function();
    auto combine_function =
            AggregateStateCombine::create(nested_function, DataTypes {argument_type}, state_type);
    auto input_column =
            ColumnHelper::create_nullable_column<DataTypeInt64>({10, 0, 20, 30}, {0, 1, 0, 0});

    Arena arena;
    std::vector<AggregateDataPtr> combine_places(2);
    for (auto& place : combine_places) {
        place = reinterpret_cast<AggregateDataPtr>(arena.alloc(combine_function->size_of_data()));
        combine_function->create(place);
    }
    AggregateDataPtr row_places[] = {combine_places[0], combine_places[0], combine_places[1],
                                     combine_places[1]};
    const IColumn* input_columns[] = {input_column.get()};
    combine_function->add_batch(input_column->size(), row_places, 0, input_columns, arena, false);

    auto combined_states = state_type->create_column();
    combine_function->insert_result_into_vec(combine_places, 0, *combined_states,
                                             combine_places.size());
    ASSERT_EQ(combined_states->size(), 2);
    combine_function->insert_result_into(combine_places[0], *combined_states);
    ASSERT_EQ(combined_states->size(), 3);

    auto merge_function = AggregateStateMerge::create(nested_function, DataTypes {state_type},
                                                      nested_function->get_return_type());
    std::vector<AggregateDataPtr> merge_places(combined_states->size());
    for (auto& place : merge_places) {
        place = reinterpret_cast<AggregateDataPtr>(arena.alloc(merge_function->size_of_data()));
        merge_function->create(place);
    }
    const IColumn* state_columns[] = {combined_states.get()};
    merge_function->add_batch(combined_states->size(), merge_places.data(), 0, state_columns, arena,
                              false);

    auto result = nested_function->get_return_type()->create_column();
    merge_function->insert_result_into_vec(merge_places, 0, *result, merge_places.size());
    const auto& count_result = assert_cast<const ColumnInt64&>(*result);
    ASSERT_EQ(count_result.size(), 3);
    EXPECT_EQ(count_result.get_data()[0], 1);
    EXPECT_EQ(count_result.get_data()[1], 2);
    EXPECT_EQ(count_result.get_data()[2], 1);

    for (auto* place : combine_places) {
        combine_function->destroy(place);
    }
    for (auto* place : merge_places) {
        merge_function->destroy(place);
    }
}

TEST_F(AggregateStateCombineTest, LargeGroupedArrayState) {
    constexpr size_t group_count = 8;
    constexpr size_t rows_per_group = 4096;
    constexpr size_t row_count = group_count * rows_per_group;

    auto argument_type = std::make_shared<DataTypeInt64>();
    auto state_type =
            std::make_shared<DataTypeAggState>(DataTypes {argument_type}, false, "array_agg",
                                               BeExecVersionManager::get_newest_version());
    auto nested_function = state_type->get_nested_function();
    auto combine_function =
            AggregateStateCombine::create(nested_function, DataTypes {argument_type}, state_type);

    std::vector<Int64> values(row_count);
    std::vector<AggregateDataPtr> row_places(row_count);
    std::vector<AggregateDataPtr> combine_places(group_count);
    Arena arena;
    for (size_t group = 0; group < group_count; ++group) {
        combine_places[group] =
                reinterpret_cast<AggregateDataPtr>(arena.alloc(combine_function->size_of_data()));
        combine_function->create(combine_places[group]);
        for (size_t row = 0; row < rows_per_group; ++row) {
            const size_t index = group * rows_per_group + row;
            values[index] = static_cast<Int64>(index);
            row_places[index] = combine_places[group];
        }
    }

    auto input_column = ColumnHelper::create_column<DataTypeInt64>(values);
    const IColumn* input_columns[] = {input_column.get()};
    combine_function->add_batch(row_count, row_places.data(), 0, input_columns, arena, false);

    auto combined_states = state_type->create_column();
    combine_function->insert_result_into_vec(combine_places, 0, *combined_states, group_count);
    ASSERT_EQ(combined_states->size(), group_count);

    auto merge_function = AggregateStateMerge::create(nested_function, DataTypes {state_type},
                                                      nested_function->get_return_type());
    std::vector<AggregateDataPtr> merge_places(group_count);
    for (auto& place : merge_places) {
        place = reinterpret_cast<AggregateDataPtr>(arena.alloc(merge_function->size_of_data()));
        merge_function->create(place);
    }
    const IColumn* state_columns[] = {combined_states.get()};
    merge_function->add_batch(group_count, merge_places.data(), 0, state_columns, arena, false);

    auto result = nested_function->get_return_type()->create_column();
    merge_function->insert_result_into_vec(merge_places, 0, *result, group_count);
    const auto& array_result = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(array_result.size(), group_count);
    ASSERT_EQ(array_result.get_data().size(), row_count);
    for (size_t group = 0; group < group_count; ++group) {
        EXPECT_EQ(array_result.get_offsets()[group], (group + 1) * rows_per_group);
    }

    for (auto* place : combine_places) {
        combine_function->destroy(place);
    }
    for (auto* place : merge_places) {
        merge_function->destroy(place);
    }
}

} // namespace doris

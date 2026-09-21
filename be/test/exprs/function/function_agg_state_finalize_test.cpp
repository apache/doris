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

#include "exprs/function/function_agg_state_finalize.h"

#include <gtest/gtest.h>

#include "agent/be_exec_version_manager.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/aggregate/aggregate_function_state_merge.h"
#include "testutil/column_helper.h"

namespace doris {

class FunctionAggStateFinalizeTest : public testing::Test {
protected:
    static std::shared_ptr<DataTypeAggState> state_type(const std::string& name,
                                                        const DataTypePtr& argument_type,
                                                        bool result_nullable = true) {
        return std::make_shared<DataTypeAggState>(DataTypes {argument_type}, result_nullable, name,
                                                  BeExecVersionManager::get_newest_version());
    }

    static void append_state(const AggregateFunctionPtr& function, const ColumnPtr& input,
                             IColumn& states) {
        ASSERT_EQ(function->get_argument_types().size(), 1);
        Arena arena;
        auto* place = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
        function->create(place);
        DEFER(function->destroy(place));
        const IColumn* columns[] = {input.get()};
        function->check_input_columns_type(columns);
        function->add_batch_single_place(input->size(), place, columns, arena);
        // Some aggregates resize their no-key output instead of appending to it.
        auto serialized = function->create_serialize_column();
        function->serialize_without_key_to_column(place, *serialized);
        ASSERT_EQ(serialized->size(), 1);
        states.insert_range_from(*serialized, 0, 1);
    }

    static ColumnPtr finalize(const DataTypePtr& type, const ColumnPtr& states) {
        auto nested = assert_cast<const DataTypeAggState*>(remove_nullable(type).get())
                              ->get_nested_function();
        auto result_type = nested->get_return_type();
        if (type->is_nullable()) {
            result_type = make_nullable(result_type);
        }
        auto function = FunctionAggStateFinalize::create({type}, result_type, nested);
        Block block {{states, type, "state"}, {nullptr, result_type, "result"}};
        EXPECT_TRUE(function->execute(nullptr, block, {0}, 1, states->size()).ok());
        return block.get_by_position(1).column;
    }
};

TEST_F(FunctionAggStateFinalizeTest, FinalizesEachRowIndependently) {
    auto type = state_type("avg", std::make_shared<DataTypeInt64>());
    auto states = type->create_column();
    append_state(type->get_nested_function(), ColumnHelper::create_column<DataTypeInt64>({1, 3}),
                 *states);
    append_state(type->get_nested_function(), ColumnHelper::create_column<DataTypeInt64>({10}),
                 *states);
    ColumnPtr serialized = std::move(states);
    auto result = finalize(type, serialized);
    const auto& values = assert_cast<const ColumnFloat64&>(*result).get_data();
    ASSERT_EQ(values.size(), 2);
    EXPECT_DOUBLE_EQ(values[0], 2);
    EXPECT_DOUBLE_EQ(values[1], 10);
    // Repeated execution must not consume or mutate the serialized state.
    EXPECT_EQ(result->compare_at(0, 0, *finalize(type, serialized), 1), 0);
}

TEST_F(FunctionAggStateFinalizeTest, EmptyNonNullableAvgMatchesMerge) {
    auto type = state_type("avg", std::make_shared<DataTypeInt64>());
    auto states = type->create_column();
    append_state(type->get_nested_function(), ColumnHelper::create_column<DataTypeInt64>({}),
                 *states);
    ColumnPtr serialized = std::move(states);
    auto result = finalize(type, serialized);
    auto nested = type->get_nested_function();
    auto merge = AggregateStateMerge::create(nested, {type}, nested->get_return_type());
    Arena arena;
    auto* place = arena.aligned_alloc(merge->size_of_data(), merge->align_of_data());
    merge->create(place);
    DEFER(merge->destroy(place));
    const IColumn* columns[] = {serialized.get()};
    merge->add(place, columns, 0, arena);
    auto expected = nested->get_return_type()->create_column();
    merge->check_result_column_type(*expected);
    merge->insert_result_into(place, *expected);
    EXPECT_EQ(result->compare_at(0, 0, *expected, 1), 0);
}

TEST_F(FunctionAggStateFinalizeTest, NativeSerializedColumnsAndEmptyCount) {
    for (const auto& name : {"count", "sum", "min", "max"}) {
        SCOPED_TRACE(name);
        auto type =
                state_type(name, std::make_shared<DataTypeInt64>(), name != std::string("count"));
        auto states = type->create_column();
        append_state(type->get_nested_function(),
                     ColumnHelper::create_column<DataTypeInt64>({2, 4}), *states);
        append_state(type->get_nested_function(), ColumnHelper::create_column<DataTypeInt64>({}),
                     *states);
        auto result = finalize(type, std::move(states));
        EXPECT_EQ(result->get_int(0), name == std::string("count") ? 2
                                      : name == std::string("sum") ? 6
                                      : name == std::string("min") ? 2
                                                                   : 4);
        // Empty non-nullable states have the existing aggregate's identity value.
        auto expected = type->get_nested_function()->get_return_type()->create_column();
        Arena arena;
        auto function = type->get_nested_function();
        auto* place = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
        function->create(place);
        DEFER(function->destroy(place));
        function->check_result_column_type(*expected);
        function->insert_result_into(place, *expected);
        EXPECT_EQ(result->compare_at(1, 0, *expected, 1), 0);
    }
}

// GTest assertion macros inflate complexity in this table-driven check.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(FunctionAggStateFinalizeTest, NullableInputsAndEmptyStates) {
    for (const auto& name : {"avg", "sum", "min", "max", "count"}) {
        SCOPED_TRACE(name);
        auto type = state_type(name, make_nullable(std::make_shared<DataTypeInt64>()),
                               name != std::string("count"));
        auto states = type->create_column();
        append_state(type->get_nested_function(),
                     ColumnHelper::create_nullable_column<DataTypeInt64>({2, 0, 4}, {0, 1, 0}),
                     *states);
        append_state(type->get_nested_function(),
                     ColumnHelper::create_nullable_column<DataTypeInt64>({0}, {1}), *states);
        append_state(type->get_nested_function(),
                     ColumnHelper::create_nullable_column<DataTypeInt64>({}, {}), *states);
        auto result = finalize(type, std::move(states));
        ASSERT_EQ(result->size(), 3);
        EXPECT_FALSE(result->is_null_at(0));
        if (name == std::string("count")) {
            EXPECT_EQ(result->get_int(0), 2);
            EXPECT_EQ(result->get_int(1), 0);
            EXPECT_EQ(result->get_int(2), 0);
        } else {
            EXPECT_TRUE(result->is_null_at(1));
            EXPECT_TRUE(result->is_null_at(2));
        }
    }
}

TEST_F(FunctionAggStateFinalizeTest, OuterNullAndConstantStates) {
    auto type = state_type("avg", make_nullable(std::make_shared<DataTypeInt64>()));
    auto states = type->create_column();
    append_state(type->get_nested_function(),
                 ColumnHelper::create_nullable_column<DataTypeInt64>({2, 4}, {0, 0}), *states);
    ColumnPtr constant = ColumnConst::create(states->clone_resized(1), 5);
    auto result = finalize(type, constant);
    EXPECT_TRUE(is_column_const(*result));
    EXPECT_EQ(result->size(), 5);
    // NULL string payload is deliberately empty, and must not be deserialized.
    states->insert_default();
    ColumnPtr nullable_states = ColumnNullable::create(
            std::move(states), ColumnHelper::create_column<DataTypeUInt8>({0, 1}));
    result = finalize(make_nullable(type), nullable_states);
    ASSERT_EQ(result->size(), 2);
    EXPECT_FALSE(result->is_null_at(0));
    EXPECT_TRUE(result->is_null_at(1));
    auto null_state = nullable_states->clone_empty();
    null_state->insert_from(*nullable_states, 1);
    ColumnPtr null_constant = ColumnConst::create(std::move(null_state), 7);
    result = finalize(make_nullable(type), null_constant);
    EXPECT_EQ(result->size(), 7);
    EXPECT_TRUE(result->is_null_at(0));
}

TEST_F(FunctionAggStateFinalizeTest, VariableLengthArrayResultsOwnTheirData) {
    auto type = state_type("array_agg", std::make_shared<DataTypeString>(), false);
    auto states = type->create_column();
    const std::string large(8192, 'x');
    for (size_t group = 0; group < 16; ++group) {
        append_state(type->get_nested_function(),
                     ColumnHelper::create_column<DataTypeString>({large, std::to_string(group)}),
                     *states);
    }
    auto result = finalize(type, std::move(states));
    const auto& arrays = assert_cast<const ColumnArray&>(*result);
    const auto& strings = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(arrays.get_data()).get_nested_column());
    ASSERT_EQ(arrays.size(), 16);
    for (size_t group = 0; group < 16; ++group) {
        EXPECT_EQ(arrays.get_offsets()[group], (group + 1) * 2);
        EXPECT_EQ(strings.get_data_at(group * 2).to_string(), large);
        EXPECT_EQ(strings.get_data_at(group * 2 + 1).to_string(), std::to_string(group));
    }
}

TEST_F(FunctionAggStateFinalizeTest, EmptyBlock) {
    auto type = state_type("avg", std::make_shared<DataTypeInt64>());
    auto result = finalize(type, type->create_column());
    EXPECT_EQ(result->size(), 0);
}

} // namespace doris

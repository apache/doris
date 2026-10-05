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
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_uuid.h"
#include "exprs/aggregate/agg_function_test.h"
#include "exprs/aggregate/aggregate_function_state_merge.h"
#include "exprs/aggregate/aggregate_function_state_union.h"

namespace doris {

struct AggregateFunctionCountTest : public AggregateFunctiontest {};

TEST_F(AggregateFunctionCountTest, test_int64) {
    create_agg("count", false, {std::make_shared<DataTypeInt64>()},
               std::make_shared<DataTypeInt64>());

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3})}),
            ColumnHelper::create_column_with_name<DataTypeInt64>({3}));
}

TEST_F(AggregateFunctionCountTest, test_nullable_int64_without_null) {
    create_agg("count", false,
               {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
               std::make_shared<DataTypeInt64>());

    execute(Block({ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({1, 2, 3, 4},
                                                                                 {0, 0, 0, 0})}),
            ColumnHelper::create_column_with_name<DataTypeInt64>({4}));
}

TEST_F(AggregateFunctionCountTest, test_nullable_int64_with_null) {
    create_agg("count", false,
               {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
               std::make_shared<DataTypeInt64>());

    execute(Block({ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5},
                                                                                 {0, 1, 0, 1, 0})}),
            ColumnHelper::create_column_with_name<DataTypeInt64>({3}));
}

TEST_F(AggregateFunctionCountTest, NullableStateUnionPreservesGroupedStates) {
    const std::vector<ColumnWithTypeAndName> inputs {
            ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({0, 10, 0, 20, 30, 0},
                                                                          {1, 0, 1, 0, 0, 1}),
            ColumnHelper::create_nullable_column_with_name<DataTypeUUID>(
                    {0, 1, 0, UInt128(1) << 127, ~UInt128(0), 0}, {1, 0, 1, 0, 0, 1})};
    for (const auto& input : inputs) {
        SCOPED_TRACE(input.type->get_name());
        auto state_type = std::make_shared<DataTypeAggState>(
                DataTypes {input.type}, false, "count", BeExecVersionManager::get_newest_version());
        auto nested_function = state_type->get_nested_function();
        auto union_function =
                AggregateStateUnion::create(nested_function, DataTypes {state_type}, state_type);
        Arena arena;
        auto row_states = nested_function->create_serialize_column();
        const IColumn* input_columns[] = {input.column.get()};
        nested_function->streaming_agg_serialize_to_column(input_columns, row_states,
                                                           input.column->size(), arena);

        std::vector<AggregateDataPtr> places(3);
        for (auto& place : places) {
            place = arena.aligned_alloc(union_function->size_of_data(),
                                        union_function->align_of_data());
            union_function->create(place);
        }
        DEFER({
            for (auto* place : places) {
                union_function->destroy(place);
            }
        });
        // The groups contain only NULLs, one non-NULL value, and two non-NULL values.
        AggregateDataPtr row_places[] = {places[0], places[1], places[0],
                                         places[2], places[2], places[1]};
        const IColumn* state_columns[] = {row_states.get()};
        union_function->add_batch(row_states->size(), row_places, 0, state_columns, arena, false);

        auto union_states = state_type->create_column();
        union_function->insert_result_into_vec(places, 0, *union_states, places.size());
        ASSERT_EQ(union_states->size(), 3);
        union_function->insert_result_into(places[1], *union_states);
        ASSERT_EQ(union_states->size(), 4);
        union_function->insert_result_into_vec(places, 0, *union_states, places.size());
        ASSERT_EQ(union_states->size(), 7);

        auto merge_function = AggregateStateMerge::create(nested_function, DataTypes {state_type},
                                                          nested_function->get_return_type());
        auto* merge_place = arena.aligned_alloc(merge_function->size_of_data(),
                                                merge_function->align_of_data());
        merge_function->create(merge_place);
        DEFER({ merge_function->destroy(merge_place); });
        auto result = merge_function->get_return_type()->create_column();
        const IColumn* union_columns[] = {union_states.get()};
        for (size_t row = 0; row < union_states->size(); ++row) {
            merge_function->reset(merge_place);
            merge_function->add(merge_place, union_columns, row, arena);
            merge_function->insert_result_into(merge_place, *result);
        }
        EXPECT_TRUE(ColumnHelper::column_equal(
                result->get_ptr(),
                ColumnHelper::create_column<DataTypeInt64>({0, 1, 2, 1, 0, 1, 2})));
    }
}
} // namespace doris

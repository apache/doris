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

#include "common/object_pool.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_agg_fn_evaluator.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(AggFnEvaluatorTest, test_single) {
    ObjectPool pool;
    [[maybe_unused]] auto* agg_fn = create_mock_agg_fn_evaluator(pool);
    Arena arena;

    Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});

    // init place
    AggregateDataPtr place = reinterpret_cast<vectorized::AggregateDataPtr>(
            arena.alloc(agg_fn->function()->size_of_data()));

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
    AggregateDataPtr place1 = reinterpret_cast<vectorized::AggregateDataPtr>(
            arena.alloc(agg_fn->function()->size_of_data()));

    AggregateDataPtr place2 = reinterpret_cast<vectorized::AggregateDataPtr>(
            arena.alloc(agg_fn->function()->size_of_data()));

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
    AggregateDataPtr place1 = reinterpret_cast<vectorized::AggregateDataPtr>(
            arena.alloc(agg_fn->function()->size_of_data()));

    AggregateDataPtr place2 = reinterpret_cast<vectorized::AggregateDataPtr>(
            arena.alloc(agg_fn->function()->size_of_data()));

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

} // namespace doris::vectorized

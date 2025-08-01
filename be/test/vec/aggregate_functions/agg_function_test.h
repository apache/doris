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
#include "testutil/column_helper.h"
#include "testutil/mock/mock_agg_fn_evaluator.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::vectorized {

struct AggregateFunctiontest : public testing::Test {
    void SetUp() override {}

    void execute(Block block, ColumnWithTypeAndName expected_column) const {
        execute_single(block, expected_column);
        execute_merge(block, expected_column);
        execute_more(block, expected_column);
    }

    void create_agg(const std::string& name, bool result_nullable, DataTypes args_type) {
        agg_fn = create_agg_fn(pool, name, args_type, result_nullable);
    }

private:
    void execute_single(Block block, ColumnWithTypeAndName expected_column) const {
        Arena arena;
        auto* place = reinterpret_cast<vectorized::AggregateDataPtr>(
                arena.alloc(agg_fn->function()->size_of_data()));

        agg_fn->create(place);

        {
            auto st = agg_fn->execute_single_add(&block, place, arena);
            EXPECT_TRUE(st.ok()) << st.msg();

            MutableColumnPtr result_column = expected_column.column->clone_empty();

            agg_fn->insert_result_info(place, result_column.get());

            for (int i = 0; i < result_column->size(); i++) {
                std::cout << expected_column.type->to_string(*result_column, i) << std::endl;
            }

            EXPECT_TRUE(
                    ColumnHelper::column_equal(std::move(result_column), expected_column.column));
        }

        // reset place
        agg_fn->reset(place);

        {
            auto st = agg_fn->execute_single_add(&block, place, arena);
            EXPECT_TRUE(st.ok()) << st.msg();

            MutableColumnPtr result_column = expected_column.column->clone_empty();

            agg_fn->insert_result_info(place, result_column.get());

            for (int i = 0; i < result_column->size(); i++) {
                std::cout << expected_column.type->to_string(*result_column, i) << std::endl;
            }

            EXPECT_TRUE(
                    ColumnHelper::column_equal(std::move(result_column), expected_column.column));
        }

        agg_fn->destroy(place);
    }

    void execute_merge(Block block, ColumnWithTypeAndName expected_column) const {
        Arena arena;
        MutableColumnPtr serialize_column = agg_fn->function()->create_serialize_column();

        {
            auto* place = reinterpret_cast<vectorized::AggregateDataPtr>(
                    arena.alloc(agg_fn->function()->size_of_data()));

            agg_fn->create(place);
            Defer defer([&]() { agg_fn->destroy(place); });

            auto st = agg_fn->execute_single_add(&block, place, arena);
            EXPECT_TRUE(st.ok()) << st.msg();

            agg_fn->function()->serialize_without_key_to_column(place, *serialize_column);
        }

        {
            auto* place = reinterpret_cast<vectorized::AggregateDataPtr>(
                    arena.alloc(agg_fn->function()->size_of_data()));

            agg_fn->create(place);
            Defer defer([&]() { agg_fn->destroy(place); });

            agg_fn->function()->deserialize_and_merge_from_column(place, *serialize_column, arena);

            MutableColumnPtr result_column = expected_column.column->clone_empty();

            agg_fn->insert_result_info(place, result_column.get());

            for (int i = 0; i < result_column->size(); i++) {
                std::cout << expected_column.type->to_string(*result_column, i) << std::endl;
            }

            EXPECT_TRUE(
                    ColumnHelper::column_equal(std::move(result_column), expected_column.column));
        }
    }

    void execute_more(Block block, ColumnWithTypeAndName expected_column) const {
        auto check_result = [&](vectorized::AggregateDataPtr place) {
            MutableColumnPtr result_column = expected_column.column->clone_empty();
            agg_fn->insert_result_info(place, result_column.get());
            for (int i = 0; i < result_column->size(); i++) {
                std::cout << expected_column.type->to_string(*result_column, i) << std::endl;
            }

            EXPECT_TRUE(
                    ColumnHelper::column_equal(std::move(result_column), expected_column.column));
        };

        { // serialize_to_column deserialize_from_column
            MutableColumnPtr serialize_column = agg_fn->function()->create_serialize_column();
            {
                Arena arena;
                auto* place = reinterpret_cast<vectorized::AggregateDataPtr>(
                        arena.alloc(agg_fn->function()->size_of_data()));

                agg_fn->create(place);
                Defer defer([&]() { agg_fn->destroy(place); });

                auto st = agg_fn->execute_single_add(&block, place, arena);
                EXPECT_TRUE(st.ok()) << st.msg();
                std::vector<AggregateDataPtr> places {place};
                agg_fn->function()->serialize_to_column(places, 0, serialize_column, 1);
            }

            {
                Arena arena;
                auto* place = reinterpret_cast<vectorized::AggregateDataPtr>(
                        arena.alloc(agg_fn->function()->size_of_data()));

                agg_fn->create(place);
                Defer defer([&]() { agg_fn->destroy(place); });
                agg_fn->function()->deserialize_from_column(place, *serialize_column, arena, 1);

                check_result(place);
            }
        }

        { // streaming_agg_serialize_to_column deserialize_and_merge_from_column_range
            MutableColumnPtr serialize_column = agg_fn->function()->create_serialize_column();
            Arena arena;
            {
                EXPECT_TRUE(agg_fn->streaming_agg_serialize_to_column(&block, serialize_column,
                                                                      block.rows(), arena));
            }

            {
                auto* place = reinterpret_cast<vectorized::AggregateDataPtr>(
                        arena.alloc(agg_fn->function()->size_of_data()));

                agg_fn->create(place);
                Defer defer([&]() { agg_fn->destroy(place); });
                agg_fn->function()->deserialize_and_merge_from_column_range(
                        place, *serialize_column, 0, block.rows() - 1, arena);

                check_result(place);
            }
        }

        { // deserialize_and_merge_vec
            MutableColumnPtr serialize_column = agg_fn->function()->create_serialize_column();
            Arena arena;
            {
                auto* place = reinterpret_cast<vectorized::AggregateDataPtr>(
                        arena.alloc(agg_fn->function()->size_of_data()));

                agg_fn->create(place);
                Defer defer([&]() { agg_fn->destroy(place); });

                auto st = agg_fn->execute_single_add(&block, place, arena);
                EXPECT_TRUE(st.ok()) << st.msg();
                std::vector<AggregateDataPtr> places {place};
                agg_fn->function()->serialize_to_column(places, 0, serialize_column, 1);
            }
            {
                auto* place1 = reinterpret_cast<vectorized::AggregateDataPtr>(
                        arena.alloc(agg_fn->function()->size_of_data()));
                agg_fn->create(place1);
                Defer defer([&]() { agg_fn->destroy(place1); });
                std::vector<AggregateDataPtr> places {place1};

                auto* place2 = reinterpret_cast<vectorized::AggregateDataPtr>(
                        arena.alloc(agg_fn->function()->size_of_data()));

                agg_fn->function()->deserialize_and_merge_vec(places.data(), 0, place2,
                                                              serialize_column.get(), arena, 1);

                check_result(place1);
            }
        }

        { // deserialize_and_merge_vec_selected
            MutableColumnPtr serialize_column = agg_fn->function()->create_serialize_column();
            Arena arena;
            {
                auto* place = reinterpret_cast<vectorized::AggregateDataPtr>(
                        arena.alloc(agg_fn->function()->size_of_data()));

                agg_fn->create(place);
                Defer defer([&]() { agg_fn->destroy(place); });

                auto st = agg_fn->execute_single_add(&block, place, arena);
                EXPECT_TRUE(st.ok()) << st.msg();
                std::vector<AggregateDataPtr> places {place};
                agg_fn->function()->serialize_to_column(places, 0, serialize_column, 1);
            }
            {
                auto* place1 = reinterpret_cast<vectorized::AggregateDataPtr>(
                        arena.alloc(agg_fn->function()->size_of_data()));
                agg_fn->create(place1);
                Defer defer([&]() { agg_fn->destroy(place1); });
                std::vector<AggregateDataPtr> places {place1};

                auto* place2 = reinterpret_cast<vectorized::AggregateDataPtr>(
                        arena.alloc(agg_fn->function()->size_of_data()));

                agg_fn->function()->deserialize_and_merge_vec_selected(
                        places.data(), 0, place2, serialize_column.get(), arena, 1);

                check_result(place1);
            }
        }
    }

    ObjectPool pool;
    AggFnEvaluator* agg_fn;
};

} // namespace doris::vectorized
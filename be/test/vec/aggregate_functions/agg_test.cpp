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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <stdint.h>

#include <memory>
#include <random>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_topn.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vectorized_agg_fn.h"

const int agg_test_batch_size = 4096;

namespace doris::vectorized {
// declare function
void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_topn(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_bit(AggregateFunctionSimpleFactory& factory);

TEST(AggTest, basic_test) {
    Arena arena;
    auto column_vector_int32 = ColumnInt32::create();
    for (int i = 0; i < agg_test_batch_size; i++) {
        column_vector_int32->insert(Field::create_field<TYPE_INT>(cast_to_nearest_field_type(i)));
    }
    // test implement interface
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_sum(factory);
    DataTypePtr data_type(std::make_shared<DataTypeInt32>());
    DataTypes data_types = {data_type};
    auto agg_function = factory.get("sum", data_types, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[1] = {column_vector_int32.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, arena);
    }
    int ans = 0;
    for (int i = 0; i < agg_test_batch_size; i++) {
        ans += i;
    }
    EXPECT_EQ(ans, *reinterpret_cast<int32_t*>(place));
    agg_function->destroy(place);
}

TEST(AggTest, topn_test) {
    Arena arena;
    MutableColumns datas(2);
    datas[0] = ColumnString::create();
    datas[1] = ColumnInt32::create();
    int top = 10;

    for (int i = 0; i < agg_test_batch_size; i++) {
        std::string str = std::to_string(agg_test_batch_size / (i + 1));
        datas[0]->insert_data(str.c_str(), str.length());
        datas[1]->insert_data(reinterpret_cast<char*>(&top), sizeof(top));
    }

    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topn(factory);
    DataTypes data_types = {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()};

    auto agg_function = factory.get("topn", data_types, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    IColumn* columns[2] = {datas[0].get(), datas[1].get()};

    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, const_cast<const IColumn**>(columns), i, arena);
    }

    std::string result = reinterpret_cast<AggregateFunctionTopNData<TYPE_STRING>*>(place)->get();
    std::string expect_result =
            "{\"1\":2048,\"2\":683,\"3\":341,\"4\":205,\"5\":137,\"6\":97,\"7\":73,\"8\":57,\"9\":"
            "46,\"10\":37}";
    EXPECT_EQ(result, expect_result);
    agg_function->destroy(place);
}

TEST(AggTest, window_function_test) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bit(factory);
    DataTypes data_types = {make_nullable(std::make_shared<DataTypeInt8>())};
    bool is_window_function = true;
    auto agg_function = factory.get("group_bit_or", data_types, true, -1,
                                    {.enable_decimal256 = false,
                                     .is_window_function = is_window_function,
                                     .column_names = {}});
    auto size = agg_function->size_of_data();
    EXPECT_EQ(size, 6);
    size = agg_function->align_of_data();
    EXPECT_EQ(size, 4);

    is_window_function = false;
    auto agg_function2 = factory.get("group_bit_or", data_types, true, -1,
                                     {.enable_decimal256 = false,
                                      .is_window_function = is_window_function,
                                      .column_names = {}});
    auto size2 = agg_function2->size_of_data();
    EXPECT_EQ(size2, 2);
    size2 = agg_function2->align_of_data();
    EXPECT_EQ(size2, 1);
}

TEST(AggTest, window_function_test2) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_sum(factory);
    register_aggregate_function_topn(factory);
    register_aggregate_function_bit(factory);

    bool is_window_function = true;
    auto agg_function_sum = factory.get("sum", {std::make_shared<DataTypeInt32>()}, true, -1,
                                        {.enable_decimal256 = false,
                                         .is_window_function = is_window_function,
                                         .column_names = {}});

    auto agg_function_group =
            factory.get("group_bit_or", {make_nullable(std::make_shared<DataTypeInt8>())}, true, -1,
                        {.enable_decimal256 = false,
                         .is_window_function = is_window_function,
                         .column_names = {}});

    auto agg_function_topn = factory.get(
            "topn", {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()}, true,
            -1,
            {.enable_decimal256 = false,
             .is_window_function = is_window_function,
             .column_names = {}});
    std::vector<vectorized::AggregateFunctionPtr> _agg_functions;
    _agg_functions.push_back(agg_function_sum);
    _agg_functions.push_back(agg_function_group);
    _agg_functions.push_back(agg_function_topn);

    auto lambda_function = [&]() {
        /// The offset of the n-th functions.
        std::vector<size_t> _offsets_of_aggregate_states;
        /// The total size of the row from the functions.
        size_t _total_size_of_aggregate_states = 0;
        /// The max align size for functions
        size_t _align_aggregate_states = 1;
        auto _agg_functions_size = 3;

        _offsets_of_aggregate_states.resize(_agg_functions_size);
        for (size_t i = 0; i < _agg_functions_size; ++i) {
            _offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;
            // aggregate states are aligned based on maximum requirement
            _align_aggregate_states =
                    std::max(_align_aggregate_states, _agg_functions[i]->align_of_data());
            _total_size_of_aggregate_states += _agg_functions[i]->size_of_data();
            // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
            if (i + 1 < _agg_functions_size) {
                size_t alignment_of_next_state = _agg_functions[i + 1]->align_of_data();
                if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0) {
                    DCHECK(false) << "Logical error: align_of_data is not 2^N "
                                  << alignment_of_next_state;
                }
                /// Extend total_size to next alignment requirement
                /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
                _total_size_of_aggregate_states =
                        (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                        alignment_of_next_state * alignment_of_next_state;
            }
        }

        vectorized::Arena _agg_arena_pool;
        vectorized::AggregateDataPtr _fn_place_ptr = _agg_arena_pool.aligned_alloc(
                _total_size_of_aggregate_states, _align_aggregate_states);
        EXPECT_TRUE(_fn_place_ptr != nullptr);

        for (size_t i = 0; i < _agg_functions_size; ++i) {
            _agg_functions[i]->create(_fn_place_ptr + _offsets_of_aggregate_states[i]);
        }
    };
    std::random_device rd;
    std::mt19937 g(rd());

    for (int i = 0; i < 5; ++i) {
        std::shuffle(_agg_functions.begin(), _agg_functions.end(), g);
        lambda_function();
    }
}

} // namespace doris::vectorized

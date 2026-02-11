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
#include "runtime/primitive_type.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_topn.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vectorized_agg_fn.h"

const int agg_test_batch_size = 4096;

namespace doris::vectorized {
// declare function
void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_topn(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_bit(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_minmax(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_replace_reader_load(AggregateFunctionSimpleFactory& factory);

TEST(AggTest, basic_test) {
    Arena arena;
    auto column_vector_int32 = ColumnInt32::create();
    for (int i = 0; i < agg_test_batch_size; i++) {
        column_vector_int32->insert(Field::create_field<TYPE_INT>(i));
    }
    // test implement interface
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_sum(factory);
    DataTypePtr data_type(std::make_shared<DataTypeInt32>());
    DataTypes data_types = {data_type};
    auto agg_function = factory.get("sum", data_types, nullptr, false, -1);
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

    auto agg_function = factory.get("topn", data_types, nullptr, false, -1);
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
    auto agg_function = factory.get("group_bit_or", data_types, nullptr, true, -1,
                                    {.is_window_function = is_window_function, .column_names = {}});
    auto size = agg_function->size_of_data();
    EXPECT_EQ(size, 6);
    size = agg_function->align_of_data();
    EXPECT_EQ(size, 4);

    is_window_function = false;
    auto agg_function2 =
            factory.get("group_bit_or", data_types, nullptr, true, -1,
                        {.is_window_function = is_window_function, .column_names = {}});
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
    auto agg_function_sum =
            factory.get("sum", {std::make_shared<DataTypeInt32>()}, nullptr, true, -1,
                        {.is_window_function = is_window_function, .column_names = {}});

    auto agg_function_group =
            factory.get("group_bit_or", {make_nullable(std::make_shared<DataTypeInt8>())}, nullptr,
                        true, -1, {.is_window_function = is_window_function, .column_names = {}});

    auto agg_function_topn = factory.get(
            "topn", {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()},
            nullptr, true, -1, {.is_window_function = is_window_function, .column_names = {}});
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

TEST(AggTest, datetime_min_max_test) {
    Arena arena;
    auto column_datetime = ColumnDateTime::create();

    // Insert DateTime values (stored as Int64)
    // Format: YYYYMMDDHHmmss as Int64
    VecDateTimeValue tmp;
    std::vector<VecDateTimeValue> datetime_values;
    tmp.from_date_int64(20230101120000LL);
    datetime_values.push_back(tmp);
    tmp.from_date_int64(20230615093000LL);
    datetime_values.push_back(tmp);
    tmp.from_date_int64(20220315180000LL);
    datetime_values.push_back(tmp);
    tmp.from_date_int64(20240801220000LL);
    datetime_values.push_back(tmp);
    tmp.from_date_int64(20230301060000LL);
    datetime_values.push_back(tmp);

    for (auto val : datetime_values) {
        column_datetime->insert_data(reinterpret_cast<const char*>(&val), sizeof(int64_t));
    }

    // Test MIN
    {
        AggregateFunctionSimpleFactory factory;
        register_aggregate_function_minmax(factory);
        DataTypePtr data_type = std::make_shared<DataTypeDateTime>();
        DataTypes data_types = {data_type};
        auto agg_function = factory.get("min", data_types, data_type, false, -1);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        const IColumn* column[1] = {column_datetime.get()};
        for (size_t i = 0; i < datetime_values.size(); i++) {
            agg_function->add(place, column, i, arena);
        }

        // Get result by inserting into a column
        auto result_column = ColumnDateTime::create();
        agg_function->insert_result_into(place, *result_column);
        auto result = result_column->get_element(0).to_datetime_int64();
        EXPECT_EQ(result, 20220315180000LL); // Minimum datetime
        agg_function->destroy(place);
    }

    // Test MAX
    {
        AggregateFunctionSimpleFactory factory;
        register_aggregate_function_minmax(factory);
        DataTypePtr data_type = std::make_shared<DataTypeDateTime>();
        DataTypes data_types = {data_type};
        auto agg_function = factory.get("max", data_types, data_type, false, -1);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        const IColumn* column[1] = {column_datetime.get()};
        for (size_t i = 0; i < datetime_values.size(); i++) {
            agg_function->add(place, column, i, arena);
        }

        // Get result by inserting into a column
        auto result_column = ColumnDateTime::create();
        agg_function->insert_result_into(place, *result_column);
        auto result = result_column->get_element(0).to_datetime_int64();
        EXPECT_EQ(result, 20240801220000LL); // Maximum datetime
        agg_function->destroy(place);
    }
}

TEST(AggTest, date_replace_test) {
    Arena arena;
    auto column_date = ColumnDate::create();

    // Insert Date values (stored as Int64)
    std::vector<int64_t> date_values = {100, 200, 300, 400, 500};

    for (auto val : date_values) {
        column_date->insert_data(reinterpret_cast<const char*>(&val), sizeof(int64_t));
    }

    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_replace_reader_load(factory);
    DataTypePtr data_type = std::make_shared<DataTypeDate>();
    DataTypes data_types = {data_type};
    auto agg_function = factory.get("replace_load", data_types, data_type, false, -1);

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    const IColumn* column[1] = {column_date.get()};
    for (size_t i = 0; i < date_values.size(); i++) {
        agg_function->add(place, column, i, arena);
    }

    // REPLACE should return the last value
    auto result_column = ColumnDate::create();
    agg_function->insert_result_into(place, *result_column);
    auto result = binary_cast<VecDateTimeValue, int64_t>(result_column->get_element(0));
    EXPECT_EQ(result, 500LL);
    agg_function->destroy(place);
}

TEST(AggTest, datetime_replace_if_not_null_test) {
    Arena arena;
    auto column_datetime = ColumnDateTime::create();
    auto null_map = ColumnUInt8::create();

    // Insert DateTime values with some nulls (stored as Int64 in YYYYMMDDHHmmss format)
    std::vector<int64_t> datetime_values = {20230101120000LL, 20230615093000LL, 20220315180000LL,
                                            20240801220000LL, 20230301060000LL};

    std::vector<uint8_t> nulls = {0, 1, 0, 0, 0}; // Second value is null

    for (auto val : datetime_values) {
        column_datetime->insert_data(reinterpret_cast<const char*>(&val), sizeof(int64_t));
    }

    for (auto null : nulls) {
        null_map->insert_data(reinterpret_cast<const char*>(&null), sizeof(uint8_t));
    }

    auto nullable_column = ColumnNullable::create(std::move(column_datetime), std::move(null_map));

    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_replace_reader_load(factory);
    DataTypePtr data_type = make_nullable(std::make_shared<DataTypeDateTime>());
    DataTypes data_types = {data_type};
    auto agg_function = factory.get("replace_if_not_null_load", data_types, data_type, true, -1);

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    const IColumn* column[1] = {nullable_column.get()};
    for (size_t i = 0; i < datetime_values.size(); i++) {
        agg_function->add(place, column, i, arena);
    }

    // REPLACE_IF_NOT_NULL should return the last non-null value
    // Need to use nullable column for result since input is nullable
    auto result_nested_column = ColumnDateTime::create();
    auto result_null_map = ColumnUInt8::create();
    auto result_column =
            ColumnNullable::create(std::move(result_nested_column), std::move(result_null_map));
    agg_function->insert_result_into(place, *result_column);

    // Get the nested column and read the value
    const auto& nested_col = assert_cast<const ColumnDateTime&>(result_column->get_nested_column());
    auto result = binary_cast<VecDateTimeValue, int64_t>(nested_col.get_element(0));
    EXPECT_EQ(result, 20230301060000LL); // Last value
    agg_function->destroy(place);
}

} // namespace doris::vectorized

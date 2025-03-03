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

#include <cstddef>
#include <memory>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
const int agg_test_batch_size = 2;

namespace doris::vectorized {
void register_aggregate_function_group_array_intersect(AggregateFunctionSimpleFactory& factory);

template <typename T>
void sort_numeric_array(Array& array) {
    std::sort(array.begin(), array.end(), [](const Field& a, const Field& b) {
        if (a.is_null() || b.is_null()) {
            return a.is_null() && !b.is_null();
        }
        return doris::vectorized::get<T>(a) < doris::vectorized::get<T>(b);
    });
}

void sort_string_array(Array& array) {
    std::sort(array.begin(), array.end(), [](const Field& a, const Field& b) {
        if (a.is_null() || b.is_null()) {
            return a.is_null() && !b.is_null();
        }
        return a < b;
    });
}

template <typename T>
void validate_numeric_test(MutableColumnPtr& test_col_data) {
    // Prepare test data.
    auto nested_column = ColumnVector<T>::create();
    nested_column->insert_value((T)1);
    nested_column->insert_value((T)2);
    nested_column->insert_value((T)3);
    nested_column->insert_value((T)11);
    nested_column->insert_value((T)2);
    nested_column->insert_value((T)3);

    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->insert(3);
    offsets_column->insert(6);

    test_col_data = ColumnArray::create(std::move(nested_column), std::move(offsets_column));
    EXPECT_EQ(test_col_data->size(), 2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_group_array_intersect(factory);
    DataTypePtr data_type_array_numeric(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<T>>()));
    DataTypes data_types = {data_type_array_numeric};
    auto agg_function = factory.get("group_array_intersect", data_types, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Initialize Arena
    Arena arena;

    // Do aggregation.
    const IColumn* column[1] = {test_col_data.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, &arena);
    }

    // Check result.
    ColumnArray ans(ColumnVector<T>::create(), ColumnArray::ColumnOffsets::create());
    agg_function->insert_result_into(place, ans);
    Field actual_field;
    ans.get(0, actual_field);
    const auto& actual_result = doris::vectorized::get<Array&>(actual_field);

    Array expected_result = {(T)2, (T)3};

    Array sorted_actual_result = actual_result;
    Array sorted_expected_result = expected_result;
    sort_numeric_array<T>(sorted_actual_result);
    sort_numeric_array<T>(sorted_expected_result);

    EXPECT_EQ(sorted_actual_result.size(), sorted_expected_result.size());
    for (size_t i = 0; i < sorted_actual_result.size(); ++i) {
        EXPECT_EQ(sorted_actual_result[i], sorted_expected_result[i]);
    }

    agg_function->destroy(place);
}

template <typename T>
void validate_numeric_nullable_test(MutableColumnPtr& test_col_data) {
    // Prepare test data.
    auto nested_column = ColumnVector<T>::create();

    auto nullable_nested_column =
            ColumnNullable::create(std::move(nested_column), ColumnUInt8::create());

    nullable_nested_column->insert((T)1);
    nullable_nested_column->insert(Null());
    nullable_nested_column->insert((T)3);
    nullable_nested_column->insert((T)11);
    nullable_nested_column->insert(Null());
    nullable_nested_column->insert((T)3);

    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->insert(3);
    offsets_column->insert(6);

    test_col_data =
            ColumnArray::create(std::move(nullable_nested_column), std::move(offsets_column));
    EXPECT_EQ(test_col_data->size(), 2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_group_array_intersect(factory);

    DataTypePtr data_type_array_numeric(std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<T>>())));
    DataTypes data_types = {data_type_array_numeric};
    auto agg_function = factory.get("group_array_intersect", data_types, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Initialize Arena
    Arena arena;

    // Do aggregation.
    const IColumn* column[1] = {test_col_data.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, &arena);
    }

    // Check result.
    auto nested_result_column = ColumnVector<T>::create();
    auto nullable_nested_result_column =
            ColumnNullable::create(std::move(nested_result_column), ColumnUInt8::create());
    ColumnArray ans(std::move(nullable_nested_result_column), ColumnArray::ColumnOffsets::create());

    agg_function->insert_result_into(place, ans);

    Field actual_field;
    ans.get(0, actual_field);
    const auto& actual_result = doris::vectorized::get<Array&>(actual_field);

    Array expected_result = {Null(), (T)3};

    Array sorted_actual_result = actual_result;
    Array sorted_expected_result = expected_result;
    sort_numeric_array<T>(sorted_actual_result);
    sort_numeric_array<T>(sorted_expected_result);

    EXPECT_EQ(sorted_actual_result.size(), sorted_expected_result.size());
    for (size_t i = 0; i < sorted_actual_result.size(); ++i) {
        if (sorted_expected_result[i].is_null()) {
            EXPECT_TRUE(sorted_actual_result[i].is_null());
        } else {
            EXPECT_EQ(sorted_actual_result[i], sorted_expected_result[i]);
        }
    }

    agg_function->destroy(place);
}

template <typename T>
void numeric_test_aggregate_function_group_array_intersect() {
    MutableColumnPtr column_array_numeric;
    validate_numeric_test<T>(column_array_numeric);

    MutableColumnPtr column_array_numeric_nullable;
    validate_numeric_nullable_test<T>(column_array_numeric_nullable);
}

TEST(AggGroupArrayIntersectTest, numeric_test) {
    numeric_test_aggregate_function_group_array_intersect<UInt8>();
    numeric_test_aggregate_function_group_array_intersect<Int8>();
    numeric_test_aggregate_function_group_array_intersect<Int16>();
    numeric_test_aggregate_function_group_array_intersect<Int32>();
    numeric_test_aggregate_function_group_array_intersect<Int64>();
    numeric_test_aggregate_function_group_array_intersect<Int128>();
    numeric_test_aggregate_function_group_array_intersect<Float32>();
    numeric_test_aggregate_function_group_array_intersect<Float64>();
    numeric_test_aggregate_function_group_array_intersect<DateV2>();
    numeric_test_aggregate_function_group_array_intersect<DateTimeV2>();
}

TEST(AggGroupArrayIntersectTest, string_test) {
    // Prepare test data.
    auto nested_column = ColumnString::create();
    nested_column->insert_data("a", 1);
    nested_column->insert_data("b", 1);
    nested_column->insert_data("c", 1);
    nested_column->insert_data("aaaa", 4);
    nested_column->insert_data("b", 1);
    nested_column->insert_data("c", 1);

    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->insert(3);
    offsets_column->insert(6);

    auto column_array_string =
            ColumnArray::create(std::move(nested_column), std::move(offsets_column));

    EXPECT_EQ(column_array_string->size(), 2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_group_array_intersect(factory);
    DataTypePtr data_type_array_string(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    DataTypes data_types = {data_type_array_string};
    auto agg_function = factory.get("group_array_intersect", data_types, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Initialize Arena
    Arena arena;

    // Do aggregation.
    const IColumn* column[1] = {column_array_string.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, &arena);
    }

    // Check result.
    ColumnArray ans(ColumnString::create(), ColumnArray::ColumnOffsets::create());
    agg_function->insert_result_into(place, ans);
    Field actual_field;
    ans.get(0, actual_field);
    const auto& actual_result = doris::vectorized::get<Array&>(actual_field);

    Array expected_result = {"b", "c"};

    Array sorted_actual_result = actual_result;
    Array sorted_expected_result = expected_result;
    sort_string_array(sorted_actual_result);
    sort_string_array(sorted_expected_result);

    EXPECT_EQ(sorted_actual_result.size(), sorted_expected_result.size());
    for (size_t i = 0; i < sorted_actual_result.size(); ++i) {
        EXPECT_EQ(sorted_actual_result[i], sorted_expected_result[i]);
    }

    agg_function->destroy(place);
}

TEST(AggGroupArrayIntersectTest, string_nullable_test) {
    // Prepare test data.
    auto nested_column = ColumnString::create();

    auto nullable_nested_column =
            ColumnNullable::create(std::move(nested_column), ColumnUInt8::create());

    nullable_nested_column->insert("a");
    nullable_nested_column->insert(Null());
    nullable_nested_column->insert("c");
    nullable_nested_column->insert("aaaa");
    nullable_nested_column->insert(Null());
    nullable_nested_column->insert("c");

    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->insert(3);
    offsets_column->insert(6);

    auto column_array_string_nullable =
            ColumnArray::create(std::move(nullable_nested_column), std::move(offsets_column));
    EXPECT_EQ(column_array_string_nullable->size(), 2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_group_array_intersect(factory);
    DataTypePtr data_type_array_string(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    DataTypes data_types = {data_type_array_string};
    auto agg_function = factory.get("group_array_intersect", data_types, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Initialize Arena
    Arena arena;

    // Do aggregation.
    const IColumn* column[1] = {column_array_string_nullable.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, &arena);
    }

    // Check result.
    auto nested_result_column = ColumnString::create();
    auto nullable_nested_result_column =
            ColumnNullable::create(std::move(nested_result_column), ColumnUInt8::create());
    ColumnArray ans(std::move(nullable_nested_result_column), ColumnArray::ColumnOffsets::create());
    agg_function->insert_result_into(place, ans);
    Field actual_field;
    ans.get(0, actual_field);
    const auto& actual_result = doris::vectorized::get<Array&>(actual_field);

    Array expected_result = {Null(), "c"};

    Array sorted_actual_result = actual_result;
    Array sorted_expected_result = expected_result;
    sort_string_array(sorted_actual_result);
    sort_string_array(sorted_expected_result);

    EXPECT_EQ(sorted_actual_result.size(), sorted_expected_result.size());
    for (size_t i = 0; i < sorted_actual_result.size(); ++i) {
        if (sorted_expected_result[i].is_null()) {
            EXPECT_TRUE(sorted_actual_result[i].is_null());
        } else {
            EXPECT_EQ(sorted_actual_result[i], sorted_expected_result[i]);
        }
    }

    agg_function->destroy(place);
}

} // namespace doris::vectorized

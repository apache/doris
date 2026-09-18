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

#include "common/exception.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "gtest/gtest_pred_impl.h"
const int agg_test_batch_size = 2;

namespace doris {
void register_aggregate_function_group_array_set_op(AggregateFunctionSimpleFactory& factory);

template <PrimitiveType T>
void sort_numeric_array(Array& array) {
    std::sort(array.begin(), array.end(), [](const Field& a, const Field& b) {
        if (a.is_null() || b.is_null()) {
            return a.is_null() && !b.is_null();
        }
        return a.get<T>() < b.get<T>();
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

template <PrimitiveType T>
void validate_numeric_test(MutableColumnPtr& test_col_data) {
    // Prepare test data.
    auto nested_column = ColumnVector<T>::create();
    Array expected_result;
    if constexpr (T == TYPE_DATE || T == TYPE_DATETIME) {
        int64_t tmp = 1;
        nested_column->insert_value(binary_cast<int64_t, VecDateTimeValue>(tmp));
        tmp = 2;
        nested_column->insert_value(binary_cast<int64_t, VecDateTimeValue>(tmp));
        tmp = 3;
        nested_column->insert_value(binary_cast<int64_t, VecDateTimeValue>(tmp));
        tmp = 11;
        nested_column->insert_value(binary_cast<int64_t, VecDateTimeValue>(tmp));
        tmp = 2;
        nested_column->insert_value(binary_cast<int64_t, VecDateTimeValue>(tmp));
        tmp = 3;
        nested_column->insert_value(binary_cast<int64_t, VecDateTimeValue>(tmp));

        int64_t tmp_exp1 = 2;
        int64_t tmp_exp2 = 3;
        expected_result = {
                Field::create_field<T>(binary_cast<int64_t, VecDateTimeValue>(tmp_exp1)),
                Field::create_field<T>(binary_cast<int64_t, VecDateTimeValue>(tmp_exp2))};
    } else if constexpr (T == TYPE_DATEV2) {
        uint32_t tmp = 1;
        nested_column->insert_value(binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(tmp));
        tmp = 2;
        nested_column->insert_value(binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(tmp));
        tmp = 3;
        nested_column->insert_value(binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(tmp));
        tmp = 11;
        nested_column->insert_value(binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(tmp));
        tmp = 2;
        nested_column->insert_value(binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(tmp));
        tmp = 3;
        nested_column->insert_value(binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(tmp));

        uint32_t tmp_exp1 = 2;
        uint32_t tmp_exp2 = 3;
        expected_result = {Field::create_field<T>(
                                   binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(tmp_exp1)),
                           Field::create_field<T>(
                                   binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(tmp_exp2))};
    } else if constexpr (T == TYPE_DATETIMEV2) {
        uint64_t tmp = 1;
        nested_column->insert_value(binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(tmp));
        tmp = 2;
        nested_column->insert_value(binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(tmp));
        tmp = 3;
        nested_column->insert_value(binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(tmp));
        tmp = 11;
        nested_column->insert_value(binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(tmp));
        tmp = 2;
        nested_column->insert_value(binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(tmp));
        tmp = 3;
        nested_column->insert_value(binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(tmp));

        uint64_t tmp_exp1 = 2;
        uint64_t tmp_exp2 = 3;
        expected_result = {
                Field::create_field<T>(
                        binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(tmp_exp1)),
                Field::create_field<T>(
                        binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(tmp_exp2))};
    } else if constexpr (T == TYPE_TIMESTAMPTZ) {
        uint64_t tmp = 1;
        nested_column->insert_value(binary_cast<uint64_t, TimestampTzValue>(tmp));
        tmp = 2;
        nested_column->insert_value(binary_cast<uint64_t, TimestampTzValue>(tmp));
        tmp = 3;
        nested_column->insert_value(binary_cast<uint64_t, TimestampTzValue>(tmp));
        tmp = 11;
        nested_column->insert_value(binary_cast<uint64_t, TimestampTzValue>(tmp));
        tmp = 2;
        nested_column->insert_value(binary_cast<uint64_t, TimestampTzValue>(tmp));
        tmp = 3;
        nested_column->insert_value(binary_cast<uint64_t, TimestampTzValue>(tmp));

        uint64_t tmp_exp1 = 2;
        uint64_t tmp_exp2 = 3;
        expected_result = {
                Field::create_field<T>(binary_cast<uint64_t, TimestampTzValue>(tmp_exp1)),
                Field::create_field<T>(binary_cast<uint64_t, TimestampTzValue>(tmp_exp2))};
    } else {
        nested_column->insert_value((typename PrimitiveTypeTraits<T>::CppType)1);
        nested_column->insert_value((typename PrimitiveTypeTraits<T>::CppType)2);
        nested_column->insert_value((typename PrimitiveTypeTraits<T>::CppType)3);
        nested_column->insert_value((typename PrimitiveTypeTraits<T>::CppType)11);
        nested_column->insert_value((typename PrimitiveTypeTraits<T>::CppType)2);
        nested_column->insert_value((typename PrimitiveTypeTraits<T>::CppType)3);

        expected_result = {Field::create_field<T>((typename PrimitiveTypeTraits<T>::CppType)2),
                           Field::create_field<T>((typename PrimitiveTypeTraits<T>::CppType)3)};
    }
    auto null_map_column = ColumnUInt8::create();
    null_map_column->get_data().resize_fill(nested_column->size(), 0);

    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->insert(Field::create_field<TYPE_UINT64>(3));
    offsets_column->insert(Field::create_field<TYPE_UINT64>(6));

    // array nested column should be nullable
    test_col_data = ColumnArray::create(
            ColumnNullable::create(std::move(nested_column), std::move(null_map_column)),
            std::move(offsets_column));
    EXPECT_EQ(test_col_data->size(), 2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_group_array_set_op(factory);
    const auto nested =
            T == TYPE_DATEV2
                    ? std::dynamic_pointer_cast<const IDataType>(std::make_shared<DataTypeDateV2>())
            : T == TYPE_DATETIMEV2
                    ? std::dynamic_pointer_cast<const IDataType>(
                              std::make_shared<DataTypeDateTimeV2>())
                    : std::dynamic_pointer_cast<const IDataType>(
                              std::make_shared<typename PrimitiveTypeTraits<T>::DataType>());
    DataTypePtr data_type_array_numeric(std::make_shared<DataTypeArray>(nested));
    DataTypes data_types = {data_type_array_numeric};
    auto agg_function = factory.get("group_array_intersect", data_types, nullptr, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Initialize Arena
    Arena arena;

    // Do aggregation.
    const IColumn* column[1] = {test_col_data.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    auto nested_result_column = PrimitiveTypeTraits<T>::ColumnType::create();
    auto null_map_result_column = ColumnUInt8::create();
    auto nullable_nested_result_column = ColumnNullable::create(std::move(nested_result_column),
                                                                std::move(null_map_result_column));
    ColumnArray ans(std::move(nullable_nested_result_column), ColumnArray::ColumnOffsets::create());
    agg_function->insert_result_into(place, ans);
    Field actual_field;
    ans.get(0, actual_field);
    const auto& actual_result = actual_field.get<TYPE_ARRAY>();

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

template <PrimitiveType T>
void validate_numeric_nullable_test(MutableColumnPtr& test_col_data) {
    // Prepare test data.
    auto nested_column = ColumnVector<T>::create();

    auto nullable_nested_column =
            ColumnNullable::create(std::move(nested_column), ColumnUInt8::create());
    Array expected_result;
    if constexpr (T == TYPE_DATE || T == TYPE_DATETIME) {
        int64_t tmp0 = 1;
        int64_t tmp1 = 3;
        int64_t tmp2 = 11;
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp0));
        nullable_nested_column->insert(Field());
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp1));
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp2));
        nullable_nested_column->insert(Field());
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp1));

        int64_t tmp_exp2 = 3;
        expected_result = {Field(), Field::create_field<T>(
                                            *(typename PrimitiveTypeTraits<T>::CppType*)&tmp_exp2)};
    } else if constexpr (T == TYPE_DATEV2) {
        uint32_t tmp0 = 1;
        uint32_t tmp1 = 3;
        uint32_t tmp2 = 11;
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp0));
        nullable_nested_column->insert(Field());
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp1));
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp2));
        nullable_nested_column->insert(Field());
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp1));

        uint32_t tmp_exp2 = 3;
        expected_result = {Field(), Field::create_field<T>(
                                            *(typename PrimitiveTypeTraits<T>::CppType*)&tmp_exp2)};
    } else if constexpr (T == TYPE_DATETIMEV2 || T == TYPE_TIMESTAMPTZ) {
        uint64_t tmp0 = 1;
        uint64_t tmp1 = 3;
        uint64_t tmp2 = 11;
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp0));
        nullable_nested_column->insert(Field());
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp1));
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp2));
        nullable_nested_column->insert(Field());
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp1));

        uint64_t tmp_exp2 = 3;
        expected_result = {Field(), Field::create_field<T>(
                                            *(typename PrimitiveTypeTraits<T>::CppType*)&tmp_exp2)};
    } else {
        typename PrimitiveTypeTraits<T>::CppType tmp0 = 1;
        typename PrimitiveTypeTraits<T>::CppType tmp1 = 3;
        typename PrimitiveTypeTraits<T>::CppType tmp2 = 11;
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp0));
        nullable_nested_column->insert(Field());
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp1));
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp2));
        nullable_nested_column->insert(Field());
        nullable_nested_column->insert(
                Field::create_field<T>(*(typename PrimitiveTypeTraits<T>::CppType*)&tmp1));

        expected_result = {Field(),
                           Field::create_field<T>((typename PrimitiveTypeTraits<T>::CppType)3)};
    }

    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->insert(Field::create_field<TYPE_UINT64>(3));
    offsets_column->insert(Field::create_field<TYPE_UINT64>(6));

    test_col_data =
            ColumnArray::create(std::move(nullable_nested_column), std::move(offsets_column));
    EXPECT_EQ(test_col_data->size(), 2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_group_array_set_op(factory);

    const auto nested =
            T == TYPE_DATEV2
                    ? std::dynamic_pointer_cast<const IDataType>(std::make_shared<DataTypeDateV2>())
            : T == TYPE_DATETIMEV2
                    ? std::dynamic_pointer_cast<const IDataType>(
                              std::make_shared<DataTypeDateTimeV2>())
                    : std::dynamic_pointer_cast<const IDataType>(
                              std::make_shared<typename PrimitiveTypeTraits<T>::DataType>());
    DataTypePtr data_type_array_numeric(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(nested)));
    DataTypes data_types = {data_type_array_numeric};
    auto agg_function = factory.get("group_array_intersect", data_types, nullptr, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Initialize Arena
    Arena arena;

    // Do aggregation.
    const IColumn* column[1] = {test_col_data.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    auto nested_result_column = PrimitiveTypeTraits<T>::ColumnType::create();
    auto nullable_nested_result_column =
            ColumnNullable::create(std::move(nested_result_column), ColumnUInt8::create());
    ColumnArray ans(std::move(nullable_nested_result_column), ColumnArray::ColumnOffsets::create());

    agg_function->insert_result_into(place, ans);

    Field actual_field;
    ans.get(0, actual_field);
    const auto& actual_result = actual_field.get<TYPE_ARRAY>();

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

template <PrimitiveType T>
void numeric_test_aggregate_function_group_array_intersect() {
    MutableColumnPtr column_array_numeric;
    validate_numeric_test<T>(column_array_numeric);

    MutableColumnPtr column_array_numeric_nullable;
    validate_numeric_nullable_test<T>(column_array_numeric_nullable);
}

TEST(AggGroupArrayIntersectTest, numeric_test) {
    //    numeric_test_aggregate_function_group_array_intersect<TYPE_BOOLEAN>();
    numeric_test_aggregate_function_group_array_intersect<TYPE_TINYINT>();
    numeric_test_aggregate_function_group_array_intersect<TYPE_SMALLINT>();
    numeric_test_aggregate_function_group_array_intersect<TYPE_INT>();
    numeric_test_aggregate_function_group_array_intersect<TYPE_BIGINT>();
    numeric_test_aggregate_function_group_array_intersect<TYPE_LARGEINT>();
    numeric_test_aggregate_function_group_array_intersect<TYPE_FLOAT>();
    numeric_test_aggregate_function_group_array_intersect<TYPE_DOUBLE>();
    numeric_test_aggregate_function_group_array_intersect<TYPE_DATEV2>();
    numeric_test_aggregate_function_group_array_intersect<TYPE_DATETIMEV2>();
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
    auto null_map_column = ColumnUInt8::create();
    null_map_column->get_data().resize_fill(nested_column->size(), 0);

    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->insert(Field::create_field<TYPE_UINT64>(3));
    offsets_column->insert(Field::create_field<TYPE_UINT64>(6));

    // array nested column should be nullable
    auto column_array_string = ColumnArray::create(
            ColumnNullable::create(std::move(nested_column), std::move(null_map_column)),
            std::move(offsets_column));

    EXPECT_EQ(column_array_string->size(), 2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_group_array_set_op(factory);
    DataTypePtr data_type_array_string(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    DataTypes data_types = {data_type_array_string};
    auto agg_function = factory.get("group_array_intersect", data_types, nullptr, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Initialize Arena
    Arena arena;

    // Do aggregation.
    const IColumn* column[1] = {column_array_string.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    auto nested_result_column = ColumnString::create();
    auto null_map_result_column = ColumnUInt8::create();
    auto nullable_nested_result_column = ColumnNullable::create(std::move(nested_result_column),
                                                                std::move(null_map_result_column));
    ColumnArray ans(std::move(nullable_nested_result_column), ColumnArray::ColumnOffsets::create());
    agg_function->insert_result_into(place, ans);
    Field actual_field;
    ans.get(0, actual_field);
    const auto& actual_result = actual_field.get<TYPE_ARRAY>();

    Array expected_result = {Field::create_field<TYPE_STRING>("b"),
                             Field::create_field<TYPE_STRING>("c")};

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

    nullable_nested_column->insert(Field::create_field<TYPE_STRING>("a"));
    nullable_nested_column->insert(Field());
    nullable_nested_column->insert(Field::create_field<TYPE_STRING>("c"));
    nullable_nested_column->insert(Field::create_field<TYPE_STRING>("aaaa"));
    nullable_nested_column->insert(Field());
    nullable_nested_column->insert(Field::create_field<TYPE_STRING>("c"));

    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->insert(Field::create_field<TYPE_UINT64>(3));
    offsets_column->insert(Field::create_field<TYPE_UINT64>(6));

    auto column_array_string_nullable =
            ColumnArray::create(std::move(nullable_nested_column), std::move(offsets_column));
    EXPECT_EQ(column_array_string_nullable->size(), 2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_group_array_set_op(factory);
    DataTypePtr data_type_array_string(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    DataTypes data_types = {data_type_array_string};
    auto agg_function = factory.get("group_array_intersect", data_types, nullptr, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Initialize Arena
    Arena arena;

    // Do aggregation.
    const IColumn* column[1] = {column_array_string_nullable.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    auto nested_result_column = ColumnString::create();
    auto nullable_nested_result_column =
            ColumnNullable::create(std::move(nested_result_column), ColumnUInt8::create());
    ColumnArray ans(std::move(nullable_nested_result_column), ColumnArray::ColumnOffsets::create());
    agg_function->insert_result_into(place, ans);
    Field actual_field;
    ans.get(0, actual_field);
    const auto& actual_result = actual_field.get<TYPE_ARRAY>();

    Array expected_result = {Field(), Field::create_field<TYPE_STRING>("c")};

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

TEST(AggGroupArrayIntersectTest, string_null_element_does_not_match_empty_string) {
    DataTypePtr array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    DataTypes data_types = {array_type};
    auto test_column = array_type->create_column();
    test_column->insert(
            Field::create_field<TYPE_ARRAY>({Field::create_field<TYPE_STRING>(std::string())}));
    test_column->insert(Field::create_field<TYPE_ARRAY>({Field()}));

    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_group_array_set_op(factory);
    auto agg_function = factory.get("group_array_intersect", data_types, nullptr, false, -1);
    ASSERT_NE(agg_function, nullptr);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    Arena arena;
    ColumnRawPtrs columns(data_types.size(), test_column.get());
    agg_function->check_input_columns_type(columns.data());
    agg_function->add_batch_single_place(test_column->size(), place, columns.data(), arena);

    auto result_column = array_type->create_column();
    agg_function->insert_result_into(place, *result_column);
    Field actual_field;
    result_column->get(0, actual_field);
    EXPECT_TRUE(actual_field.get<TYPE_ARRAY>().empty());

    agg_function->destroy(place);
}

TEST(AggGroupArrayIntersectTest, raw_aggregate_rejects_outer_nullable_column) {
    DataTypePtr array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    DataTypes data_types = {array_type};

    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_group_array_set_op(factory);
    auto agg_function = factory.get("group_array_intersect", data_types, nullptr, false, -1);
    ASSERT_NE(agg_function, nullptr);

    auto outer_nullable_column = std::make_shared<DataTypeNullable>(array_type)->create_column();
    ColumnRawPtrs columns(data_types.size(), outer_nullable_column.get());
    EXPECT_THROW(agg_function->check_input_columns_type(columns.data()), Exception);
}

void validate_outer_nullable_array(const std::string& function_name, const DataTypes& data_types,
                                   const DataTypePtr& result_type, const IColumn& input_column,
                                   const Array& expected_result) {
    for (bool enable_null_v2 : {false, true}) {
        SCOPED_TRACE("function=" + function_name +
                     ", enable_aggregate_function_null_v2=" + std::to_string(enable_null_v2));
        AggregateFunctionSimpleFactory factory;
        register_aggregate_function_group_array_set_op(factory);
        AggregateFunctionAttr attr;
        attr.enable_aggregate_function_null_v2 = enable_null_v2;
        auto agg_function =
                factory.get(function_name, data_types, nullptr, false, -1, std::move(attr));
        ASSERT_NE(agg_function, nullptr);
        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        Arena arena;
        ColumnRawPtrs columns(data_types.size(), &input_column);
        agg_function->check_input_columns_type(columns.data());
        agg_function->add_batch_single_place(input_column.size(), place, columns.data(), arena);

        auto result_column = result_type->create_column();
        agg_function->insert_result_into(place, *result_column);
        Field actual_field;
        result_column->get(0, actual_field);
        auto actual_result = actual_field.get<TYPE_ARRAY>();
        auto sorted_expected_result = expected_result;
        sort_numeric_array<TYPE_INT>(actual_result);
        sort_numeric_array<TYPE_INT>(sorted_expected_result);
        EXPECT_EQ(actual_result, sorted_expected_result);

        agg_function->destroy(place);
    }
}

TEST(AggGroupArrayIntersectTest, outer_nullable_array_test) {
    auto nested_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr array_type = std::make_shared<DataTypeArray>(nested_type);
    DataTypes data_types = {std::make_shared<DataTypeNullable>(array_type)};
    auto test_column = data_types[0]->create_column();
    test_column->insert(Field::create_field<TYPE_ARRAY>(
            {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2)}));
    test_column->insert(Field());
    test_column->insert(Field::create_field<TYPE_ARRAY>(
            {Field::create_field<TYPE_INT>(2), Field::create_field<TYPE_INT>(3)}));

    validate_outer_nullable_array("group_array_intersect", data_types, array_type, *test_column,
                                  {Field::create_field<TYPE_INT>(2)});
}

TEST(AggGroupArrayIntersectTest, group_array_union_skips_outer_null_payload) {
    auto nested_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr array_type = std::make_shared<DataTypeArray>(nested_type);
    DataTypes data_types = {std::make_shared<DataTypeNullable>(array_type)};
    auto test_column = data_types[0]->create_column();
    test_column->insert(Field::create_field<TYPE_ARRAY>(
            {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2)}));
    test_column->insert(Field::create_field<TYPE_ARRAY>({Field::create_field<TYPE_INT>(99)}));
    test_column->insert(Field::create_field<TYPE_ARRAY>(
            {Field::create_field<TYPE_INT>(2), Field::create_field<TYPE_INT>(3)}));
    auto& nullable_column = static_cast<ColumnNullable&>(*test_column);
    nullable_column.get_null_map_data()[1] = 1;

    validate_outer_nullable_array(
            "group_array_union", data_types, array_type, *test_column,
            {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2),
             Field::create_field<TYPE_INT>(3)});
}

} // namespace doris

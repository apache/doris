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

#include "storage/iterator/olap_data_convertor.h"

#include <gtest/gtest.h>

#include <barrier>
#include <bit>
#include <cstdint>
#include <limits>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/column/column_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
namespace {

template <PrimitiveType T>
using CppType = typename PrimitiveTypeTraits<T>::CppType;

template <PrimitiveType T>
using SimpleConvertor = OlapBlockDataConvertor::OlapColumnDataConvertorSimple<T>;

static_assert(std::is_same_v<decltype(SimpleConvertor<TYPE_BIGINT>::_values), const int64_t*>);

template <PrimitiveType T>
struct NumericSource {
    ColumnWithTypeAndName typed_column;
    const CppType<T>* values;
    const IColumn* nested_column;
};

template <PrimitiveType T>
NumericSource<T> create_numeric_source(std::vector<CppType<T>> values,
                                       std::vector<UInt8> null_map = {}) {
    auto nested_column = PrimitiveTypeTraits<T>::ColumnType::create();
    nested_column->get_data().assign(values.begin(), values.end());
    const CppType<T>* source_values = nested_column->get_data().data();
    const IColumn* source_nested_column = nested_column.get();

    DataTypePtr data_type = std::make_shared<typename PrimitiveTypeTraits<T>::DataType>();
    ColumnPtr column;
    if (null_map.empty()) {
        column = std::move(nested_column);
    } else {
        auto null_map_column = ColumnUInt8::create();
        null_map_column->get_data().assign(null_map.begin(), null_map.end());
        column = ColumnNullable::create(std::move(nested_column), std::move(null_map_column));
        data_type = std::make_shared<DataTypeNullable>(data_type);
    }
    return {{std::move(column), std::move(data_type), "value"},
            source_values,
            source_nested_column};
}

TabletColumn create_tablet_column(FieldType type, bool nullable) {
    TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type, nullable);
    column.set_unique_id(0);
    column.set_name("value");
    const int32_t length =
            type == FieldType::OLAP_FIELD_TYPE_FLOAT ? sizeof(float) : sizeof(int64_t);
    column.set_length(length);
    column.set_index_length(length);
    return column;
}

template <PrimitiveType T>
FieldType field_type() {
    if constexpr (T == TYPE_BIGINT) {
        return FieldType::OLAP_FIELD_TYPE_BIGINT;
    } else if constexpr (T == TYPE_FLOAT) {
        return FieldType::OLAP_FIELD_TYPE_FLOAT;
    } else {
        static_assert(T == TYPE_DOUBLE);
        return FieldType::OLAP_FIELD_TYPE_DOUBLE;
    }
}

template <PrimitiveType T>
SimpleConvertor<T>* get_simple_convertor(OlapBlockDataConvertor& convertor) {
    return assert_cast<SimpleConvertor<T>*>(convertor._convertors[0].get());
}

template <PrimitiveType T>
std::pair<Status, IOlapColumnDataAccessor*> convert(OlapBlockDataConvertor& convertor,
                                                    const NumericSource<T>& source, size_t row_pos,
                                                    size_t num_rows) {
    auto status = convertor.set_source_content_with_specifid_column(source.typed_column, row_pos,
                                                                    num_rows, 0);
    if (!status.ok()) {
        return {std::move(status), nullptr};
    }
    return convertor.convert_column_data(0);
}

template <typename T>
using UIntOfSize = std::conditional_t<sizeof(T) == sizeof(uint32_t), uint32_t, uint64_t>;

template <typename T>
T from_bits(UIntOfSize<T> bits) {
    return std::bit_cast<T>(bits);
}

template <typename T>
UIntOfSize<T> bits_of(T value) {
    return std::bit_cast<UIntOfSize<T>>(value);
}

template <PrimitiveType T>
// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
void expect_float_without_nan_is_zero_copy(bool nullable) {
    using Value = CppType<T>;
    const std::vector<Value> values = {Value(1.25), Value(-2.5), Value(0), Value(9.75)};
    auto source = create_numeric_source<T>(
            values, nullable ? std::vector<UInt8> {0, 1, 0, 0} : std::vector<UInt8> {});
    OlapBlockDataConvertor convertor;
    convertor.add_column_data_convertor(create_tablet_column(field_type<T>(), nullable));

    auto [status, accessor] = convert(convertor, source, 1, 2);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(nullptr, accessor);
    EXPECT_EQ(source.values + 1, accessor->get_data());
    EXPECT_EQ(Value(-2.5), static_cast<const Value*>(accessor->get_data())[0]);
    EXPECT_EQ(Value(0), static_cast<const Value*>(accessor->get_data())[1]);
    if (nullable) {
        ASSERT_NE(nullptr, accessor->get_nullmap());
        EXPECT_EQ(1, accessor->get_nullmap()[0]);
        EXPECT_EQ(nullptr, accessor->get_data_at(0));
        EXPECT_EQ(source.values + 2, accessor->get_data_at(1));
    } else {
        EXPECT_EQ(nullptr, accessor->get_nullmap());
    }
    EXPECT_EQ(0, get_simple_convertor<T>(convertor)->_converted_values.allocated_bytes());
    EXPECT_EQ(values, std::vector<Value>(source.values, source.values + values.size()));
}

template <PrimitiveType T>
// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
void expect_nan_payloads_are_normalized_without_source_mutation() {
    using Value = CppType<T>;
    using UInt = UIntOfSize<Value>;
    const UInt nan_bits_1 =
            sizeof(Value) == sizeof(float) ? UInt(0x7fc00011U) : UInt(0x7ff8000000000011ULL);
    const UInt nan_bits_2 =
            sizeof(Value) == sizeof(float) ? UInt(0x7fa12345U) : UInt(0x7ff123456789abcdULL);
    const UInt nan_bits_3 =
            sizeof(Value) == sizeof(float) ? UInt(0xffc54321U) : UInt(0xfff8123456789abcULL);
    const std::vector<Value> values = {Value(1.5), from_bits<Value>(nan_bits_1),
                                       from_bits<Value>(nan_bits_2), Value(-8.25),
                                       from_bits<Value>(nan_bits_3)};
    std::vector<UInt> source_bits;
    for (Value value : values) {
        source_bits.push_back(bits_of(value));
    }

    auto source = create_numeric_source<T>(values, {0, 0, 1, 0, 0});
    OlapBlockDataConvertor convertor;
    convertor.add_column_data_convertor(create_tablet_column(field_type<T>(), true));

    auto [status, accessor] = convert(convertor, source, 0, values.size());
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(nullptr, accessor);
    ASSERT_NE(source.values, accessor->get_data());
    const auto* converted_values = static_cast<const Value*>(accessor->get_data());
    const UInt quiet_nan_bits = bits_of(std::numeric_limits<Value>::quiet_NaN());
    EXPECT_EQ(bits_of(Value(1.5)), bits_of(converted_values[0]));
    EXPECT_EQ(quiet_nan_bits, bits_of(converted_values[1]));
    EXPECT_EQ(quiet_nan_bits, bits_of(converted_values[2]));
    EXPECT_EQ(bits_of(Value(-8.25)), bits_of(converted_values[3]));
    EXPECT_EQ(quiet_nan_bits, bits_of(converted_values[4]));
    EXPECT_EQ(nullptr, accessor->get_data_at(2));

    for (size_t i = 0; i < source_bits.size(); ++i) {
        EXPECT_EQ(source_bits[i], bits_of(source.values[i]));
    }
    EXPECT_EQ(values.size(), get_simple_convertor<T>(convertor)->_converted_values.size());
}

} // namespace

TEST(OlapDataConvertorTest, ConcurrentNullableBigIntSourceIsReadOnly) {
    const std::vector<int64_t> values = {10, 20, 30, 40};
    auto source = create_numeric_source<TYPE_BIGINT>(values, {0, 1, 0, 0});
    ASSERT_EQ(1, source.nested_column->use_count());

    OlapBlockDataConvertor data_convertor;
    OlapBlockDataConvertor row_binlog_convertor;
    const TabletColumn tablet_column =
            create_tablet_column(FieldType::OLAP_FIELD_TYPE_BIGINT, true);
    data_convertor.add_column_data_convertor(tablet_column);
    row_binlog_convertor.add_column_data_convertor(tablet_column);
    ASSERT_TRUE(data_convertor
                        .set_source_content_with_specifid_column(source.typed_column, 0,
                                                                 values.size(), 0)
                        .ok());
    ASSERT_TRUE(row_binlog_convertor
                        .set_source_content_with_specifid_column(source.typed_column, 0,
                                                                 values.size(), 0)
                        .ok());

    std::barrier start(2);
    std::pair<Status, IOlapColumnDataAccessor*> data_result;
    std::pair<Status, IOlapColumnDataAccessor*> row_binlog_result;
    std::thread data_thread([&] {
        start.arrive_and_wait();
        data_result = data_convertor.convert_column_data(0);
    });
    std::thread row_binlog_thread([&] {
        start.arrive_and_wait();
        row_binlog_result = row_binlog_convertor.convert_column_data(0);
    });
    data_thread.join();
    row_binlog_thread.join();

    ASSERT_TRUE(data_result.first.ok()) << data_result.first;
    ASSERT_TRUE(row_binlog_result.first.ok()) << row_binlog_result.first;
    EXPECT_EQ(source.values, data_result.second->get_data());
    EXPECT_EQ(source.values, row_binlog_result.second->get_data());
    EXPECT_EQ(1, source.nested_column->use_count());
    EXPECT_EQ(values, std::vector<int64_t>(source.values, source.values + values.size()));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST(OlapDataConvertorTest, BigIntNullableAndNonNullableAreZeroCopy) {
    for (bool nullable : {false, true}) {
        const std::vector<int64_t> values = {11, 22, 33, 44};
        auto source = create_numeric_source<TYPE_BIGINT>(
                values, nullable ? std::vector<UInt8> {0, 1, 0, 0} : std::vector<UInt8> {});
        OlapBlockDataConvertor convertor;
        convertor.add_column_data_convertor(
                create_tablet_column(FieldType::OLAP_FIELD_TYPE_BIGINT, nullable));

        ASSERT_TRUE(convertor.set_source_content_with_specifid_column(source.typed_column, 1, 2, 0)
                            .ok());
        const auto source_use_count = source.nested_column->use_count();
        auto [status, accessor] = convertor.convert_column_data(0);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_NE(nullptr, accessor);
        EXPECT_EQ(source.values + 1, accessor->get_data());
        EXPECT_EQ(values, std::vector<int64_t>(source.values, source.values + values.size()));
        EXPECT_EQ(source_use_count, source.nested_column->use_count());
        if (nullable) {
            EXPECT_EQ(nullptr, accessor->get_data_at(0));
            EXPECT_EQ(source.values + 2, accessor->get_data_at(1));
        } else {
            EXPECT_EQ(source.values + 1, accessor->get_data_at(0));
        }
    }
}

TEST(OlapDataConvertorTest, FloatAndDoubleWithoutNanAreZeroCopy) {
    expect_float_without_nan_is_zero_copy<TYPE_FLOAT>(false);
    expect_float_without_nan_is_zero_copy<TYPE_FLOAT>(true);
    expect_float_without_nan_is_zero_copy<TYPE_DOUBLE>(false);
    expect_float_without_nan_is_zero_copy<TYPE_DOUBLE>(true);
}

TEST(OlapDataConvertorTest, FloatAndDoubleNanPayloadsDoNotMutateSource) {
    expect_nan_payloads_are_normalized_without_source_mutation<TYPE_FLOAT>();
    expect_nan_payloads_are_normalized_without_source_mutation<TYPE_DOUBLE>();
}

TEST(OlapDataConvertorTest, OnlyCurrentFloatSliceIsInspectedAndCopied) {
    const auto nan_before = from_bits<float>(0x7fc00011U);
    const auto nan_after = from_bits<float>(0x7fa12345U);
    const std::vector<float> values = {nan_before, 1.0F, 2.0F, nan_after};
    const std::vector<uint32_t> source_bits = {bits_of(nan_before), bits_of(1.0F), bits_of(2.0F),
                                               bits_of(nan_after)};
    auto source = create_numeric_source<TYPE_FLOAT>(values);
    OlapBlockDataConvertor convertor;
    convertor.add_column_data_convertor(
            create_tablet_column(FieldType::OLAP_FIELD_TYPE_FLOAT, false));

    auto [no_nan_status, no_nan_accessor] = convert(convertor, source, 1, 2);
    ASSERT_TRUE(no_nan_status.ok()) << no_nan_status;
    EXPECT_EQ(source.values + 1, no_nan_accessor->get_data());
    EXPECT_EQ(0, get_simple_convertor<TYPE_FLOAT>(convertor)->_converted_values.allocated_bytes());

    auto [nan_status, nan_accessor] = convert(convertor, source, 3, 1);
    ASSERT_TRUE(nan_status.ok()) << nan_status;
    ASSERT_NE(source.values + 3, nan_accessor->get_data());
    EXPECT_EQ(1, get_simple_convertor<TYPE_FLOAT>(convertor)->_converted_values.size());
    EXPECT_EQ(bits_of(std::numeric_limits<float>::quiet_NaN()),
              bits_of(*static_cast<const float*>(nan_accessor->get_data())));
    for (size_t i = 0; i < source_bits.size(); ++i) {
        EXPECT_EQ(source_bits[i], bits_of(source.values[i]));
    }
}

TEST(OlapDataConvertorTest, ReusesFloatingPointBufferAcrossNanAndNonNanBatches) {
    const auto nan_1 = from_bits<double>(0x7ff8000000000011ULL);
    const auto nan_2 = from_bits<double>(0x7ff123456789abcdULL);
    const std::vector<double> values = {nan_1, 1.0, 2.0, 3.0, nan_2, 4.0};
    std::vector<uint64_t> source_bits;
    for (double value : values) {
        source_bits.push_back(bits_of(value));
    }
    auto source = create_numeric_source<TYPE_DOUBLE>(values);
    OlapBlockDataConvertor convertor;
    convertor.add_column_data_convertor(
            create_tablet_column(FieldType::OLAP_FIELD_TYPE_DOUBLE, false));

    auto [first_status, first_accessor] = convert(convertor, source, 0, 2);
    ASSERT_TRUE(first_status.ok()) << first_status;
    const void* first_buffer = first_accessor->get_data();
    ASSERT_NE(source.values, first_buffer);
    const size_t first_capacity =
            get_simple_convertor<TYPE_DOUBLE>(convertor)->_converted_values.capacity();

    auto [second_status, second_accessor] = convert(convertor, source, 2, 2);
    ASSERT_TRUE(second_status.ok()) << second_status;
    EXPECT_EQ(source.values + 2, second_accessor->get_data());
    EXPECT_EQ(0, get_simple_convertor<TYPE_DOUBLE>(convertor)->_converted_values.size());
    EXPECT_EQ(first_capacity,
              get_simple_convertor<TYPE_DOUBLE>(convertor)->_converted_values.capacity());

    auto [third_status, third_accessor] = convert(convertor, source, 4, 2);
    ASSERT_TRUE(third_status.ok()) << third_status;
    EXPECT_EQ(first_buffer, third_accessor->get_data());
    EXPECT_EQ(first_capacity,
              get_simple_convertor<TYPE_DOUBLE>(convertor)->_converted_values.capacity());
    for (size_t i = 0; i < source_bits.size(); ++i) {
        EXPECT_EQ(source_bits[i], bits_of(source.values[i]));
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST(OlapDataConvertorTest, ArrayWithNullableDoubleLeafDoesNotMutateSource) {
    const auto nan_1 = from_bits<double>(0x7ff8000000000011ULL);
    const auto nan_2 = from_bits<double>(0xfff8123456789abcULL);
    const std::vector<double> values = {nan_1, 7.5, nan_2};
    auto nested_values = ColumnFloat64::create();
    nested_values->get_data().assign(values.begin(), values.end());
    const double* source_values = nested_values->get_data().data();
    const std::vector<uint64_t> source_bits = {bits_of(source_values[0]), bits_of(source_values[1]),
                                               bits_of(source_values[2])};
    const std::vector<UInt8> null_map = {0, 1, 0};
    auto nested_null_map = ColumnUInt8::create();
    nested_null_map->get_data().assign(null_map.begin(), null_map.end());
    auto nullable_values =
            ColumnNullable::create(std::move(nested_values), std::move(nested_null_map));
    const std::vector<UInt64> array_offsets = {2, 3};
    auto offsets = ColumnOffset64::create();
    offsets->get_data().assign(array_offsets.begin(), array_offsets.end());
    ColumnPtr array_column = ColumnArray::create(std::move(nullable_values), std::move(offsets));
    DataTypePtr array_type = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()));
    ColumnWithTypeAndName typed_array {std::move(array_column), std::move(array_type), "array"};

    TabletColumn item_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                             FieldType::OLAP_FIELD_TYPE_DOUBLE, true);
    item_column.set_name("item");
    item_column.set_length(sizeof(double));
    item_column.set_index_length(sizeof(double));
    TabletColumn array_tablet_column;
    array_tablet_column.set_name("array");
    array_tablet_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    array_tablet_column.set_is_nullable(false);
    array_tablet_column.add_sub_column(item_column);

    OlapBlockDataConvertor convertor;
    convertor.add_column_data_convertor(array_tablet_column);
    ASSERT_TRUE(convertor.set_source_content_with_specifid_column(typed_array, 0, 2, 0).ok());
    auto [status, accessor] = convertor.convert_column_data(0);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(nullptr, accessor);
    const auto* array_result = static_cast<const void* const*>(accessor->get_data());
    const auto* converted_values = static_cast<const double*>(array_result[2]);
    const auto* converted_null_map = static_cast<const UInt8*>(array_result[3]);
    ASSERT_NE(source_values, converted_values);
    EXPECT_EQ(bits_of(std::numeric_limits<double>::quiet_NaN()), bits_of(converted_values[0]));
    EXPECT_EQ(bits_of(7.5), bits_of(converted_values[1]));
    EXPECT_EQ(bits_of(std::numeric_limits<double>::quiet_NaN()), bits_of(converted_values[2]));
    ASSERT_NE(nullptr, converted_null_map);
    EXPECT_EQ(0, converted_null_map[0]);
    EXPECT_EQ(1, converted_null_map[1]);
    EXPECT_EQ(0, converted_null_map[2]);
    for (size_t i = 0; i < source_bits.size(); ++i) {
        EXPECT_EQ(source_bits[i], bits_of(source_values[i]));
    }
}

} // namespace doris

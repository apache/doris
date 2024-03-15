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

#include "vec/utils/arrow_column_to_doris_column.h"

#include <arrow/array.h> // IWYU pragma: keep
#include <arrow/array/array_decimal.h>
#include <arrow/array/data.h>
#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/util/bit_util.h>
#include <assert.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/decimalv2_value.h"
#include "runtime/types.h"
#include "util/binary_cast.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

template <typename ArrowType, typename ArrowCppType = typename arrow::TypeTraits<ArrowType>::CType>
ArrowCppType string_to_arrow_datetime(std::shared_ptr<ArrowType> type, const std::string& value) {
    VecDateTimeValue tv;
    tv.from_date_str(value.c_str(), value.size());
    int64_t unix_seconds = 0;
    tv.unix_timestamp(&unix_seconds, "UTC");
    if constexpr (std::is_same_v<ArrowType, arrow::TimestampType>) {
        arrow::TimeUnit::type unit = type->unit();
        VecDateTimeValue vdtv;
        vdtv.from_unixtime(unix_seconds, "UTC");
        vdtv.unix_timestamp(&unix_seconds, type->timezone());
        switch (unit) {
        case arrow::TimeUnit::SECOND:
            return unix_seconds;
        case arrow::TimeUnit::MILLI:
            return unix_seconds * 1000L;
        case arrow::TimeUnit::MICRO:
            return unix_seconds * 1000'000L;
        case arrow::TimeUnit::NANO:
            return unix_seconds * 1000'000'000L;
        default:
            assert(false);
        }
    } else if constexpr (std::is_same_v<ArrowType, arrow::Date32Type>) {
        return unix_seconds / (24 * 3600);
    } else if constexpr (std::is_same_v<ArrowType, arrow::Date64Type>) {
        return unix_seconds * 1000L;
    } else {
        assert(false);
    }
    return 0;
}

template <typename ArrowType, bool is_nullable,
          typename ArrowCppType = typename arrow::TypeTraits<ArrowType>::CType>
std::shared_ptr<arrow::Array> create_constant_numeric_array(size_t num_elements, ArrowCppType value,
                                                            std::shared_ptr<arrow::DataType> type,
                                                            size_t& counter) {
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
    buffers.resize(2);
    size_t null_bitmap_byte_size = (num_elements + 7) / 8;
    size_t data_byte_size = num_elements * sizeof(value);
    auto buffer0_res = arrow::AllocateBuffer(null_bitmap_byte_size);
    buffers[0] = std::move(buffer0_res.ValueOrDie());
    auto buffer1_res = arrow::AllocateBuffer(data_byte_size);
    buffers[1] = std::move(buffer1_res.ValueOrDie());
    auto* nulls = buffers[0]->mutable_data();
    auto* data = (ArrowCppType*)buffers[1]->mutable_data();

    for (auto i = 0; i < num_elements; ++i) {
        if (is_nullable && (i % 2 == 0)) {
            arrow::bit_util::ClearBit(nulls, i);
        } else {
            arrow::bit_util::SetBit(nulls, i);
        }
        data[i] = value;
    }
    counter += num_elements;
    using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
    auto array_data = std::make_shared<arrow::ArrayData>(type, num_elements, buffers);
    auto array = std::make_shared<ArrayType>(array_data);
    return std::static_pointer_cast<arrow::Array>(array);
}

template <typename ArrowType, typename ColumnType, bool is_nullable,
          typename ArrowCppType = typename arrow::TypeTraits<ArrowType>::CType>
void test_arrow_to_datetime_column(std::shared_ptr<ArrowType> type, ColumnWithTypeAndName& column,
                                   size_t num_elements, ArrowCppType arrow_datetime,
                                   VecDateTimeValue datetime, size_t& counter) {
    ASSERT_EQ(column.column->size(), counter);
    auto array = create_constant_numeric_array<ArrowType, is_nullable>(num_elements, arrow_datetime,
                                                                       type, counter);
    std::string time_zone = "UTC";
    if constexpr (std::is_same_v<ArrowType, arrow::TimestampType>) {
        time_zone = type->timezone();
    }
    auto ret = arrow_column_to_doris_column(array.get(), 0, column.column, column.type,
                                            num_elements, time_zone);
    ASSERT_EQ(ret.ok(), true);
    ASSERT_EQ(column.column->size(), counter);
    MutableColumnPtr data_column = nullptr;
    vectorized::ColumnNullable* nullable_column = nullptr;
    if (column.column->is_nullable()) {
        nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(column.column)).mutate().get());
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        data_column = (*std::move(column.column)).mutate();
    }
    auto& datetime_data = static_cast<ColumnType&>(*data_column).get_data();
    for (auto i = 0; i < num_elements; ++i) {
        auto idx = counter - num_elements + i;
        if (is_nullable) {
            ASSERT_NE(nullable_column, nullptr);
            NullMap& map_data = nullable_column->get_null_map_data();
            if (i % 2 == 0) {
                ASSERT_EQ(map_data[idx], true);
            } else {
                ASSERT_EQ(map_data[idx], false);
                auto val = binary_cast<VecDateTimeValue, Int64>(datetime);
                ASSERT_EQ(datetime_data[idx], val);
            }
        } else {
            auto val = binary_cast<VecDateTimeValue, Int64>(datetime);
            ASSERT_EQ(datetime_data[idx], val);
        }
    }
}

template <typename ArrowType, typename ColumnType, bool is_nullable>
void test_datetime(std::shared_ptr<ArrowType> type, const std::vector<std::string>& test_cases,
                   size_t num_elements) {
    TimezoneUtils::load_timezone_names();
    using ArrowCppType = typename arrow::TypeTraits<ArrowType>::CType;
    size_t counter = 0;
    auto pt = arrow_type_to_primitive_type(type->id());
    ASSERT_NE(pt, INVALID_TYPE);
    DataTypePtr data_type = DataTypeFactory::instance().create_data_type(pt, true);
    MutableColumnPtr data_column = data_type->create_column();
    ColumnWithTypeAndName column(std::move(data_column), data_type, "test_datatime_column");
    for (auto& value : test_cases) {
        ArrowCppType arrow_datetime = string_to_arrow_datetime<ArrowType>(type, value);
        VecDateTimeValue tv;
        tv.from_date_str(value.c_str(), value.size());
        test_arrow_to_datetime_column<ArrowType, ColumnType, is_nullable>(
                type, column, num_elements, arrow_datetime, tv, counter);
    }
}

TEST(ArrowColumnToDorisColumnTest, test_date32_to_date) {
    auto type = std::make_shared<arrow::Date32Type>();
    std::vector<std::string> test_cases = {{"1970-01-01"}, {"2021-05-30"}, {"2022-05-08"}};
    test_datetime<arrow::Date32Type, ColumnVector<Int64>, false>(type, test_cases, 32);
    test_datetime<arrow::Date32Type, ColumnVector<Int64>, true>(type, test_cases, 32);
}

TEST(ArrowColumnToDorisColumnTest, test_date64_to_datetime) {
    auto type = std::make_shared<arrow::Date64Type>();
    std::vector<std::string> test_cases = {
            {"1970-01-01 12:12:12"}, {"2021-05-30 22:22:22"}, {"2022-05-08 00:00:01"}};
    test_datetime<arrow::Date64Type, ColumnVector<Int64>, false>(type, test_cases, 64);
    test_datetime<arrow::Date64Type, ColumnVector<Int64>, true>(type, test_cases, 64);
}

TEST(ArrowColumnToDorisColumnTest, test_timestamp_to_datetime) {
    auto type = std::make_shared<arrow::Date64Type>();
    std::vector<std::string> test_cases = {
            {"1970-01-01 12:12:12"}, {"2021-05-30 22:22:22"}, {"2022-05-08 00:00:01"}};
    std::vector<std::string> zones = {"UTC",    "GMT",           "CST",          "+01:00",
                                      "-09:00", "Asia/Shanghai", "Europe/Zurich"};
    std::vector<arrow::TimeUnit::type> time_units = {arrow::TimeUnit::SECOND,
                                                     arrow::TimeUnit::MICRO, arrow::TimeUnit::MILLI,
                                                     arrow::TimeUnit::NANO};
    for (auto& unit : time_units) {
        for (auto& zone : zones) {
            auto type = std::make_shared<arrow::TimestampType>(unit, zone);
            test_datetime<arrow::TimestampType, ColumnVector<Int64>, false>(type, test_cases, 64);
            test_datetime<arrow::TimestampType, ColumnVector<Int64>, true>(type, test_cases, 64);
        }
    }
}

template <typename ArrowType, typename CppType, bool is_nullable,
          typename ColumnType = ColumnVector<CppType>,
          typename ArrowCppType = typename arrow::TypeTraits<ArrowType>::CType>
void test_arrow_to_numeric_column(std::shared_ptr<ArrowType> type, ColumnWithTypeAndName& column,
                                  size_t num_elements, ArrowCppType arrow_numeric, CppType numeric,
                                  size_t& counter) {
    ASSERT_EQ(column.column->size(), counter);
    auto array = create_constant_numeric_array<ArrowType, is_nullable>(num_elements, arrow_numeric,
                                                                       type, counter);
    auto ret = arrow_column_to_doris_column(array.get(), 0, column.column, column.type,
                                            num_elements, "UTC");
    ASSERT_EQ(ret.ok(), true);
    ASSERT_EQ(column.column->size(), counter);
    MutableColumnPtr data_column = nullptr;
    vectorized::ColumnNullable* nullable_column = nullptr;
    if (column.column->is_nullable()) {
        nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(column.column)).mutate().get());
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        data_column = (*std::move(column.column)).mutate();
    }
    auto& numeric_data = static_cast<ColumnType&>(*data_column).get_data();
    for (auto i = 0; i < num_elements; ++i) {
        auto idx = counter - num_elements + i;
        if (is_nullable) {
            ASSERT_NE(nullable_column, nullptr);
            NullMap& map_data = nullable_column->get_null_map_data();
            if (i % 2 == 0) {
                ASSERT_EQ(map_data[idx], true);
            } else {
                ASSERT_EQ(map_data[idx], false);
                ASSERT_EQ(numeric_data[idx], numeric);
            }
        } else {
            ASSERT_EQ(numeric_data[idx], numeric);
        }
    }
}

template <typename ArrowType, typename CppType, bool is_nullable,
          typename ColumnType = ColumnVector<CppType>>
void test_numeric(std::shared_ptr<ArrowType> type, const std::vector<CppType>& test_cases,
                  size_t num_elements) {
    using ArrowCppType = typename arrow::TypeTraits<ArrowType>::CType;
    size_t counter = 0;
    auto pt = arrow_type_to_primitive_type(type->id());
    ASSERT_NE(pt, INVALID_TYPE);
    DataTypePtr data_type = DataTypeFactory::instance().create_data_type(pt, true);
    MutableColumnPtr data_column = data_type->create_column();
    ColumnWithTypeAndName column(std::move(data_column), data_type, "test_numeric_column");
    for (auto& value : test_cases) {
        test_arrow_to_numeric_column<ArrowType, CppType, is_nullable>(
                type, column, num_elements, ArrowCppType(value), value, counter);
    }
}

TEST(ArrowColumnToDorisColumnTest, test_int8) {
    auto type = std::make_shared<arrow::Int8Type>();
    std::vector<Int8> test_cases = {1, -1, -128, 127, int8_t(255)};
    test_numeric<arrow::Int8Type, Int8, false>(type, test_cases, 64);
    test_numeric<arrow::Int8Type, Int8, true>(type, test_cases, 64);
}

TEST(ArrowColumnToDorisColumnTest, test_uint8) {
    auto type = std::make_shared<arrow::UInt8Type>();
    std::vector<UInt8> test_cases = {uint8_t(-1), uint8_t(1), uint8_t(-128), uint8_t(127),
                                     uint8_t(255)};
    test_numeric<arrow::UInt8Type, UInt8, false>(type, test_cases, 64);
    test_numeric<arrow::UInt8Type, UInt8, true>(type, test_cases, 64);
}

TEST(ArrowColumnToDorisColumnTest, test_uint16) {
    auto type = std::make_shared<arrow::UInt16Type>();
    std::vector<UInt16> test_cases = {uint16_t(-1), uint16_t(1), uint16_t(-128), uint16_t(127),
                                      uint16_t(65535)};
    test_numeric<arrow::UInt16Type, UInt16, false>(type, test_cases, 64);
    test_numeric<arrow::UInt16Type, UInt16, true>(type, test_cases, 64);
}

TEST(ArrowColumnToDorisColumnTest, test_uint32) {
    auto type = std::make_shared<arrow::UInt32Type>();
    std::vector<UInt32> test_cases = {uint32_t(-1), uint32_t(1), uint32_t(-65535), uint32_t(65535),
                                      uint32_t(4294967295)};
    test_numeric<arrow::UInt32Type, UInt32, false>(type, test_cases, 64);
    test_numeric<arrow::UInt32Type, UInt32, true>(type, test_cases, 64);
}

TEST(ArrowColumnToDorisColumnTest, test_uint64) {
    auto type = std::make_shared<arrow::UInt64Type>();
    std::vector<UInt64> test_cases = {uint64_t(-1),
                                      uint64_t(1),
                                      uint64_t(-4294967295),
                                      uint64_t(4294967295),
                                      uint64_t(std::numeric_limits<uint64_t>::min()),
                                      uint64_t(std::numeric_limits<uint64_t>::max())};
    test_numeric<arrow::UInt64Type, UInt64, false>(type, test_cases, 64);
    test_numeric<arrow::UInt64Type, UInt64, true>(type, test_cases, 64);
}

TEST(ArrowColumnToDorisColumnTest, test_float64) {
    auto type = std::make_shared<arrow::DoubleType>();
    std::vector<double> test_cases = {double(-1.11f),
                                      double(1.11f),
                                      double(-4294967295),
                                      double(4294967295),
                                      double(std::numeric_limits<double>::min()),
                                      double(std::numeric_limits<double>::max())};
    test_numeric<arrow::DoubleType, Float64, false>(type, test_cases, 64);
    test_numeric<arrow::DoubleType, Float64, true>(type, test_cases, 64);
}

template <bool is_nullable>
std::shared_ptr<arrow::Array> create_decimal_array(size_t num_elements, int128_t decimal,
                                                   std::shared_ptr<arrow::Decimal128Type> type,
                                                   size_t& counter) {
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
    buffers.resize(2);
    auto byte_width = type->byte_width();
    auto buffer0_res = arrow::AllocateBuffer((num_elements + 7) / 8);
    buffers[0] = std::move(buffer0_res.ValueOrDie());
    auto buffer1_res = arrow::AllocateBuffer(byte_width * num_elements);
    buffers[1] = std::move(buffer1_res.ValueOrDie());
    auto* nulls = buffers[0]->mutable_data();
    auto* data = buffers[1]->mutable_data();
    for (auto i = 0; i < num_elements; ++i) {
        if (is_nullable && (i % 2 == 0)) {
            arrow::bit_util::ClearBit(nulls, i);
        } else {
            arrow::bit_util::SetBit(nulls, i);
            memcpy(data + i * byte_width, &decimal, sizeof(decimal));
        }
    }
    auto array_data = std::make_shared<arrow::ArrayData>(type, num_elements, buffers);
    auto array = std::make_shared<arrow::Decimal128Array>(array_data);
    counter += num_elements;
    return array;
}

template <bool is_nullable>
void test_arrow_to_decimal_column(std::shared_ptr<arrow::Decimal128Type> type,
                                  ColumnWithTypeAndName& column, size_t num_elements,
                                  int128_t arrow_value, int128_t expect_value, size_t& counter) {
    ASSERT_EQ(column.column->size(), counter);
    auto array = create_decimal_array<is_nullable>(num_elements, arrow_value, type, counter);
    auto ret = arrow_column_to_doris_column(array.get(), 0, column.column, column.type,
                                            num_elements, "UTC");
    ASSERT_EQ(ret.ok(), true);
    ASSERT_EQ(column.column->size(), counter);
    MutableColumnPtr data_column = nullptr;
    vectorized::ColumnNullable* nullable_column = nullptr;
    if (column.column->is_nullable()) {
        nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(column.column)).mutate().get());
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        data_column = (*std::move(column.column)).mutate();
    }
    auto& decimal_data =
            static_cast<ColumnDecimal<vectorized::Decimal128V2>&>(*data_column).get_data();
    for (auto i = 0; i < num_elements; ++i) {
        auto idx = counter - num_elements + i;
        if (is_nullable) {
            ASSERT_NE(nullable_column, nullptr);
            NullMap& map_data = nullable_column->get_null_map_data();
            if (i % 2 == 0) {
                ASSERT_EQ(map_data[idx], true);
            } else {
                ASSERT_EQ(map_data[idx], false);
                ASSERT_EQ(Int128(decimal_data[idx]), expect_value);
            }
        } else {
            ASSERT_EQ(Int128(decimal_data[idx]), expect_value);
        }
    }
}

template <int bytes_width, bool is_nullable = false>
std::shared_ptr<arrow::Array> create_fixed_size_binary_array(int64_t num_elements,
                                                             const std::string& value,
                                                             size_t& counter) {
    auto data_buf_size = bytes_width * num_elements;
    auto data_buf_tmp = arrow::AllocateBuffer(data_buf_size);
    std::shared_ptr<arrow::Buffer> data_buf = std::move(data_buf_tmp.ValueOrDie());
    auto* p = data_buf->mutable_data();

    auto null_bitmap_bytes = (num_elements + 7) / 8;
    auto null_bitmap_tmp = arrow::AllocateBuffer(null_bitmap_bytes);
    std::shared_ptr<arrow::Buffer> null_bitmap_buf = std::move(null_bitmap_tmp.ValueOrDie());
    auto* nulls = null_bitmap_buf->mutable_data();

    for (auto i = 0; i < num_elements; ++i) {
        if (is_nullable && i % 2 == 0) {
            arrow::bit_util::ClearBit(nulls, i);
        } else {
            arrow::bit_util::SetBit(nulls, i);
        }
        memcpy(p, value.c_str(), std::min(value.size() + 1, (std::string::size_type)bytes_width));
        p += bytes_width;
    }
    auto type = std::make_shared<arrow::FixedSizeBinaryType>(bytes_width);
    auto array = std::make_shared<arrow::FixedSizeBinaryArray>(type, num_elements, data_buf,
                                                               null_bitmap_buf);
    counter += num_elements;
    return std::static_pointer_cast<arrow::Array>(array);
}

template <int bytes_width, bool is_nullable>
void test_arrow_to_fixed_binary_column(ColumnWithTypeAndName& column, size_t num_elements,
                                       const std::string value, size_t& counter) {
    ASSERT_EQ(column.column->size(), counter);
    auto array =
            create_fixed_size_binary_array<bytes_width, is_nullable>(num_elements, value, counter);
    auto ret = arrow_column_to_doris_column(array.get(), 0, column.column, column.type,
                                            num_elements, "UTC");
    ASSERT_EQ(ret.ok(), true);
    ASSERT_EQ(column.column->size(), counter);
    MutableColumnPtr data_column = nullptr;
    vectorized::ColumnNullable* nullable_column = nullptr;
    if (column.column->is_nullable()) {
        nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(column.column)).mutate().get());
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        data_column = (*std::move(column.column)).mutate();
    }
    auto& string_column = static_cast<ColumnString&>(*data_column);
    auto string_size = std::min((std::string::size_type)bytes_width, value.size());
    for (auto i = 0; i < num_elements; ++i) {
        auto idx = counter - num_elements + i;
        auto s = string_column.get_data_at(idx);
        if (is_nullable) {
            ASSERT_NE(nullable_column, nullptr);
            NullMap& map_data = nullable_column->get_null_map_data();
            if (i % 2 == 0) {
                ASSERT_EQ(map_data[idx], true);
                ASSERT_EQ(s.size, 0);
            } else {
                ASSERT_EQ(map_data[idx], false);
                ASSERT_EQ(value.compare(0, string_size, s.to_string(), 0, string_size), 0);
            }
        } else {
            ASSERT_EQ(value.compare(0, string_size, s.to_string(), 0, string_size), 0);
        }
    }
}

template <int bytes_width, bool is_nullable>
void test_fixed_binary(const std::vector<std::string>& test_cases, size_t num_elements) {
    size_t counter = 0;
    auto pt = arrow_type_to_primitive_type(::arrow::Type::FIXED_SIZE_BINARY);
    ASSERT_NE(pt, INVALID_TYPE);
    DataTypePtr data_type = DataTypeFactory::instance().create_data_type(pt, true);
    MutableColumnPtr data_column = data_type->create_column();
    ColumnWithTypeAndName column(std::move(data_column), data_type, "test_fixed_binary_column");
    for (auto& value : test_cases) {
        test_arrow_to_fixed_binary_column<bytes_width, is_nullable>(column, num_elements, value,
                                                                    counter);
    }
}

TEST(ArrowColumnToDorisColumnTest, test_fixed_binary) {
    std::vector<std::string> test_cases = {"1.2345678", "-12.34567890", "99999999999.99999999",
                                           "-99999999999.99999999"};
    test_fixed_binary<10, false>(test_cases, 64);
    test_fixed_binary<10, true>(test_cases, 64);

    test_fixed_binary<255, false>(test_cases, 64);
    test_fixed_binary<255, true>(test_cases, 64);
}

template <typename ArrowType, bool is_nullable = false>
std::shared_ptr<arrow::Array> create_binary_array(int64_t num_elements, const std::string& value,
                                                  size_t& counter) {
    using offset_type = typename ArrowType::offset_type;
    size_t offsets_bytes = (num_elements + 1) * sizeof(offset_type);
    auto offsets_buf_tmp = arrow::AllocateBuffer(offsets_bytes);
    std::shared_ptr<arrow::Buffer> offsets_buf = std::move(offsets_buf_tmp.ValueOrDie());
    auto* offsets = (offset_type*)offsets_buf->mutable_data();
    offsets[0] = 0;

    auto value_size = value.size();
    size_t data_bytes = value_size * num_elements;
    auto data_buf_tmp = arrow::AllocateBuffer(data_bytes);
    std::shared_ptr<arrow::Buffer> data_buf = std::move(data_buf_tmp.ValueOrDie());
    auto* data = data_buf->mutable_data();

    auto null_bitmap_bytes = (num_elements + 7) / 8;
    auto null_bitmap_tmp = arrow::AllocateBuffer(null_bitmap_bytes);
    std::shared_ptr<arrow::Buffer> null_bitmap = std::move(null_bitmap_tmp.ValueOrDie());
    auto nulls = null_bitmap->mutable_data();
    auto data_off = 0;
    for (auto i = 0; i < num_elements; ++i) {
        if (is_nullable && i % 2 == 0) {
            arrow::bit_util::ClearBit(nulls, i);
        } else {
            arrow::bit_util::SetBit(nulls, i);
            memcpy(data + data_off, value.data(), value_size);
            data_off += value_size;
        }
        offsets[i + 1] = data_off;
    }

    using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
    auto array = std::make_shared<ArrayType>(num_elements, offsets_buf, data_buf, null_bitmap);
    counter += num_elements;
    return std::static_pointer_cast<arrow::Array>(array);
}

template <typename ArrowType, bool is_nullable,
          typename ArrowCppType = typename arrow::TypeTraits<ArrowType>::CType>
void test_arrow_to_binary_column(ColumnWithTypeAndName& column, size_t num_elements,
                                 ArrowCppType value, size_t& counter) {
    ASSERT_EQ(column.column->size(), counter);
    auto array = create_binary_array<ArrowType, is_nullable>(num_elements, value, counter);
    auto ret = arrow_column_to_doris_column(array.get(), 0, column.column, column.type,
                                            num_elements, "UTC");
    ASSERT_EQ(ret.ok(), true);
    ASSERT_EQ(column.column->size(), counter);
    MutableColumnPtr data_column = nullptr;
    vectorized::ColumnNullable* nullable_column = nullptr;
    if (column.column->is_nullable()) {
        nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(column.column)).mutate().get());
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        data_column = (*std::move(column.column)).mutate();
    }
    auto& string_column = static_cast<ColumnString&>(*data_column);
    for (auto i = 0; i < num_elements; ++i) {
        auto idx = counter - num_elements + i;
        auto s = string_column.get_data_at(idx);
        if (is_nullable) {
            ASSERT_NE(nullable_column, nullptr);
            NullMap& map_data = nullable_column->get_null_map_data();
            if (i % 2 == 0) {
                ASSERT_EQ(map_data[idx], true);
                ASSERT_EQ(s.size, 0);
            } else {
                ASSERT_EQ(map_data[idx], false);
                ASSERT_EQ(value, s.to_string());
            }
        } else {
            ASSERT_EQ(value, s.to_string());
        }
    }
}

template <typename ArrowType, bool is_nullable>
void test_binary(const std::vector<std::string>& test_cases, size_t num_elements) {
    size_t counter = 0;
    DataTypePtr data_type = DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, true);
    MutableColumnPtr data_column = data_type->create_column();
    ColumnWithTypeAndName column(std::move(data_column), data_type, "test_binary_column");
    for (auto& value : test_cases) {
        test_arrow_to_binary_column<ArrowType, is_nullable>(column, num_elements, value, counter);
    }
}

TEST(ArrowColumnToDorisColumnTest, test_binary) {
    std::vector<std::string> test_cases = {"1.2345678", "-12.34567890", "99999999999.99999999",
                                           "-99999999999.99999999"};
    test_binary<arrow::StringType, false>(test_cases, 64);
    test_binary<arrow::StringType, true>(test_cases, 64);

    test_binary<arrow::BinaryType, false>(test_cases, 64);
    test_binary<arrow::BinaryType, true>(test_cases, 64);
}

template <typename ArrowValueType, bool is_nullable = false>
std::shared_ptr<arrow::Array> create_array_array(std::vector<ColumnArray::Offset64>& vec_offsets,
                                                 std::vector<bool>& null_map,
                                                 std::shared_ptr<arrow::DataType> value_type,
                                                 std::shared_ptr<arrow::Array> values,
                                                 size_t& counter) {
    using offset_type = typename arrow::ListType::offset_type;
    size_t num_rows = vec_offsets.size() - 1;
    EXPECT_EQ(null_map.size(), num_rows);
    size_t offsets_bytes = (vec_offsets.size()) * sizeof(offset_type);
    auto offsets_buf_tmp = arrow::AllocateBuffer(offsets_bytes);
    std::shared_ptr<arrow::Buffer> offsets_buf = std::move(offsets_buf_tmp.ValueOrDie());
    auto* offsets = (offset_type*)offsets_buf->mutable_data();
    offsets[0] = 0;

    auto null_bitmap_bytes = ((num_rows) + 7) / 8;
    auto null_bitmap_tmp = arrow::AllocateBuffer(null_bitmap_bytes);
    std::shared_ptr<arrow::Buffer> null_bitmap = std::move(null_bitmap_tmp.ValueOrDie());
    auto nulls = null_bitmap->mutable_data();
    for (auto i = 0; i < num_rows; ++i) {
        if (is_nullable && null_map[i]) {
            arrow::bit_util::ClearBit(nulls, i);
        } else {
            arrow::bit_util::SetBit(nulls, i);
        }
        offsets[i + 1] = vec_offsets[i + 1];
    }

    auto array = std::make_shared<arrow::ListArray>(value_type, num_rows, offsets_buf, values,
                                                    null_bitmap);
    counter += num_rows;
    return std::static_pointer_cast<arrow::Array>(array);
}

template <typename ArrowType, bool is_nullable>
void test_arrow_to_array_column(ColumnWithTypeAndName& column,
                                std::vector<ColumnArray::Offset64>& vec_offsets,
                                std::vector<bool>& null_map,
                                std::shared_ptr<arrow::DataType> value_type,
                                std::shared_ptr<arrow::Array> values, const std::string& value,
                                size_t& counter) {
    auto array = create_array_array<ArrowType, is_nullable>(vec_offsets, null_map, value_type,
                                                            values, counter);
    auto old_size = column.column->size();
    auto ret = arrow_column_to_doris_column(array.get(), 0, column.column, column.type,
                                            vec_offsets.size() - 1, "UTC");
    ASSERT_EQ(ret.ok(), true);
    ASSERT_EQ(column.column->size() - old_size, counter);
    MutableColumnPtr data_column = nullptr;
    vectorized::ColumnNullable* nullable_column = nullptr;
    if (column.column->is_nullable()) {
        nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(column.column)).mutate().get());
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        data_column = (*std::move(column.column)).mutate();
    }
    auto& array_column = static_cast<ColumnArray&>(*data_column);
    EXPECT_EQ(array_column.size() - old_size, vec_offsets.size() - 1);
    for (size_t i = 0; i < array_column.size() - old_size; ++i) {
        auto v = get<Array>(array_column[old_size + i]);
        EXPECT_EQ(v.size(), vec_offsets[i + 1] - vec_offsets[i]);
        EXPECT_EQ(v.size(), array_column.get_offsets()[old_size + i] -
                                    array_column.get_offsets()[old_size + i - 1]);
        if (is_nullable) {
            ASSERT_NE(nullable_column, nullptr);
            NullMap& map_data = nullable_column->get_null_map_data();
            ASSERT_EQ(map_data[old_size + i], null_map[i]);
            if (!null_map[i]) {
                // check value
                for (size_t j = 0; j < v.size(); ++j) {
                    // in nested column, values like [null, xx, null, xx, ...]
                    if ((vec_offsets[i] + j) % 2 != 0) {
                        EXPECT_EQ(value, get<std::string>(v[j]));
                    }
                }
            }
        } else {
            // check value
            for (size_t j = 0; j < v.size(); ++j) {
                EXPECT_EQ(value, get<std::string>(v[j]));
            }
        }
    }
}

template <typename ArrowType, bool is_nullable>
void test_array(const std::vector<std::string>& test_cases, size_t num_elements,
                std::vector<ColumnArray::Offset64>& vec_offsets, std::vector<bool>& null_map,
                std::shared_ptr<arrow::DataType> value_type) {
    TypeDescriptor type(TYPE_ARRAY);
    type.children.push_back(TYPE_VARCHAR);
    type.contains_nulls.push_back(true);
    DataTypePtr data_type = DataTypeFactory::instance().create_data_type(type, true);
    for (auto& value : test_cases) {
        MutableColumnPtr data_column = data_type->create_column();
        ColumnWithTypeAndName column(std::move(data_column), data_type, "test_array_column");
        // create nested column
        size_t nested_counter = 0;
        auto array =
                create_binary_array<ArrowType, is_nullable>(num_elements, value, nested_counter);
        ASSERT_EQ(nested_counter, num_elements);
        size_t counter = 0;
        test_arrow_to_array_column<ArrowType, is_nullable>(column, vec_offsets, null_map,
                                                           value_type, array, value, counter);
        // multi arrow array can merge into one array column, here test again with non empty array column
        counter = 0;
        test_arrow_to_array_column<ArrowType, is_nullable>(column, vec_offsets, null_map,
                                                           value_type, array, value, counter);
    }
}

TEST(ArrowColumnToDorisColumnTest, test_array) {
    std::vector<std::string> test_cases = {"1.2345678", "-12.34567890", "99999999999.99999999",
                                           "-99999999999.99999999"};
    std::vector<ColumnArray::Offset64> vec_offsets = {0, 3, 3, 4, 6, 6, 64};
    std::vector<bool> null_map = {false, true, false, false, false, false};
    test_array<arrow::BinaryType, false>(test_cases, 64, vec_offsets, null_map,
                                         arrow::list(arrow::binary()));
    test_array<arrow::BinaryType, true>(test_cases, 64, vec_offsets, null_map,
                                        arrow::list(arrow::binary()));
}
} // namespace doris::vectorized

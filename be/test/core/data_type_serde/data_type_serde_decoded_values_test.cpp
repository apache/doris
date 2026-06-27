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

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/value/timestamptz_value.h"
#include "util/timezone_utils.h"

namespace doris {
namespace {

struct ReadColumnResult {
    Status status;
    MutableColumnPtr column;
};

template <typename T>
DecodedColumnView make_fixed_view(DecodedValueKind kind, const std::vector<T>& values,
                                  const std::vector<uint8_t>* null_map = nullptr) {
    DecodedColumnView view;
    view.value_kind = kind;
    view.row_count = null_map != nullptr ? static_cast<int64_t>(null_map->size())
                                         : static_cast<int64_t>(values.size());
    view.values = values.empty() ? nullptr : reinterpret_cast<const uint8_t*>(values.data());
    view.null_map = null_map == nullptr || null_map->empty() ? nullptr : null_map->data();
    return view;
}

DecodedColumnView make_binary_view(DecodedValueKind kind, const std::vector<StringRef>& values,
                                   int fixed_length = -1,
                                   const std::vector<uint8_t>* null_map = nullptr) {
    DecodedColumnView view;
    view.value_kind = kind;
    view.row_count = null_map != nullptr ? static_cast<int64_t>(null_map->size())
                                         : static_cast<int64_t>(values.size());
    view.binary_values = values.empty() ? nullptr : &values;
    view.fixed_length = fixed_length;
    view.null_map = null_map == nullptr || null_map->empty() ? nullptr : null_map->data();
    return view;
}

DecodedColumnView make_bool_view(const std::vector<uint8_t>& values,
                                 const std::vector<uint8_t>* null_map = nullptr) {
    DecodedColumnView view;
    view.value_kind = DecodedValueKind::BOOL;
    view.row_count = null_map != nullptr ? static_cast<int64_t>(null_map->size())
                                         : static_cast<int64_t>(values.size());
    view.values = values.empty() ? nullptr : reinterpret_cast<const uint8_t*>(values.data());
    view.null_map = null_map == nullptr || null_map->empty() ? nullptr : null_map->data();
    return view;
}

DecodedColumnView with_logical_integer(DecodedColumnView view, int bit_width, bool is_signed) {
    view.logical_integer_bit_width = bit_width;
    view.logical_integer_is_signed = is_signed;
    return view;
}

ReadColumnResult read_column(const DataTypePtr& type, const DecodedColumnView& view) {
    auto column = type->create_column();
    auto status = type->get_serde()->read_column_from_decoded_values(*column, view);
    return {std::move(status), std::move(column)};
}

void expect_not_supported(const Status& status) {
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(ErrorCode::NOT_IMPLEMENTED_ERROR, status.code()) << status;
}

void expect_corruption(const Status& status) {
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(ErrorCode::CORRUPTION, status.code()) << status;
}

void expect_data_quality_error(const Status& status) {
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(ErrorCode::DATA_QUALITY_ERROR, status.code()) << status;
}

void expect_column_strings(const IDataType& type, const IColumn& column,
                           const std::vector<std::string>& expected) {
    ASSERT_EQ(expected.size(), column.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(expected[row], type.to_string(column, row)) << "row=" << row;
    }
}

void expect_binary_column(const IColumn& column, const std::vector<std::string>& expected) {
    const auto& string_column = assert_cast<const ColumnString&>(column);
    ASSERT_EQ(expected.size(), string_column.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        const auto value = string_column.get_data_at(row);
        EXPECT_EQ(expected[row], std::string(value.data, value.size)) << "row=" << row;
    }
}

void expect_nullable_all_null(const IColumn& column, size_t expected_size) {
    const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
    ASSERT_EQ(expected_size, nullable_column.size());
    ASSERT_EQ(expected_size, nullable_column.get_nested_column().size());
    for (size_t row = 0; row < expected_size; ++row) {
        EXPECT_TRUE(nullable_column.is_null_at(row)) << "row=" << row;
    }
}

Field read_field(const DataTypePtr& type, const DecodedColumnView& view) {
    Field field;
    auto status = type->get_serde()->read_field_from_decoded_value(*type, &field, view);
    EXPECT_TRUE(status.ok()) << status;
    return field;
}

Status read_field_status(const DataTypePtr& type, const DecodedColumnView& view) {
    Field field;
    return type->get_serde()->read_field_from_decoded_value(*type, &field, view);
}

std::vector<StringRef> string_refs(const std::vector<std::string>& values) {
    std::vector<StringRef> refs;
    refs.reserve(values.size());
    for (const auto& value : values) {
        refs.emplace_back(value.data(), value.size());
    }
    return refs;
}

#pragma pack(1)
struct TestInt96Timestamp {
    int64_t nanos_of_day;
    int32_t julian_day;
};
#pragma pack()

static_assert(sizeof(TestInt96Timestamp) == 12);

Decimal128V3 decimal128_v3(Int128 value) {
    return Decimal128V3(value);
}

Decimal256 decimal256_from_int64(int64_t value) {
    return Decimal256(wide::Int256(value));
}

} // namespace

// ----------------------------------------------------------------------
// Base SerDe behavior
// ----------------------------------------------------------------------
// These cases define the default contract for types that have not implemented decoded-value
// materialization. Batch reads must report NotSupported, and the single-field path must surface
// the same error because it is implemented by delegating to the batch reader.

TEST(DataTypeSerDeDecodedValuesTest, BaseSerdeRejectsDecodedValues) {
    auto type = std::make_shared<DataTypeNothing>();
    std::vector<int32_t> values = {1};
    auto view = make_fixed_view(DecodedValueKind::INT32, values);

    auto result = read_column(type, view);

    expect_not_supported(result.status);
    EXPECT_EQ(0, result.column->size());
    EXPECT_NE(std::string::npos, result.status.to_string().find("Nothing"));
}

TEST(DataTypeSerDeDecodedValuesTest, BaseFieldUsesBatchReaderAndPropagatesError) {
    auto type = std::make_shared<DataTypeNothing>();
    std::vector<int32_t> values = {1};
    auto view = make_fixed_view(DecodedValueKind::INT32, values);
    Field field = Field::create_field<TYPE_INT>(123);

    auto status = type->get_serde()->read_field_from_decoded_value(*type, &field, view);

    expect_not_supported(status);
    EXPECT_EQ(TYPE_INT, field.get_type());
    EXPECT_EQ(123, field.get<TYPE_INT>());
}

// ----------------------------------------------------------------------
// Number SerDe happy path
// ----------------------------------------------------------------------
// The numeric matrix verifies physical kind dispatch and the exact static_cast behavior used by
// the reader. Narrow integer overflow is intentionally locked to current C++ conversion behavior;
// if product semantics change to reject overflow, these expectations should be updated with the
// implementation change.

TEST(DataTypeSerDeDecodedValuesTest, ReadBooleanFromBool) {
    auto type = std::make_shared<DataTypeBool>();
    std::vector<uint8_t> values = {true, false, true};
    auto view = make_bool_view(values);

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& column = assert_cast<const ColumnBool&>(*result.column);
    ASSERT_EQ(3, column.size());
    EXPECT_EQ(1, column.get_element(0));
    EXPECT_EQ(0, column.get_element(1));
    EXPECT_EQ(1, column.get_element(2));
}

TEST(DataTypeSerDeDecodedValuesTest, ReadSignedIntegersFromInt32) {
    std::vector<int32_t> values = {0, 1, -1, 127, -128};
    auto view = make_fixed_view(DecodedValueKind::INT32, values);

    {
        auto result = read_column(std::make_shared<DataTypeInt8>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt8&>(*result.column);
        ASSERT_EQ(values.size(), column.size());
        EXPECT_EQ(0, column.get_element(0));
        EXPECT_EQ(1, column.get_element(1));
        EXPECT_EQ(-1, column.get_element(2));
        EXPECT_EQ(127, column.get_element(3));
        EXPECT_EQ(-128, column.get_element(4));
    }
    {
        auto result = read_column(std::make_shared<DataTypeInt16>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt16&>(*result.column);
        ASSERT_EQ(values.size(), column.size());
        for (size_t row = 0; row < values.size(); ++row) {
            EXPECT_EQ(static_cast<int16_t>(values[row]), column.get_element(row));
        }
    }
    {
        auto result = read_column(std::make_shared<DataTypeInt32>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt32&>(*result.column);
        ASSERT_EQ(values.size(), column.size());
        for (size_t row = 0; row < values.size(); ++row) {
            EXPECT_EQ(values[row], column.get_element(row));
        }
    }
    {
        auto result = read_column(std::make_shared<DataTypeInt64>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt64&>(*result.column);
        ASSERT_EQ(values.size(), column.size());
        for (size_t row = 0; row < values.size(); ++row) {
            EXPECT_EQ(static_cast<int64_t>(values[row]), column.get_element(row));
        }
    }
    {
        auto result = read_column(std::make_shared<DataTypeInt128>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt128&>(*result.column);
        ASSERT_EQ(values.size(), column.size());
        for (size_t row = 0; row < values.size(); ++row) {
            EXPECT_EQ(static_cast<__int128_t>(values[row]), column.get_element(row));
        }
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadSignedIntegersFromInt64) {
    std::vector<int64_t> values = {0, 1, -1, 127, -128};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);

    auto tiny = read_column(std::make_shared<DataTypeInt8>(), view);
    ASSERT_TRUE(tiny.status.ok()) << tiny.status;
    const auto& tiny_column = assert_cast<const ColumnInt8&>(*tiny.column);
    EXPECT_EQ(127, tiny_column.get_element(3));
    EXPECT_EQ(-128, tiny_column.get_element(4));

    auto small = read_column(std::make_shared<DataTypeInt16>(), view);
    ASSERT_TRUE(small.status.ok()) << small.status;
    const auto& small_column = assert_cast<const ColumnInt16&>(*small.column);
    EXPECT_EQ(127, small_column.get_element(3));
    EXPECT_EQ(-128, small_column.get_element(4));

    auto integer = read_column(std::make_shared<DataTypeInt32>(), view);
    ASSERT_TRUE(integer.status.ok()) << integer.status;
    const auto& int_column = assert_cast<const ColumnInt32&>(*integer.column);
    EXPECT_EQ(127, int_column.get_element(3));
    EXPECT_EQ(-128, int_column.get_element(4));

    auto bigint = read_column(std::make_shared<DataTypeInt64>(), view);
    ASSERT_TRUE(bigint.status.ok()) << bigint.status;
    const auto& bigint_column = assert_cast<const ColumnInt64&>(*bigint.column);
    ASSERT_EQ(values.size(), bigint_column.size());
    for (size_t row = 0; row < values.size(); ++row) {
        EXPECT_EQ(values[row], bigint_column.get_element(row));
    }

    auto largeint = read_column(std::make_shared<DataTypeInt128>(), view);
    ASSERT_TRUE(largeint.status.ok()) << largeint.status;
    const auto& largeint_column = assert_cast<const ColumnInt128&>(*largeint.column);
    ASSERT_EQ(values.size(), largeint_column.size());
    for (size_t row = 0; row < values.size(); ++row) {
        EXPECT_EQ(static_cast<__int128_t>(values[row]), largeint_column.get_element(row));
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadIntegersFromUnsignedSources) {
    {
        std::vector<uint32_t> values = {0, 1, std::numeric_limits<uint32_t>::max()};
        auto view = make_fixed_view(DecodedValueKind::UINT32, values);
        auto result = read_column(std::make_shared<DataTypeInt64>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt64&>(*result.column);
        EXPECT_EQ(0, column.get_element(0));
        EXPECT_EQ(1, column.get_element(1));
        EXPECT_EQ(static_cast<int64_t>(std::numeric_limits<uint32_t>::max()),
                  column.get_element(2));
    }
    {
        std::vector<uint64_t> values = {0, 1, std::numeric_limits<uint64_t>::max()};
        auto view = make_fixed_view(DecodedValueKind::UINT64, values);
        auto result = read_column(std::make_shared<DataTypeInt128>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt128&>(*result.column);
        EXPECT_EQ(0, column.get_element(0));
        EXPECT_EQ(1, column.get_element(1));
        EXPECT_EQ(static_cast<__int128_t>(std::numeric_limits<uint64_t>::max()),
                  column.get_element(2));
    }
    {
        std::vector<uint64_t> values = {static_cast<uint64_t>(std::numeric_limits<int64_t>::max())};
        auto view = make_fixed_view(DecodedValueKind::UINT64, values);
        auto result = read_column(std::make_shared<DataTypeInt64>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt64&>(*result.column);
        EXPECT_EQ(std::numeric_limits<int64_t>::max(), column.get_element(0));
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadUnsignedLogicalIntegersCastsPhysicalValues) {
    {
        std::vector<int32_t> values = {0, 127, 255, 32767, 65535, -1};
        auto view =
                with_logical_integer(make_fixed_view(DecodedValueKind::INT32, values), 8, false);
        auto result = read_column(std::make_shared<DataTypeInt16>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt16&>(*result.column);
        ASSERT_EQ(values.size(), column.size());
        EXPECT_EQ(0, column.get_element(0));
        EXPECT_EQ(127, column.get_element(1));
        EXPECT_EQ(255, column.get_element(2));
        EXPECT_EQ(255, column.get_element(3));
        EXPECT_EQ(255, column.get_element(4));
        EXPECT_EQ(255, column.get_element(5));
    }
    {
        std::vector<int32_t> values = {32767, 65535, -1};
        auto view =
                with_logical_integer(make_fixed_view(DecodedValueKind::INT32, values), 16, false);
        auto result = read_column(std::make_shared<DataTypeInt32>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt32&>(*result.column);
        ASSERT_EQ(values.size(), column.size());
        EXPECT_EQ(32767, column.get_element(0));
        EXPECT_EQ(65535, column.get_element(1));
        EXPECT_EQ(65535, column.get_element(2));
    }
    {
        std::vector<int32_t> values = {-1};
        auto view =
                with_logical_integer(make_fixed_view(DecodedValueKind::UINT32, values), 32, false);
        auto result = read_column(std::make_shared<DataTypeInt64>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt64&>(*result.column);
        ASSERT_EQ(1, column.size());
        EXPECT_EQ(4294967295LL, column.get_element(0));
    }
    {
        std::vector<int64_t> values = {-1};
        auto view =
                with_logical_integer(make_fixed_view(DecodedValueKind::UINT64, values), 64, false);
        auto result = read_column(std::make_shared<DataTypeInt128>(), view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnInt128&>(*result.column);
        ASSERT_EQ(1, column.size());
        EXPECT_EQ(static_cast<__int128_t>(std::numeric_limits<uint64_t>::max()),
                  column.get_element(0));
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadSignedLogicalIntegersCastsPhysicalValues) {
    std::vector<int32_t> values = {127, 128, 255, -1};
    auto view = with_logical_integer(make_fixed_view(DecodedValueKind::INT32, values), 8, true);
    auto result = read_column(std::make_shared<DataTypeInt8>(), view);
    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& column = assert_cast<const ColumnInt8&>(*result.column);
    ASSERT_EQ(values.size(), column.size());
    EXPECT_EQ(static_cast<Int8>(127), column.get_element(0));
    EXPECT_EQ(static_cast<Int8>(-128), column.get_element(1));
    EXPECT_EQ(static_cast<Int8>(-1), column.get_element(2));
    EXPECT_EQ(static_cast<Int8>(-1), column.get_element(3));
}

TEST(DataTypeSerDeDecodedValuesTest, ReadFloatAndDouble) {
    {
        auto type = std::make_shared<DataTypeFloat32>();
        std::vector<float> values = {0.0F, -0.0F, 1.5F, -2.25F};
        auto result = read_column(type, make_fixed_view(DecodedValueKind::FLOAT, values));
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnFloat32&>(*result.column);
        EXPECT_FLOAT_EQ(0.0F, column.get_element(0));
        EXPECT_TRUE(std::signbit(column.get_element(1)));
        EXPECT_FLOAT_EQ(1.5F, column.get_element(2));
        EXPECT_FLOAT_EQ(-2.25F, column.get_element(3));
    }
    {
        auto type = std::make_shared<DataTypeFloat64>();
        std::vector<double> values = {0.0, -0.0, 1.5, -2.25};
        auto result = read_column(type, make_fixed_view(DecodedValueKind::DOUBLE, values));
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnFloat64&>(*result.column);
        EXPECT_DOUBLE_EQ(0.0, column.get_element(0));
        EXPECT_TRUE(std::signbit(column.get_element(1)));
        EXPECT_DOUBLE_EQ(1.5, column.get_element(2));
        EXPECT_DOUBLE_EQ(-2.25, column.get_element(3));
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadFloatSpecialValues) {
    {
        std::vector<float> values = {std::numeric_limits<float>::quiet_NaN(),
                                     std::numeric_limits<float>::infinity(),
                                     -std::numeric_limits<float>::infinity()};
        auto result = read_column(std::make_shared<DataTypeFloat32>(),
                                  make_fixed_view(DecodedValueKind::FLOAT, values));
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnFloat32&>(*result.column);
        EXPECT_TRUE(std::isnan(column.get_element(0)));
        EXPECT_TRUE(std::isinf(column.get_element(1)));
        EXPECT_FALSE(std::signbit(column.get_element(1)));
        EXPECT_TRUE(std::isinf(column.get_element(2)));
        EXPECT_TRUE(std::signbit(column.get_element(2)));
    }
    {
        std::vector<double> values = {std::numeric_limits<double>::quiet_NaN(),
                                      std::numeric_limits<double>::infinity(),
                                      -std::numeric_limits<double>::infinity()};
        auto result = read_column(std::make_shared<DataTypeFloat64>(),
                                  make_fixed_view(DecodedValueKind::DOUBLE, values));
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnFloat64&>(*result.column);
        EXPECT_TRUE(std::isnan(column.get_element(0)));
        EXPECT_TRUE(std::isinf(column.get_element(1)));
        EXPECT_FALSE(std::signbit(column.get_element(1)));
        EXPECT_TRUE(std::isinf(column.get_element(2)));
        EXPECT_TRUE(std::signbit(column.get_element(2)));
    }
}

// ----------------------------------------------------------------------
// Number SerDe error paths
// ----------------------------------------------------------------------
// These cases separate unsupported physical kinds from corrupt decoded buffers. Unsupported kinds
// must not append to the destination column; missing value buffers are allowed only for empty or
// all-null batches where no non-null row can dereference the buffer.

TEST(DataTypeSerDeDecodedValuesTest, NumberRejectsMismatchedKind) {
    struct Case {
        DataTypePtr type;
        DecodedValueKind kind;
    };
    std::vector<Case> cases = {
            {std::make_shared<DataTypeBool>(), DecodedValueKind::INT32},
            {std::make_shared<DataTypeInt32>(), DecodedValueKind::BOOL},
            {std::make_shared<DataTypeFloat32>(), DecodedValueKind::DOUBLE},
            {std::make_shared<DataTypeFloat64>(), DecodedValueKind::FLOAT},
            {std::make_shared<DataTypeInt32>(), DecodedValueKind::BINARY},
    };

    for (const auto& test_case : cases) {
        std::vector<int32_t> values = {1};
        auto result = read_column(test_case.type, make_fixed_view(test_case.kind, values));
        expect_not_supported(result.status);
        EXPECT_EQ(0, result.column->size());
    }
}

TEST(DataTypeSerDeDecodedValuesTest, NumberRejectsMissingValuesWhenNonNullExists) {
    auto type = std::make_shared<DataTypeInt32>();
    {
        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT32;
        view.row_count = 3;
        auto result = read_column(type, view);
        expect_corruption(result.status);
    }
    {
        std::vector<uint8_t> null_map = {1, 0, 1};
        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT32;
        view.row_count = 3;
        view.null_map = null_map.data();
        auto result = read_column(type, view);
        expect_corruption(result.status);
    }
}

TEST(DataTypeSerDeDecodedValuesTest, NumberAllowsMissingValuesForAllNullOrEmpty) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    {
        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT32;
        view.row_count = 0;
        auto result = read_column(type, view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        EXPECT_EQ(0, result.column->size());
    }
    {
        std::vector<uint8_t> null_map = {1, 1, 1};
        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT32;
        view.row_count = 3;
        view.null_map = null_map.data();
        auto result = read_column(type, view);
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
        const auto& nested_column =
                assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        ASSERT_EQ(3, nullable_column.size());
        for (size_t row = 0; row < nullable_column.size(); ++row) {
            EXPECT_TRUE(nullable_column.is_null_at(row));
            EXPECT_EQ(0, nested_column.get_element(row));
        }
    }
}

TEST(DataTypeSerDeDecodedValuesTest, NumberRejectsOutOfRangeValueInStrictMode) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>());
    std::vector<int64_t> values = {127, 128};
    std::vector<uint8_t> null_map = {0, 0};
    auto view = make_fixed_view(DecodedValueKind::INT64, values, &null_map);
    view.enable_strict_mode = true;

    auto result = read_column(type, view);

    expect_data_quality_error(result.status);
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    EXPECT_EQ(0, nullable_column.size());
    EXPECT_EQ(0, nullable_column.get_null_map_data().size());
    EXPECT_EQ(0, nullable_column.get_nested_column().size());
}

TEST(DataTypeSerDeDecodedValuesTest, NumberNullsOutOfRangeValueInNonStrictMode) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>());
    std::vector<int64_t> values = {127, 128, -129, -128};
    std::vector<uint8_t> null_map = {0, 0, 0, 0};
    auto view = make_fixed_view(DecodedValueKind::INT64, values, &null_map);

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    const auto& nested_column = assert_cast<const ColumnInt8&>(nullable_column.get_nested_column());
    ASSERT_EQ(4, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_TRUE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));
    EXPECT_EQ(127, nested_column.get_element(0));
    EXPECT_EQ(0, nested_column.get_element(1));
    EXPECT_EQ(0, nested_column.get_element(2));
    EXPECT_EQ(-128, nested_column.get_element(3));
}

TEST(DataTypeSerDeDecodedValuesTest, NumberRejectsUnsignedOverflowInStrictMode) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    std::vector<uint64_t> values = {static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
                                    std::numeric_limits<uint64_t>::max()};
    std::vector<uint8_t> null_map = {0, 0};
    auto view = make_fixed_view(DecodedValueKind::UINT64, values, &null_map);
    view.enable_strict_mode = true;

    auto result = read_column(type, view);

    expect_data_quality_error(result.status);
}

TEST(DataTypeSerDeDecodedValuesTest, NumberNullsUnsignedOverflowInNonStrictMode) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    std::vector<uint64_t> values = {static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
                                    std::numeric_limits<uint64_t>::max()};
    std::vector<uint8_t> null_map = {0, 0};
    auto view = make_fixed_view(DecodedValueKind::UINT64, values, &null_map);

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    const auto& nested_column =
            assert_cast<const ColumnInt64&>(nullable_column.get_nested_column());
    ASSERT_EQ(2, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), nested_column.get_element(0));
    EXPECT_EQ(0, nested_column.get_element(1));
}

// ----------------------------------------------------------------------
// String / Binary SerDe
// ----------------------------------------------------------------------
// String-like decoded reads must preserve exact byte sequences. The embedded-NUL case prevents
// accidental C-string truncation. Nullable string tests ensure null rows materialize default nested
// values while the outer null map remains authoritative.

TEST(DataTypeSerDeDecodedValuesTest, ReadStringFromBinary) {
    auto type = std::make_shared<DataTypeString>();
    std::vector<std::string> storage = {"alpha", "", std::string("a\0b", 3), "utf8-\xe4\xb8\xad"};
    auto refs = string_refs(storage);

    auto result = read_column(type, make_binary_view(DecodedValueKind::BINARY, refs));

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_binary_column(*result.column, storage);
}

TEST(DataTypeSerDeDecodedValuesTest, ReadStringFromFixedBinary) {
    auto type = std::make_shared<DataTypeString>();
    std::vector<std::string> storage = {std::string("\x00\x01\x02\x03", 4),
                                        std::string("\x7f\x80\xfe\xff", 4)};
    auto refs = string_refs(storage);

    auto result = read_column(type, make_binary_view(DecodedValueKind::FIXED_BINARY, refs, 4));

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_binary_column(*result.column, storage);
}

TEST(DataTypeSerDeDecodedValuesTest, StringNullMapMaterialization) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    std::vector<std::string> storage = {"alpha", "", "omega"};
    auto refs = string_refs(storage);
    std::vector<uint8_t> null_map = {0, 1, 0};

    auto result =
            read_column(type, make_binary_view(DecodedValueKind::BINARY, refs, -1, &null_map));

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    ASSERT_EQ(3, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    expect_binary_column(nullable_column.get_nested_column(), {"alpha", "", "omega"});
}

TEST(DataTypeSerDeDecodedValuesTest, StringRejectsMismatchedKind) {
    auto type = std::make_shared<DataTypeString>();
    for (auto kind : {DecodedValueKind::INT32, DecodedValueKind::INT64, DecodedValueKind::DOUBLE}) {
        std::vector<int64_t> values = {1};
        auto result = read_column(type, make_fixed_view(kind, values));
        expect_not_supported(result.status);
        EXPECT_EQ(0, result.column->size());
    }
}

TEST(DataTypeSerDeDecodedValuesTest, StringRejectsMissingBinaryValuesWhenNonNullExists) {
    auto type = std::make_shared<DataTypeString>();
    DecodedColumnView view;
    view.value_kind = DecodedValueKind::BINARY;
    view.row_count = 1;

    auto result = read_column(type, view);

    expect_corruption(result.status);
}

TEST(DataTypeSerDeDecodedValuesTest, StringAllowsAllNullWithoutBinaryValues) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    std::vector<uint8_t> null_map = {1, 1};
    DecodedColumnView view;
    view.value_kind = DecodedValueKind::BINARY;
    view.row_count = 2;
    view.null_map = null_map.data();

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    ASSERT_EQ(2, nullable_column.size());
    EXPECT_TRUE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    expect_binary_column(nullable_column.get_nested_column(), {"", ""});
}

// ----------------------------------------------------------------------
// DateV2 SerDe
// ----------------------------------------------------------------------
// DateV2 accepts Parquet DATE-style epoch days as INT32. Null rows insert default nested dates and
// missing buffers are rejected only when a non-null row requires a value.

TEST(DataTypeSerDeDecodedValuesTest, ReadDateV2FromEpochDays) {
    auto type = std::make_shared<DataTypeDateV2>();
    std::vector<int32_t> values = {-1, 0, 1, 18628, 18321};

    auto result = read_column(type, make_fixed_view(DecodedValueKind::INT32, values));

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(*type, *result.column,
                          {"1969-12-31", "1970-01-01", "1970-01-02", "2021-01-01", "2020-02-29"});
}

TEST(DataTypeSerDeDecodedValuesTest, DateV2HandlesNulls) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateV2>());
    std::vector<int32_t> values = {0, 1, 2};
    std::vector<uint8_t> null_map = {0, 1, 0};

    auto result = read_column(type, make_fixed_view(DecodedValueKind::INT32, values, &null_map));

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    ASSERT_EQ(3, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    expect_column_strings(*type, *result.column, {"1970-01-01", "NULL", "1970-01-03"});
}

TEST(DataTypeSerDeDecodedValuesTest, DateV2RejectsInvalidKind) {
    auto type = std::make_shared<DataTypeDateV2>();
    for (auto kind :
         {DecodedValueKind::INT64, DecodedValueKind::BINARY, DecodedValueKind::DOUBLE}) {
        std::vector<int64_t> values = {0};
        auto result = read_column(type, make_fixed_view(kind, values));
        expect_not_supported(result.status);
        EXPECT_EQ(0, result.column->size());
    }
}

TEST(DataTypeSerDeDecodedValuesTest, DateV2RejectsMissingValuesWhenNonNullExists) {
    auto type = std::make_shared<DataTypeDateV2>();
    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT32;
    view.row_count = 1;

    auto result = read_column(type, view);

    expect_corruption(result.status);
}

// ----------------------------------------------------------------------
// DateTimeV2 SerDe
// ----------------------------------------------------------------------
// Timestamp decoding covers INT64 micros/millis, UNKNOWN-as-micros compatibility, UTC-adjusted
// conversion with explicit/default timezones, INT96 Julian-day timestamps, and invalid buffer/kind
// errors. Negative epoch values are included to lock correct floor-division behavior.

TEST(DataTypeSerDeDecodedValuesTest, ReadDateTimeV2Micros) {
    auto type = std::make_shared<DataTypeDateTimeV2>(6);
    std::vector<int64_t> values = {-1, 0, 1, 1234567, 86400000000LL - 1};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);
    view.time_unit = DecodedTimeUnit::MICROS;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(*type, *result.column,
                          {"1969-12-31 23:59:59.999999", "1970-01-01 00:00:00.000000",
                           "1970-01-01 00:00:00.000001", "1970-01-01 00:00:01.234567",
                           "1970-01-01 23:59:59.999999"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDateTimeV2Millis) {
    auto type = std::make_shared<DataTypeDateTimeV2>(6);
    std::vector<int64_t> values = {-1, 0, 1, 1234};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);
    view.time_unit = DecodedTimeUnit::MILLIS;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(*type, *result.column,
                          {"1969-12-31 23:59:59.999000", "1970-01-01 00:00:00.000000",
                           "1970-01-01 00:00:00.001000", "1970-01-01 00:00:01.234000"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDateTimeV2Nanos) {
    auto type = std::make_shared<DataTypeDateTimeV2>(6);
    std::vector<int64_t> values = {-1000, 0, 1000, 1234567890};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);
    view.time_unit = DecodedTimeUnit::NANOS;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(*type, *result.column,
                          {"1969-12-31 23:59:59.999999", "1970-01-01 00:00:00.000000",
                           "1970-01-01 00:00:00.000001", "1970-01-01 00:00:01.234567"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDateTimeV2UnknownUnitAsMicros) {
    auto type = std::make_shared<DataTypeDateTimeV2>(6);
    std::vector<int64_t> values = {1000000};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);
    view.time_unit = DecodedTimeUnit::UNKNOWN;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(*type, *result.column, {"1970-01-01 00:00:01.000000"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDateTimeV2UtcAdjustedDefaultUtc) {
    auto type = std::make_shared<DataTypeDateTimeV2>(6);
    std::vector<int64_t> values = {0};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);
    view.time_unit = DecodedTimeUnit::MICROS;
    view.timestamp_is_adjusted_to_utc = true;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(*type, *result.column, {"1970-01-01 00:00:00.000000"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDateTimeV2UtcAdjustedWithTimezones) {
    auto type = std::make_shared<DataTypeDateTimeV2>(6);
    std::vector<int64_t> values = {0, -1, 1234567};
    cctz::time_zone shanghai;
    cctz::time_zone new_york;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("+08:00", shanghai));
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("-05:00", new_york));

    auto shanghai_view = make_fixed_view(DecodedValueKind::INT64, values);
    shanghai_view.time_unit = DecodedTimeUnit::MICROS;
    shanghai_view.timestamp_is_adjusted_to_utc = true;
    shanghai_view.timezone = &shanghai;
    auto shanghai_result = read_column(type, shanghai_view);
    ASSERT_TRUE(shanghai_result.status.ok()) << shanghai_result.status;
    expect_column_strings(*type, *shanghai_result.column,
                          {"1970-01-01 08:00:00.000000", "1970-01-01 07:59:59.999999",
                           "1970-01-01 08:00:01.234567"});

    auto new_york_view = make_fixed_view(DecodedValueKind::INT64, values);
    new_york_view.time_unit = DecodedTimeUnit::MICROS;
    new_york_view.timestamp_is_adjusted_to_utc = true;
    new_york_view.timezone = &new_york;
    auto new_york_result = read_column(type, new_york_view);
    ASSERT_TRUE(new_york_result.status.ok()) << new_york_result.status;
    expect_column_strings(*type, *new_york_result.column,
                          {"1969-12-31 19:00:00.000000", "1969-12-31 18:59:59.999999",
                           "1969-12-31 19:00:01.234567"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDateTimeV2Int96) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6));
    std::vector<TestInt96Timestamp> values = {
            {0, 2440588},
            {86399999999000LL, 2440587},
            {0, 2440589},
    };
    std::vector<uint8_t> null_map = {0, 0, 1};
    auto view = make_fixed_view(DecodedValueKind::INT96, values, &null_map);
    cctz::time_zone shanghai;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("+08:00", shanghai));
    view.timezone = &shanghai;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(*type, *result.column,
                          {"1970-01-01 08:00:00.000000", "1970-01-01 07:59:59.999999", "NULL"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadTimestampTzInt64AsUtcInstant) {
    auto type = std::make_shared<DataTypeTimeStampTz>(6);
    // 2024-12-31 16:00:00 UTC is displayed as 2025-01-01 00:00:00+08:00.
    cctz::time_zone shanghai;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("+08:00", shanghai));

    std::vector<int64_t> micros_values = {1735660800000000LL, 1735660800123456LL};
    auto micros_view = make_fixed_view(DecodedValueKind::INT64, micros_values);
    micros_view.time_unit = DecodedTimeUnit::MICROS;
    auto micros_result = read_column(type, micros_view);
    ASSERT_TRUE(micros_result.status.ok()) << micros_result.status;
    const auto& micros_column = assert_cast<const ColumnTimeStampTz&>(*micros_result.column);
    EXPECT_EQ(micros_column.get_element(0).to_string(shanghai, 6),
              "2025-01-01 00:00:00.000000+08:00");
    EXPECT_EQ(micros_column.get_element(1).to_string(shanghai, 6),
              "2025-01-01 00:00:00.123456+08:00");

    std::vector<int64_t> millis_values = {1735660800000LL};
    auto millis_view = make_fixed_view(DecodedValueKind::INT64, millis_values);
    millis_view.time_unit = DecodedTimeUnit::MILLIS;
    auto millis_result = read_column(type, millis_view);
    ASSERT_TRUE(millis_result.status.ok()) << millis_result.status;
    const auto& millis_column = assert_cast<const ColumnTimeStampTz&>(*millis_result.column);
    EXPECT_EQ(millis_column.get_element(0).to_string(shanghai, 6),
              "2025-01-01 00:00:00.000000+08:00");

    std::vector<int64_t> nanos_values = {1735660800123456000LL};
    auto nanos_view = make_fixed_view(DecodedValueKind::INT64, nanos_values);
    nanos_view.time_unit = DecodedTimeUnit::NANOS;
    auto nanos_result = read_column(type, nanos_view);
    ASSERT_TRUE(nanos_result.status.ok()) << nanos_result.status;
    const auto& nanos_column = assert_cast<const ColumnTimeStampTz&>(*nanos_result.column);
    EXPECT_EQ(nanos_column.get_element(0).to_string(shanghai, 6),
              "2025-01-01 00:00:00.123456+08:00");
}

TEST(DataTypeSerDeDecodedValuesTest, TimestampTzReadsInt96AsUtcInstant) {
    auto type = std::make_shared<DataTypeTimeStampTz>(6);
    std::vector<TestInt96Timestamp> values = {{0, 2440588}, {123456789000LL, 2440588}};
    auto view = make_fixed_view(DecodedValueKind::INT96, values);
    cctz::time_zone shanghai;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("+08:00", shanghai));

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& column = assert_cast<const ColumnTimeStampTz&>(*result.column);
    EXPECT_EQ(column.get_element(0).to_string(shanghai, 6), "1970-01-01 08:00:00.000000+08:00");
    EXPECT_EQ(column.get_element(1).to_string(shanghai, 6), "1970-01-01 08:02:03.456789+08:00");
}

TEST(DataTypeSerDeDecodedValuesTest, DateTimeV2RejectsInvalidKind) {
    auto type = std::make_shared<DataTypeDateTimeV2>(6);
    for (auto kind :
         {DecodedValueKind::INT32, DecodedValueKind::BINARY, DecodedValueKind::DOUBLE}) {
        std::vector<int64_t> values = {0};
        auto result = read_column(type, make_fixed_view(kind, values));
        expect_not_supported(result.status);
        EXPECT_EQ(0, result.column->size());
    }
}

TEST(DataTypeSerDeDecodedValuesTest, DateTimeV2RejectsMissingValuesWhenNonNullExists) {
    auto type = std::make_shared<DataTypeDateTimeV2>(6);
    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT64;
    view.row_count = 1;

    auto result = read_column(type, view);

    expect_corruption(result.status);
}

TEST(DataTypeSerDeDecodedValuesTest, DateTimeV2RejectsOutOfRangeEpochWithoutAbort) {
    auto type = std::make_shared<DataTypeDateTimeV2>(6);
    std::vector<int64_t> values = {0, -377673580800000001LL};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);
    view.time_unit = DecodedTimeUnit::MICROS;

    auto result = read_column(type, view);

    expect_data_quality_error(result.status);
    EXPECT_EQ(0, result.column->size());
}

TEST(DataTypeSerDeDecodedValuesTest, NullableDateTimeV2RejectsOutOfRangeEpochInStrictMode) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6));
    std::vector<int64_t> values = {0, -377673580800000001LL};
    std::vector<uint8_t> null_map = {0, 0};
    auto view = make_fixed_view(DecodedValueKind::INT64, values, &null_map);
    view.time_unit = DecodedTimeUnit::MICROS;
    view.enable_strict_mode = true;

    auto result = read_column(type, view);

    expect_data_quality_error(result.status);
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    EXPECT_EQ(0, nullable_column.size());
    EXPECT_EQ(0, nullable_column.get_null_map_data().size());
    EXPECT_EQ(0, nullable_column.get_nested_column().size());
}

TEST(DataTypeSerDeDecodedValuesTest, NullableDateTimeV2NullsOutOfRangeEpochInNonStrictMode) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6));
    std::vector<int64_t> values = {0, -377673580800000001LL, 1};
    std::vector<uint8_t> null_map = {0, 0, 0};
    auto view = make_fixed_view(DecodedValueKind::INT64, values, &null_map);
    view.time_unit = DecodedTimeUnit::MICROS;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    ASSERT_EQ(3, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    expect_column_strings(*type, *result.column,
                          {"1970-01-01 00:00:00.000000", "NULL", "1970-01-01 00:00:00.000001"});
}

// ----------------------------------------------------------------------
// TimeV2 SerDe
// ----------------------------------------------------------------------
// TimeV2 decodes INT32 as milliseconds and INT64 according to the supplied time unit. Negative
// durations are verified because they use a sign bit in TimeValue::TimeType rather than DateTimeV2
// epoch arithmetic.

TEST(DataTypeSerDeDecodedValuesTest, ReadTimeV2FromInt32Millis) {
    auto type = std::make_shared<DataTypeTimeV2>(6);
    std::vector<int32_t> values = {0, 1, -1, 3661001};

    auto result = read_column(type, make_fixed_view(DecodedValueKind::INT32, values));

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(
            *type, *result.column,
            {"00:00:00.000000", "00:00:00.001000", "-00:00:00.001000", "01:01:01.001000"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadTimeV2FromInt64Micros) {
    auto type = std::make_shared<DataTypeTimeV2>(6);
    std::vector<int64_t> values = {0, 1, -1, 3661000001LL};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);
    view.time_unit = DecodedTimeUnit::MICROS;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(
            *type, *result.column,
            {"00:00:00.000000", "00:00:00.000001", "-00:00:00.000001", "01:01:01.000001"});

    view.time_unit = DecodedTimeUnit::UNKNOWN;
    auto unknown_result = read_column(type, view);
    ASSERT_TRUE(unknown_result.status.ok()) << unknown_result.status;
    expect_column_strings(
            *type, *unknown_result.column,
            {"00:00:00.000000", "00:00:00.000001", "-00:00:00.000001", "01:01:01.000001"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadTimeV2FromInt64Millis) {
    auto type = std::make_shared<DataTypeTimeV2>(6);
    std::vector<int64_t> values = {1, -1, 3661001};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);
    view.time_unit = DecodedTimeUnit::MILLIS;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(*type, *result.column,
                          {"00:00:00.001000", "-00:00:00.001000", "01:01:01.001000"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadTimeV2FromInt64Nanos) {
    auto type = std::make_shared<DataTypeTimeV2>(6);
    std::vector<int64_t> values = {1000, -1000, 3661000001000LL};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);
    view.time_unit = DecodedTimeUnit::NANOS;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(*type, *result.column,
                          {"00:00:00.000001", "-00:00:00.000001", "01:01:01.000001"});
}

TEST(DataTypeSerDeDecodedValuesTest, TimeV2HandlesNulls) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeTimeV2>(6));
    std::vector<int64_t> values = {0, 1, 2};
    std::vector<uint8_t> null_map = {0, 1, 0};
    auto view = make_fixed_view(DecodedValueKind::INT64, values, &null_map);
    view.time_unit = DecodedTimeUnit::MICROS;

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    ASSERT_EQ(3, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    expect_column_strings(*type, *result.column, {"00:00:00.000000", "NULL", "00:00:00.000002"});
}

TEST(DataTypeSerDeDecodedValuesTest, TimeV2RejectsInvalidKind) {
    auto type = std::make_shared<DataTypeTimeV2>(6);
    for (auto kind : {DecodedValueKind::BOOL, DecodedValueKind::BINARY, DecodedValueKind::DOUBLE}) {
        std::vector<int64_t> values = {0};
        auto result = read_column(type, make_fixed_view(kind, values));
        expect_not_supported(result.status);
        EXPECT_EQ(0, result.column->size());
    }
}

// ----------------------------------------------------------------------
// Decimal SerDe
// ----------------------------------------------------------------------
// Decimal cases cover integer-backed values and Parquet big-endian two's-complement binary values.
// String assertions validate the user-visible scale, while direct column checks lock the native
// unscaled value for every decimal width.

TEST(DataTypeSerDeDecodedValuesTest, ReadDecimal32FromInt32) {
    auto type = std::make_shared<DataTypeDecimal32>(9, 2);
    std::vector<int32_t> values = {12345, -67, 0};
    auto view = make_fixed_view(DecodedValueKind::INT32, values);

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& column = assert_cast<const ColumnDecimal32&>(*result.column);
    EXPECT_EQ(Decimal32(12345), column.get_element(0));
    EXPECT_EQ(Decimal32(-67), column.get_element(1));
    EXPECT_EQ(Decimal32(0), column.get_element(2));
    expect_column_strings(*type, *result.column, {"123.45", "-0.67", "0.00"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDecimal64FromInt64) {
    auto type = std::make_shared<DataTypeDecimal64>(18, 4);
    std::vector<int64_t> values = {123456789, -1};
    auto view = make_fixed_view(DecodedValueKind::INT64, values);

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& column = assert_cast<const ColumnDecimal64&>(*result.column);
    EXPECT_EQ(Decimal64(123456789), column.get_element(0));
    EXPECT_EQ(Decimal64(-1), column.get_element(1));
    expect_column_strings(*type, *result.column, {"12345.6789", "-0.0001"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDecimal128FromInt32AndInt64) {
    auto type = std::make_shared<DataTypeDecimal128>(38, 6);
    {
        std::vector<int32_t> values = {123456, -1};
        auto result = read_column(type, make_fixed_view(DecodedValueKind::INT32, values));
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnDecimal128V3&>(*result.column);
        EXPECT_EQ(decimal128_v3(123456), column.get_element(0));
        EXPECT_EQ(decimal128_v3(-1), column.get_element(1));
        expect_column_strings(*type, *result.column, {"0.123456", "-0.000001"});
    }
    {
        std::vector<int64_t> values = {1234567890123LL, -1234567LL};
        auto result = read_column(type, make_fixed_view(DecodedValueKind::INT64, values));
        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& column = assert_cast<const ColumnDecimal128V3&>(*result.column);
        EXPECT_EQ(decimal128_v3(1234567890123LL), column.get_element(0));
        EXPECT_EQ(decimal128_v3(-1234567LL), column.get_element(1));
        expect_column_strings(*type, *result.column, {"1234567.890123", "-1.234567"});
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDecimal256FromInt64) {
    auto type = std::make_shared<DataTypeDecimal256>(76, 8);
    std::vector<int64_t> values = {std::numeric_limits<int64_t>::max(),
                                   std::numeric_limits<int64_t>::min()};
    auto result = read_column(type, make_fixed_view(DecodedValueKind::INT64, values));

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& column = assert_cast<const ColumnDecimal256&>(*result.column);
    EXPECT_EQ(decimal256_from_int64(std::numeric_limits<int64_t>::max()), column.get_element(0));
    EXPECT_EQ(decimal256_from_int64(std::numeric_limits<int64_t>::min()), column.get_element(1));
    expect_column_strings(*type, *result.column, {"92233720368.54775807", "-92233720368.54775808"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDecimalFromBinaryBigEndian) {
    auto type = std::make_shared<DataTypeDecimal128>(18, 2);
    std::vector<std::string> storage = {
            std::string("\x00", 1), std::string("\x7f", 1),     std::string("\x80", 1),
            std::string("\xff", 1), std::string("\xff\xbd", 2), std::string("\x30\x39", 2),
    };
    auto refs = string_refs(storage);

    auto result = read_column(type, make_binary_view(DecodedValueKind::BINARY, refs));

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& column = assert_cast<const ColumnDecimal128V3&>(*result.column);
    std::vector<Decimal128V3> expected = {decimal128_v3(0),    decimal128_v3(127),
                                          decimal128_v3(-128), decimal128_v3(-1),
                                          decimal128_v3(-67),  decimal128_v3(12345)};
    ASSERT_EQ(expected.size(), column.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(expected[row], column.get_element(row)) << "row=" << row;
    }
    expect_column_strings(*type, *result.column,
                          {"0.00", "1.27", "-1.28", "-0.01", "-0.67", "123.45"});
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDecimalFromFixedBinaryLengths) {
    {
        auto type = std::make_shared<DataTypeDecimal128>(38, 2);
        std::vector<std::string> storage = {std::string("\x00", 1), std::string("\x80", 1)};
        auto refs = string_refs(storage);
        auto result = read_column(type, make_binary_view(DecodedValueKind::FIXED_BINARY, refs, 1));
        ASSERT_TRUE(result.status.ok()) << result.status;
        expect_column_strings(*type, *result.column, {"0.00", "-1.28"});
    }
    {
        auto type = std::make_shared<DataTypeDecimal128>(38, 2);
        std::vector<std::string> storage = {std::string("\xff\xbd", 2), std::string("\x30\x39", 2)};
        auto refs = string_refs(storage);
        auto result = read_column(type, make_binary_view(DecodedValueKind::FIXED_BINARY, refs, 2));
        ASSERT_TRUE(result.status.ok()) << result.status;
        expect_column_strings(*type, *result.column, {"-0.67", "123.45"});
    }
    {
        auto type = std::make_shared<DataTypeDecimal128>(38, 2);
        std::vector<std::string> storage = {std::string("\0\0\0\0\0\0\x30\x39", 8)};
        auto refs = string_refs(storage);
        auto result = read_column(type, make_binary_view(DecodedValueKind::FIXED_BINARY, refs, 8));
        ASSERT_TRUE(result.status.ok()) << result.status;
        expect_column_strings(*type, *result.column, {"123.45"});
    }
    {
        auto type = std::make_shared<DataTypeDecimal128>(38, 2);
        std::vector<std::string> storage = {
                std::string("\xff\xff\xff\xff\xff\xff\xff\xff"
                            "\xff\xff\xff\xff\xff\xff\xff\xbd",
                            16)};
        auto refs = string_refs(storage);
        auto result = read_column(type, make_binary_view(DecodedValueKind::FIXED_BINARY, refs, 16));
        ASSERT_TRUE(result.status.ok()) << result.status;
        expect_column_strings(*type, *result.column, {"-0.67"});
    }
    {
        auto type = std::make_shared<DataTypeDecimal256>(76, 2);
        std::vector<std::string> storage = {std::string(31, '\xff') + std::string("\xbd", 1)};
        auto refs = string_refs(storage);
        auto result = read_column(type, make_binary_view(DecodedValueKind::FIXED_BINARY, refs, 32));
        ASSERT_TRUE(result.status.ok()) << result.status;
        expect_column_strings(*type, *result.column, {"-0.67"});
    }
}

TEST(DataTypeSerDeDecodedValuesTest, DecimalHandlesNulls) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDecimal128>(18, 2));
    std::vector<int64_t> values = {12345, -1, -67};
    std::vector<uint8_t> null_map = {0, 1, 0};

    auto result = read_column(type, make_fixed_view(DecodedValueKind::INT64, values, &null_map));

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    const auto& decimal_column =
            assert_cast<const ColumnDecimal128V3&>(nullable_column.get_nested_column());
    ASSERT_EQ(3, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_EQ(decimal128_v3(12345), decimal_column.get_element(0));
    EXPECT_EQ(decimal128_v3(0), decimal_column.get_element(1));
    EXPECT_EQ(decimal128_v3(-67), decimal_column.get_element(2));
}

TEST(DataTypeSerDeDecodedValuesTest, DecimalRejectsOutOfRangeValueInStrictMode) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDecimal32>(9, 2));
    std::vector<int64_t> values = {999999999, 1000000000};
    std::vector<uint8_t> null_map = {0, 0};
    auto view = make_fixed_view(DecodedValueKind::INT64, values, &null_map);
    view.enable_strict_mode = true;

    auto result = read_column(type, view);

    expect_data_quality_error(result.status);
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    EXPECT_EQ(0, nullable_column.size());
    EXPECT_EQ(0, nullable_column.get_null_map_data().size());
    EXPECT_EQ(0, nullable_column.get_nested_column().size());
}

TEST(DataTypeSerDeDecodedValuesTest, DecimalNullsOutOfRangeValueInNonStrictMode) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDecimal32>(9, 2));
    std::vector<int64_t> values = {999999999, 1000000000, -1000000000, -999999999};
    std::vector<uint8_t> null_map = {0, 0, 0, 0};
    auto view = make_fixed_view(DecodedValueKind::INT64, values, &null_map);

    auto result = read_column(type, view);

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    const auto& decimal_column =
            assert_cast<const ColumnDecimal32&>(nullable_column.get_nested_column());
    ASSERT_EQ(4, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_TRUE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));
    EXPECT_EQ(Decimal32(999999999), decimal_column.get_element(0));
    EXPECT_EQ(Decimal32(0), decimal_column.get_element(1));
    EXPECT_EQ(Decimal32(0), decimal_column.get_element(2));
    EXPECT_EQ(Decimal32(-999999999), decimal_column.get_element(3));
}

TEST(DataTypeSerDeDecodedValuesTest, DecimalRejectsNullBinaryDataWithPositiveLength) {
    auto type = std::make_shared<DataTypeDecimal128>(18, 2);
    std::vector<StringRef> refs = {StringRef(static_cast<const char*>(nullptr), 2)};

    auto result = read_column(type, make_binary_view(DecodedValueKind::BINARY, refs));

    expect_corruption(result.status);
    EXPECT_NE(std::string::npos, result.status.to_string().find("row 0"));
}

TEST(DataTypeSerDeDecodedValuesTest, DecimalAllowsZeroLengthBinaryAsZero) {
    auto type = std::make_shared<DataTypeDecimal128>(18, 2);
    std::vector<StringRef> refs = {StringRef(static_cast<const char*>(nullptr), 0),
                                   StringRef("", 0)};

    auto result = read_column(type, make_binary_view(DecodedValueKind::BINARY, refs));

    ASSERT_TRUE(result.status.ok()) << result.status;
    expect_column_strings(*type, *result.column, {"0.00", "0.00"});
}

TEST(DataTypeSerDeDecodedValuesTest, DecimalRejectsInvalidKind) {
    auto type = std::make_shared<DataTypeDecimal128>(18, 2);
    for (auto kind : {DecodedValueKind::BOOL, DecodedValueKind::FLOAT, DecodedValueKind::DOUBLE,
                      DecodedValueKind::UINT64}) {
        std::vector<int64_t> values = {0};
        auto result = read_column(type, make_fixed_view(kind, values));
        expect_not_supported(result.status);
        EXPECT_EQ(0, result.column->size());
    }
}

TEST(DataTypeSerDeDecodedValuesTest, DecimalRejectsMissingBufferWhenNonNullExists) {
    auto type = std::make_shared<DataTypeDecimal128>(18, 2);
    {
        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT64;
        view.row_count = 1;
        auto result = read_column(type, view);
        expect_corruption(result.status);
    }
    {
        DecodedColumnView view;
        view.value_kind = DecodedValueKind::BINARY;
        view.row_count = 1;
        auto result = read_column(type, view);
        expect_corruption(result.status);
    }
}

// ----------------------------------------------------------------------
// Nullable SerDe wrapper
// ----------------------------------------------------------------------
// Nullable tests focus on wrapper responsibilities: copying the outer null map, inserting default
// nested values for null rows, treating a missing null_map as all non-null, appending to existing
// columns, and rolling back outer state when the nested reader rejects the input.

TEST(DataTypeSerDeDecodedValuesTest, NullablePropagatesNullMapAndReadsNested) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    std::vector<int32_t> values = {10, 20, 30, 40};
    std::vector<uint8_t> null_map = {0, 1, 0, 1};

    auto result = read_column(type, make_fixed_view(DecodedValueKind::INT32, values, &null_map));

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    const auto& nested_column =
            assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
    ASSERT_EQ(4, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_TRUE(nullable_column.is_null_at(3));
    EXPECT_EQ(10, nested_column.get_element(0));
    EXPECT_EQ(0, nested_column.get_element(1));
    EXPECT_EQ(30, nested_column.get_element(2));
    EXPECT_EQ(0, nested_column.get_element(3));
}

TEST(DataTypeSerDeDecodedValuesTest, NullableWithoutNullMapReadsAllNonNull) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    std::vector<std::string> storage = {"alpha", "beta"};
    auto refs = string_refs(storage);

    auto result = read_column(type, make_binary_view(DecodedValueKind::BINARY, refs));

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    ASSERT_EQ(2, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    expect_binary_column(nullable_column.get_nested_column(), storage);
}

TEST(DataTypeSerDeDecodedValuesTest, NullableAllNullDoesNotRequireNestedBuffer) {
    std::vector<uint8_t> null_map = {1, 1};
    std::vector<DataTypePtr> types = {
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateV2>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDecimal128>(18, 2)),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
    };

    for (const auto& type : types) {
        DecodedColumnView view;
        view.value_kind = type->get_name().find("String") != std::string::npos
                                  ? DecodedValueKind::BINARY
                                  : DecodedValueKind::INT32;
        view.row_count = 2;
        view.null_map = null_map.data();
        auto result = read_column(type, view);
        ASSERT_TRUE(result.status.ok()) << result.status << ", type=" << type->get_name();
        const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
        ASSERT_EQ(2, nullable_column.size());
        EXPECT_TRUE(nullable_column.is_null_at(0));
        EXPECT_TRUE(nullable_column.is_null_at(1));
        EXPECT_EQ(2, nullable_column.get_nested_column().size());
    }
}

TEST(DataTypeSerDeDecodedValuesTest, NullableAppendToExistingColumn) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto column = type->create_column();

    std::vector<int32_t> first_values = {1, 2};
    auto first_status = type->get_serde()->read_column_from_decoded_values(
            *column, make_fixed_view(DecodedValueKind::INT32, first_values));
    ASSERT_TRUE(first_status.ok()) << first_status;

    std::vector<int32_t> second_values = {10, 20, 30};
    std::vector<uint8_t> second_null_map = {0, 1, 0};
    auto second_status = type->get_serde()->read_column_from_decoded_values(
            *column, make_fixed_view(DecodedValueKind::INT32, second_values, &second_null_map));
    ASSERT_TRUE(second_status.ok()) << second_status;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    const auto& nested_column =
            assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
    ASSERT_EQ(5, nullable_column.size());
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_TRUE(nullable_column.is_null_at(3));
    EXPECT_FALSE(nullable_column.is_null_at(4));
    EXPECT_EQ(1, nested_column.get_element(0));
    EXPECT_EQ(2, nested_column.get_element(1));
    EXPECT_EQ(10, nested_column.get_element(2));
    EXPECT_EQ(0, nested_column.get_element(3));
    EXPECT_EQ(30, nested_column.get_element(4));
}

TEST(DataTypeSerDeDecodedValuesTest, NullablePropagatesNestedError) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto column = type->create_column();
    std::vector<double> values = {1.0};
    std::vector<uint8_t> null_map = {0};
    auto view = make_fixed_view(DecodedValueKind::DOUBLE, values, &null_map);
    view.enable_strict_mode = true;

    auto status = type->get_serde()->read_column_from_decoded_values(*column, view);

    expect_not_supported(status);
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    EXPECT_EQ(0, nullable_column.size());
    EXPECT_EQ(0, nullable_column.get_null_map_data().size());
    EXPECT_EQ(0, nullable_column.get_nested_column().size());
}

TEST(DataTypeSerDeDecodedValuesTest, NullableNonStrictModeNullsUnsupportedDecodedKindForAllTypes) {
    struct Case {
        DataTypePtr type;
        DecodedValueKind kind;
    };
    std::vector<Case> cases = {
            {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeBool>()),
             DecodedValueKind::INT32},
            {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
             DecodedValueKind::DOUBLE},
            {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
             DecodedValueKind::FLOAT},
            {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
             DecodedValueKind::INT64},
            {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateV2>()),
             DecodedValueKind::INT64},
            {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6)),
             DecodedValueKind::DOUBLE},
            {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeTimeV2>(6)),
             DecodedValueKind::DOUBLE},
            {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDecimal128>(18, 2)),
             DecodedValueKind::DOUBLE},
    };

    std::vector<int64_t> values = {1, 2};
    for (const auto& test_case : cases) {
        auto view = make_fixed_view(test_case.kind, values);

        auto result = read_column(test_case.type, view);

        ASSERT_TRUE(result.status.ok()) << result.status << ", type=" << test_case.type->get_name();
        expect_nullable_all_null(*result.column, values.size());
    }
}

TEST(DataTypeSerDeDecodedValuesTest, NullableStrictModeRejectsUnsupportedDecodedKind) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    std::vector<double> values = {1.0};
    std::vector<uint8_t> null_map = {0};
    auto view = make_fixed_view(DecodedValueKind::DOUBLE, values, &null_map);
    view.enable_strict_mode = true;

    auto result = read_column(type, view);

    expect_not_supported(result.status);
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    EXPECT_EQ(0, nullable_column.size());
    EXPECT_EQ(0, nullable_column.get_null_map_data().size());
    EXPECT_EQ(0, nullable_column.get_nested_column().size());
}

TEST(DataTypeSerDeDecodedValuesTest, NullableNonStrictModeNullsRowLevelDecodedConversionFailure) {
    {
        auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        std::vector<StringRef> refs = {StringRef("ok", 2),
                                       StringRef(static_cast<const char*>(nullptr), 2),
                                       StringRef("", 0)};
        auto view = make_binary_view(DecodedValueKind::BINARY, refs);

        auto result = read_column(type, view);

        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
        ASSERT_EQ(3, nullable_column.size());
        EXPECT_FALSE(nullable_column.is_null_at(0));
        EXPECT_TRUE(nullable_column.is_null_at(1));
        EXPECT_FALSE(nullable_column.is_null_at(2));
        expect_binary_column(nullable_column.get_nested_column(), {"ok", "", ""});
    }
    {
        auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDecimal128>(18, 2));
        std::vector<StringRef> refs = {StringRef("\x30\x39", 2),
                                       StringRef(static_cast<const char*>(nullptr), 2)};
        auto view = make_binary_view(DecodedValueKind::BINARY, refs);

        auto result = read_column(type, view);

        ASSERT_TRUE(result.status.ok()) << result.status;
        const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
        ASSERT_EQ(2, nullable_column.size());
        EXPECT_FALSE(nullable_column.is_null_at(0));
        EXPECT_TRUE(nullable_column.is_null_at(1));
        expect_column_strings(*type, *result.column, {"123.45", "NULL"});
    }
}

TEST(DataTypeSerDeDecodedValuesTest, NullableStrictModeRejectsRowLevelDecodedConversionFailure) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    std::vector<StringRef> refs = {StringRef("ok", 2),
                                   StringRef(static_cast<const char*>(nullptr), 2)};
    auto view = make_binary_view(DecodedValueKind::BINARY, refs);
    view.enable_strict_mode = true;

    auto result = read_column(type, view);

    expect_corruption(result.status);
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    EXPECT_EQ(0, nullable_column.size());
    EXPECT_EQ(0, nullable_column.get_null_map_data().size());
    EXPECT_EQ(0, nullable_column.get_nested_column().size());
}

// ----------------------------------------------------------------------
// read_field_from_decoded_value
// ----------------------------------------------------------------------
// The field path is used by Parquet min/max and pruning code. It must be covered independently
// because it creates a one-row column, delegates to the batch reader, and extracts a Field value.

TEST(DataTypeSerDeDecodedValuesTest, ReadFieldPrimitiveValues) {
    {
        std::vector<uint8_t> values = {true};
        auto field = read_field(std::make_shared<DataTypeBool>(), make_bool_view(values));
        EXPECT_EQ(TYPE_BOOLEAN, field.get_type());
        EXPECT_TRUE(field.get<TYPE_BOOLEAN>());
    }
    {
        std::vector<int32_t> values = {-42};
        auto field = read_field(std::make_shared<DataTypeInt32>(),
                                make_fixed_view(DecodedValueKind::INT32, values));
        EXPECT_EQ(TYPE_INT, field.get_type());
        EXPECT_EQ(-42, field.get<TYPE_INT>());
    }
    {
        std::vector<int64_t> values = {1234567890123LL};
        auto field = read_field(std::make_shared<DataTypeInt64>(),
                                make_fixed_view(DecodedValueKind::INT64, values));
        EXPECT_EQ(TYPE_BIGINT, field.get_type());
        EXPECT_EQ(1234567890123LL, field.get<TYPE_BIGINT>());
    }
    {
        std::vector<int64_t> values = {-9};
        auto field = read_field(std::make_shared<DataTypeInt128>(),
                                make_fixed_view(DecodedValueKind::INT64, values));
        EXPECT_EQ(TYPE_LARGEINT, field.get_type());
        EXPECT_EQ(static_cast<__int128_t>(-9), field.get<TYPE_LARGEINT>());
    }
    {
        std::vector<float> values = {std::numeric_limits<float>::quiet_NaN()};
        auto field = read_field(std::make_shared<DataTypeFloat32>(),
                                make_fixed_view(DecodedValueKind::FLOAT, values));
        EXPECT_EQ(TYPE_FLOAT, field.get_type());
        EXPECT_TRUE(std::isnan(field.get<TYPE_FLOAT>()));
    }
    {
        std::vector<double> values = {std::numeric_limits<double>::infinity()};
        auto field = read_field(std::make_shared<DataTypeFloat64>(),
                                make_fixed_view(DecodedValueKind::DOUBLE, values));
        EXPECT_EQ(TYPE_DOUBLE, field.get_type());
        EXPECT_TRUE(std::isinf(field.get<TYPE_DOUBLE>()));
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadFieldLogicalIntegerCastsPhysicalValue) {
    {
        std::vector<int32_t> values = {32767};
        auto view =
                with_logical_integer(make_fixed_view(DecodedValueKind::INT32, values), 8, false);
        auto field = read_field(std::make_shared<DataTypeInt16>(), view);
        EXPECT_EQ(TYPE_SMALLINT, field.get_type());
        EXPECT_EQ(255, field.get<TYPE_SMALLINT>());
    }
    {
        std::vector<int32_t> values = {-1};
        auto view =
                with_logical_integer(make_fixed_view(DecodedValueKind::UINT32, values), 32, false);
        auto field = read_field(std::make_shared<DataTypeInt64>(), view);
        EXPECT_EQ(TYPE_BIGINT, field.get_type());
        EXPECT_EQ(4294967295LL, field.get<TYPE_BIGINT>());
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadFieldStringValues) {
    auto type = std::make_shared<DataTypeString>();
    std::vector<std::string> storage = {std::string("a\0b", 3)};
    auto refs = string_refs(storage);
    auto field = read_field(type, make_binary_view(DecodedValueKind::BINARY, refs));
    EXPECT_EQ(TYPE_STRING, field.get_type());
    EXPECT_EQ(std::string("a\0b", 3), field.get<TYPE_STRING>());

    std::vector<std::string> fixed_storage = {std::string("\x00\x01\x02\x03", 4)};
    auto fixed_refs = string_refs(fixed_storage);
    auto fixed_field =
            read_field(type, make_binary_view(DecodedValueKind::FIXED_BINARY, fixed_refs, 4));
    EXPECT_EQ(TYPE_STRING, fixed_field.get_type());
    EXPECT_EQ(std::string("\x00\x01\x02\x03", 4), fixed_field.get<TYPE_STRING>());
}

TEST(DataTypeSerDeDecodedValuesTest, ReadFieldDateTimeAndTimeValues) {
    {
        auto type = std::make_shared<DataTypeDateV2>();
        std::vector<int32_t> values = {18628};
        auto field = read_field(type, make_fixed_view(DecodedValueKind::INT32, values));
        EXPECT_EQ(TYPE_DATEV2, field.get_type());
        EXPECT_EQ("2021-01-01", field.to_debug_string(0));
    }
    {
        auto type = std::make_shared<DataTypeDateTimeV2>(6);
        std::vector<int64_t> values = {1234567};
        auto view = make_fixed_view(DecodedValueKind::INT64, values);
        view.time_unit = DecodedTimeUnit::MICROS;
        auto field = read_field(type, view);
        EXPECT_EQ(TYPE_DATETIMEV2, field.get_type());
        EXPECT_EQ("1970-01-01 00:00:01.234567", field.to_debug_string(6));
    }
    {
        auto type = std::make_shared<DataTypeDateTimeV2>(6);
        std::vector<int64_t> values = {1234};
        auto view = make_fixed_view(DecodedValueKind::INT64, values);
        view.time_unit = DecodedTimeUnit::MILLIS;
        auto field = read_field(type, view);
        EXPECT_EQ(TYPE_DATETIMEV2, field.get_type());
        EXPECT_EQ("1970-01-01 00:00:01.234000", field.to_debug_string(6));
    }
    {
        auto type = std::make_shared<DataTypeDateTimeV2>(6);
        std::vector<TestInt96Timestamp> values = {{0, 2440588}};
        auto field = read_field(type, make_fixed_view(DecodedValueKind::INT96, values));
        EXPECT_EQ(TYPE_DATETIMEV2, field.get_type());
        EXPECT_EQ("1970-01-01 00:00:00.000000", field.to_debug_string(6));
    }
    {
        auto type = std::make_shared<DataTypeTimeV2>(6);
        std::vector<int64_t> values = {3661000001LL};
        auto view = make_fixed_view(DecodedValueKind::INT64, values);
        view.time_unit = DecodedTimeUnit::MICROS;
        auto field = read_field(type, view);
        EXPECT_EQ(TYPE_TIMEV2, field.get_type());
        auto column = type->create_column();
        column->insert(field);
        expect_column_strings(*type, *column, {"01:01:01.000001"});
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadFieldDecimalValues) {
    {
        auto type = std::make_shared<DataTypeDecimal32>(9, 2);
        std::vector<int32_t> values = {12345};
        auto field = read_field(type, make_fixed_view(DecodedValueKind::INT32, values));
        EXPECT_EQ(TYPE_DECIMAL32, field.get_type());
        EXPECT_EQ("123.45", field.to_debug_string(2));
    }
    {
        auto type = std::make_shared<DataTypeDecimal64>(18, 4);
        std::vector<int64_t> values = {-1};
        auto field = read_field(type, make_fixed_view(DecodedValueKind::INT64, values));
        EXPECT_EQ(TYPE_DECIMAL64, field.get_type());
        EXPECT_EQ("-0.0001", field.to_debug_string(4));
    }
    {
        auto type = std::make_shared<DataTypeDecimal128>(38, 2);
        std::vector<std::string> storage = {std::string("\x30\x39", 2)};
        auto refs = string_refs(storage);
        auto field = read_field(type, make_binary_view(DecodedValueKind::BINARY, refs));
        EXPECT_EQ(TYPE_DECIMAL128I, field.get_type());
        EXPECT_EQ("123.45", field.to_debug_string(2));
    }
    {
        auto type = std::make_shared<DataTypeDecimal256>(76, 2);
        std::vector<std::string> storage = {std::string(31, '\xff') + std::string("\xbd", 1)};
        auto refs = string_refs(storage);
        auto field = read_field(type, make_binary_view(DecodedValueKind::FIXED_BINARY, refs, 32));
        EXPECT_EQ(TYPE_DECIMAL256, field.get_type());
        EXPECT_EQ("-0.67", field.to_debug_string(2));
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadFieldPropagatesUnsupportedKind) {
    {
        auto type = std::make_shared<DataTypeString>();
        std::vector<int32_t> values = {1};
        expect_not_supported(
                read_field_status(type, make_fixed_view(DecodedValueKind::INT32, values)));
    }
    {
        auto type = std::make_shared<DataTypeInt32>();
        std::vector<double> values = {1.0};
        expect_not_supported(
                read_field_status(type, make_fixed_view(DecodedValueKind::DOUBLE, values)));
    }
    {
        auto type = std::make_shared<DataTypeDateV2>();
        std::vector<int64_t> values = {0};
        expect_not_supported(
                read_field_status(type, make_fixed_view(DecodedValueKind::INT64, values)));
    }
}

TEST(DataTypeSerDeDecodedValuesDeathTest, ReadFieldRejectsInvalidRowCountDeathTest) {
    auto type = std::make_shared<DataTypeInt32>();
    std::vector<int32_t> values = {1, 2};
    Field field;

    auto zero_row_view = make_fixed_view(DecodedValueKind::INT32, values);
    zero_row_view.row_count = 0;
    EXPECT_DEATH(
            {
                auto status = type->get_serde()->read_field_from_decoded_value(*type, &field,
                                                                               zero_row_view);
                (void)status;
            },
            "view.row_count == 1");

    auto two_row_view = make_fixed_view(DecodedValueKind::INT32, values);
    two_row_view.row_count = 2;
    EXPECT_DEATH(
            {
                auto status = type->get_serde()->read_field_from_decoded_value(*type, &field,
                                                                               two_row_view);
                (void)status;
            },
            "view.row_count == 1");
}

TEST(DataTypeSerDeDecodedValuesDeathTest, ReadFieldRejectsNullFieldPointerDeathTest) {
    auto type = std::make_shared<DataTypeInt32>();
    std::vector<int32_t> values = {1};
    auto view = make_fixed_view(DecodedValueKind::INT32, values);

    EXPECT_DEATH(
            {
                auto status =
                        type->get_serde()->read_field_from_decoded_value(*type, nullptr, view);
                (void)status;
            },
            "field != nullptr");
}

// ----------------------------------------------------------------------
// Illegal kind matrix
// ----------------------------------------------------------------------
// This compact matrix complements the focused error tests above by ensuring each decoded-aware
// family rejects representative illegal physical kinds without mutating an empty destination.

TEST(DataTypeSerDeDecodedValuesTest, IllegalKindMatrixRejectsUnsupportedCombinations) {
    struct Case {
        DataTypePtr type;
        std::vector<DecodedValueKind> illegal_kinds;
    };
    std::vector<Case> cases = {
            {std::make_shared<DataTypeBool>(), {DecodedValueKind::INT32, DecodedValueKind::BINARY}},
            {std::make_shared<DataTypeInt32>(),
             {DecodedValueKind::BOOL, DecodedValueKind::FLOAT, DecodedValueKind::DOUBLE,
              DecodedValueKind::BINARY}},
            {std::make_shared<DataTypeFloat32>(),
             {DecodedValueKind::DOUBLE, DecodedValueKind::INT32}},
            {std::make_shared<DataTypeFloat64>(),
             {DecodedValueKind::FLOAT, DecodedValueKind::INT64}},
            {std::make_shared<DataTypeString>(),
             {DecodedValueKind::INT32, DecodedValueKind::DOUBLE}},
            {std::make_shared<DataTypeDateV2>(),
             {DecodedValueKind::INT64, DecodedValueKind::BINARY}},
            {std::make_shared<DataTypeDateTimeV2>(6),
             {DecodedValueKind::INT32, DecodedValueKind::DOUBLE, DecodedValueKind::BINARY}},
            {std::make_shared<DataTypeTimeV2>(6),
             {DecodedValueKind::BOOL, DecodedValueKind::BINARY, DecodedValueKind::DOUBLE}},
            {std::make_shared<DataTypeDecimal128>(18, 2),
             {DecodedValueKind::BOOL, DecodedValueKind::UINT64, DecodedValueKind::FLOAT,
              DecodedValueKind::DOUBLE}},
    };

    for (const auto& test_case : cases) {
        for (auto kind : test_case.illegal_kinds) {
            std::vector<int64_t> values = {0};
            auto result = read_column(test_case.type, make_fixed_view(kind, values));
            expect_not_supported(result.status);
            EXPECT_EQ(0, result.column->size()) << test_case.type->get_name();
        }
    }
}

} // namespace doris

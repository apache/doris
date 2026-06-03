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

#include <cstdint>
#include <memory>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/string_ref.h"

namespace doris {

TEST(DataTypeSerDeDecodedValuesTest, ReadInt32Values) {
    auto type = std::make_shared<DataTypeInt32>();
    auto column = type->create_column();
    const int32_t values[] = {10, -20, 30};

    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT32;
    view.row_count = 3;
    view.values = reinterpret_cast<const uint8_t*>(values);

    auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
    ASSERT_TRUE(st.ok()) << st;

    const auto& int_column = assert_cast<const ColumnInt32&>(*column);
    ASSERT_EQ(int_column.size(), 3);
    EXPECT_EQ(int_column.get_element(0), 10);
    EXPECT_EQ(int_column.get_element(1), -20);
    EXPECT_EQ(int_column.get_element(2), 30);
}

TEST(DataTypeSerDeDecodedValuesTest, ReadPrimitiveNumberValues) {
    {
        auto type = std::make_shared<DataTypeBool>();
        auto column = type->create_column();
        const bool values[] = {true, false, true};

        DecodedColumnView view;
        view.value_kind = DecodedValueKind::BOOL;
        view.row_count = 3;
        view.values = reinterpret_cast<const uint8_t*>(values);

        auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
        ASSERT_TRUE(st.ok()) << st;

        const auto& bool_column = assert_cast<const ColumnBool&>(*column);
        ASSERT_EQ(bool_column.size(), 3);
        EXPECT_EQ(bool_column.get_element(0), 1);
        EXPECT_EQ(bool_column.get_element(1), 0);
        EXPECT_EQ(bool_column.get_element(2), 1);
    }
    {
        auto type = std::make_shared<DataTypeInt64>();
        auto column = type->create_column();
        const int64_t values[] = {10000000000L, -9L, 42L};

        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT64;
        view.row_count = 3;
        view.values = reinterpret_cast<const uint8_t*>(values);

        auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
        ASSERT_TRUE(st.ok()) << st;

        const auto& int_column = assert_cast<const ColumnInt64&>(*column);
        ASSERT_EQ(int_column.size(), 3);
        EXPECT_EQ(int_column.get_element(0), 10000000000L);
        EXPECT_EQ(int_column.get_element(1), -9L);
        EXPECT_EQ(int_column.get_element(2), 42L);
    }
    {
        auto type = std::make_shared<DataTypeFloat32>();
        auto column = type->create_column();
        const float values[] = {1.5F, -2.25F};

        DecodedColumnView view;
        view.value_kind = DecodedValueKind::FLOAT;
        view.row_count = 2;
        view.values = reinterpret_cast<const uint8_t*>(values);

        auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
        ASSERT_TRUE(st.ok()) << st;

        const auto& float_column = assert_cast<const ColumnFloat32&>(*column);
        ASSERT_EQ(float_column.size(), 2);
        EXPECT_FLOAT_EQ(float_column.get_element(0), 1.5F);
        EXPECT_FLOAT_EQ(float_column.get_element(1), -2.25F);
    }
    {
        auto type = std::make_shared<DataTypeFloat64>();
        auto column = type->create_column();
        const double values[] = {3.5, -4.75};

        DecodedColumnView view;
        view.value_kind = DecodedValueKind::DOUBLE;
        view.row_count = 2;
        view.values = reinterpret_cast<const uint8_t*>(values);

        auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
        ASSERT_TRUE(st.ok()) << st;

        const auto& double_column = assert_cast<const ColumnFloat64&>(*column);
        ASSERT_EQ(double_column.size(), 2);
        EXPECT_DOUBLE_EQ(double_column.get_element(0), 3.5);
        EXPECT_DOUBLE_EQ(double_column.get_element(1), -4.75);
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadStringValues) {
    auto type = std::make_shared<DataTypeString>();
    auto column = type->create_column();
    std::vector<StringRef> values = {
            StringRef("alpha", 5),
            StringRef("beta", 4),
            StringRef("gamma", 5),
    };

    DecodedColumnView view;
    view.value_kind = DecodedValueKind::BINARY;
    view.row_count = values.size();
    view.binary_values = &values;

    auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
    ASSERT_TRUE(st.ok()) << st;

    const auto& string_column = assert_cast<const ColumnString&>(*column);
    ASSERT_EQ(string_column.size(), 3);
    EXPECT_EQ(string_column.get_data_at(0).to_string(), "alpha");
    EXPECT_EQ(string_column.get_data_at(1).to_string(), "beta");
    EXPECT_EQ(string_column.get_data_at(2).to_string(), "gamma");
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDateAndDateTimeValues) {
    {
        auto type = std::make_shared<DataTypeDateV2>();
        auto column = type->create_column();
        const int32_t values[] = {0, 1, 18628};

        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT32;
        view.row_count = 3;
        view.values = reinterpret_cast<const uint8_t*>(values);

        auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
        ASSERT_TRUE(st.ok()) << st;

        ASSERT_EQ(column->size(), 3);
        EXPECT_EQ(type->to_string(*column, 0), "1970-01-01");
        EXPECT_EQ(type->to_string(*column, 1), "1970-01-02");
        EXPECT_EQ(type->to_string(*column, 2), "2021-01-01");
    }
    {
        auto type = std::make_shared<DataTypeDateTimeV2>(6);
        auto column = type->create_column();
        const int64_t values[] = {0, 1234567, -1};

        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT64;
        view.time_unit = DecodedTimeUnit::MICROS;
        view.row_count = 3;
        view.values = reinterpret_cast<const uint8_t*>(values);

        auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
        ASSERT_TRUE(st.ok()) << st;

        ASSERT_EQ(column->size(), 3);
        EXPECT_EQ(type->to_string(*column, 0), "1970-01-01 00:00:00.000000");
        EXPECT_EQ(type->to_string(*column, 1), "1970-01-01 00:00:01.234567");
        EXPECT_EQ(type->to_string(*column, 2), "1969-12-31 23:59:59.999999");
    }
}

TEST(DataTypeSerDeDecodedValuesTest, ReadDecimalValues) {
    auto type = std::make_shared<DataTypeDecimal128>(18, 2);
    auto column = type->create_column();
    const int64_t values[] = {12345, -67, 0};

    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT64;
    view.row_count = 3;
    view.values = reinterpret_cast<const uint8_t*>(values);
    view.decimal_precision = 18;
    view.decimal_scale = 2;

    auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
    ASSERT_TRUE(st.ok()) << st;

    const auto& decimal_column = assert_cast<const ColumnDecimal128V3&>(*column);
    ASSERT_EQ(decimal_column.size(), 3);
    EXPECT_EQ(decimal_column.get_element(0), Decimal128V3(12345));
    EXPECT_EQ(decimal_column.get_element(1), Decimal128V3(-67));
    EXPECT_EQ(decimal_column.get_element(2), Decimal128V3(0));
    EXPECT_EQ(type->to_string(*column, 0), "123.45");
    EXPECT_EQ(type->to_string(*column, 1), "-0.67");
}

TEST(DataTypeSerDeDecodedValuesTest, ReadNullableInt32Values) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto column = type->create_column();
    const int32_t values[] = {1, 2, 3, 4};
    const uint8_t null_map[] = {0, 1, 0, 1};

    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT32;
    view.row_count = 4;
    view.values = reinterpret_cast<const uint8_t*>(values);
    view.null_map = null_map;

    auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    const auto& nested_column =
            assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
    ASSERT_EQ(nullable_column.size(), 4);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_TRUE(nullable_column.is_null_at(3));
    EXPECT_EQ(nested_column.get_element(0), 1);
    EXPECT_EQ(nested_column.get_element(1), 2);
    EXPECT_EQ(nested_column.get_element(2), 3);
    EXPECT_EQ(nested_column.get_element(3), 4);
}

TEST(DataTypeSerDeDecodedValuesTest, RejectMismatchedValueKind) {
    auto type = std::make_shared<DataTypeInt32>();
    auto column = type->create_column();
    const int64_t values[] = {1};

    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT64;
    view.row_count = 1;
    view.values = reinterpret_cast<const uint8_t*>(values);

    auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
    EXPECT_FALSE(st.ok());
}

TEST(DataTypeSerDeDecodedValuesTest, RejectMissingValueBuffer) {
    auto type = std::make_shared<DataTypeInt32>();
    auto column = type->create_column();

    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT32;
    view.row_count = 1;

    auto st = type->get_serde()->read_column_from_decoded_values(*column, view);
    EXPECT_FALSE(st.ok());
}

} // namespace doris

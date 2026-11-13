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

#include "format/parquet/parquet_column_convert.h"

#include <cctz/time_zone.h>

#include <chrono>
#include <cstring>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "util/timezone_utils.h"

namespace doris::parquet {

// Exercise both a direct BYTE_ARRAY source and a physical-to-logical temporary for
// FIXED_LEN_BYTE_ARRAY. Both must prepare destination nulls before string casts.
TEST(ParquetColumnConvertTest, StringCastFailuresPreserveNullsAcrossBatches) {
    for (const auto physical_type :
         {tparquet::Type::BYTE_ARRAY, tparquet::Type::FIXED_LEN_BYTE_ARRAY}) {
        for (const bool nullable_source : {false, true}) {
            SCOPED_TRACE(testing::Message() << "physical_type=" << physical_type
                                            << ", nullable_source=" << nullable_source);
            FieldSchema field;
            field.parquet_schema.__set_type(physical_type);
            field.parquet_schema.__set_type_length(2);
            field.data_type =
                    DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable_source);
            const auto dst_type = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, true);
            auto converter = PhysicalToLogicalConverter::get_converter(&field, field.data_type,
                                                                       dst_type, nullptr);
            ASSERT_TRUE(converter->support()) << converter->get_error_msg();
            ColumnPtr dst = dst_type->create_column();
            const std::vector<std::string> values {"xx", "05", "07", "09"};
            const std::vector<uint8_t> expected_nulls {1, 0, 0, 0};
            for (size_t batch = 0; batch < 2; ++batch) {
                // Retain an alias to ensure preparing the null map respects COW and does not
                // modify an existing batch through another owner.
                ColumnPtr dst_alias = dst;
                MutableColumnPtr nested;
                if (physical_type == tparquet::Type::BYTE_ARRAY) {
                    nested = ColumnString::create();
                } else {
                    nested = ColumnFixedLengthObject::create(2);
                }
                auto nulls = ColumnUInt8::create();
                for (size_t i = 0; i < values.size(); ++i) {
                    nested->insert_data(values[i].data(), values[i].size());
                    nulls->insert_value(i == 2);
                }
                ColumnPtr src;
                if (nullable_source) {
                    src = ColumnNullable::create(std::move(nested), std::move(nulls));
                } else {
                    src = std::move(nested);
                }
                const ColumnPtr src_alias = src;
                ASSERT_TRUE(converter->convert(src, field.data_type, dst_type, dst, false).ok());
                const auto& nullable = assert_cast<const ColumnNullable&>(*dst);
                const auto& numbers = assert_cast<const ColumnInt64&>(nullable.get_nested_column());
                ASSERT_EQ(nullable.get_null_map_data().size(), (batch + 1) * values.size());
                ASSERT_EQ(numbers.size(), nullable.get_null_map_data().size());
                for (size_t i = 0; i < numbers.size(); ++i) {
                    const size_t value_index = i % values.size();
                    EXPECT_EQ(nullable.get_null_map_data()[i],
                              expected_nulls[value_index] || (nullable_source && value_index == 2));
                    if (!nullable.get_null_map_data()[i]) {
                        EXPECT_EQ(numbers.get_data()[i], std::stoi(values[value_index]));
                    }
                }
                EXPECT_EQ(dst_alias->size(), batch * values.size());
                const auto& original = assert_cast<const ColumnNullable&>(*dst_alias);
                EXPECT_EQ(original.get_nested_column().size(), batch * values.size());
                if (nullable_source) {
                    const auto& source = assert_cast<const ColumnNullable&>(*src_alias);
                    EXPECT_EQ(source.get_null_map_data()[0], 0);
                    EXPECT_EQ(source.get_null_map_data()[2], 1);
                }
            }
        }
    }
}

TEST(ParquetColumnConvertTest, InvalidStringToNonNullableBigIntReturnsError) {
    FieldSchema field;
    field.parquet_schema.__set_type(tparquet::Type::BYTE_ARRAY);
    field.data_type = DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
    const auto dst_type = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, false);
    auto converter =
            PhysicalToLogicalConverter::get_converter(&field, field.data_type, dst_type, nullptr);
    ASSERT_TRUE(converter->support()) << converter->get_error_msg();
    auto strings = ColumnString::create();
    strings->insert_data("xx", 2);
    ColumnPtr src = std::move(strings);
    ColumnPtr dst = dst_type->create_column();
    auto status = converter->convert(src, field.data_type, dst_type, dst, false);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Failed to cast"), std::string::npos);
}

TEST(ParquetColumnConvertTest, NumericOverflowMarksNullableDestination) {
    FieldSchema field;
    field.parquet_schema.__set_type(tparquet::Type::INT64);
    field.data_type = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, true);
    const auto dst_type = DataTypeFactory::instance().create_data_type(TYPE_FLOAT, true);
    auto converter =
            PhysicalToLogicalConverter::get_converter(&field, field.data_type, dst_type, nullptr);
    ASSERT_TRUE(converter->support()) << converter->get_error_msg();
    auto values = ColumnInt64::create();
    values->insert_value(1LL << 40);
    values->insert_value(7);
    values->insert_value(9);
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(0);
    nulls->insert_value(0);
    nulls->insert_value(1);
    ColumnPtr src = ColumnNullable::create(std::move(values), std::move(nulls));
    ColumnPtr dst = dst_type->create_column();
    ASSERT_TRUE(converter->convert(src, field.data_type, dst_type, dst, false).ok());
    const auto& nullable = assert_cast<const ColumnNullable&>(*dst);
    ASSERT_EQ(nullable.get_null_map_data().size(), 3);
    ASSERT_EQ(nullable.get_nested_column().size(), 3);
    EXPECT_EQ(nullable.get_null_map_data()[0], 1);
    EXPECT_EQ(nullable.get_null_map_data()[1], 0);
    EXPECT_EQ(nullable.get_null_map_data()[2], 1);
    EXPECT_FLOAT_EQ(assert_cast<const ColumnFloat32&>(nullable.get_nested_column()).get_data()[1],
                    7.0F);
}

TEST(ParquetColumnConvertTest, RejectInvalidUuidPhysicalWidth) {
    FieldSchema field;
    field.parquet_schema.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    field.parquet_schema.__set_type_length(15);
    tparquet::LogicalType logical_type;
    logical_type.__set_UUID(tparquet::UUIDType());
    field.parquet_schema.__set_logicalType(logical_type);
    field.data_type = DataTypeFactory::instance().create_data_type(TYPE_STRING, true);
    auto converter = PhysicalToLogicalConverter::get_converter(&field, field.data_type,
                                                               field.data_type, nullptr);
    EXPECT_FALSE(converter->support());
}

TEST(ParquetColumnConvertTest, UuidTextAndBinaryCarriersPreserveNullsAcrossBatches) {
    FieldSchema field;
    field.parquet_schema.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    field.parquet_schema.__set_type_length(16);
    tparquet::LogicalType logical_type;
    logical_type.__set_UUID(tparquet::UUIDType());
    field.parquet_schema.__set_logicalType(logical_type);
    field.data_type = DataTypeFactory::instance().create_data_type(TYPE_STRING, true);
    const std::vector<std::string> raw_values = {std::string(16, '\0'),
                                                 std::string(16, '\x7f'),
                                                 std::string("\x80", 1) + std::string(15, '\0'),
                                                 std::string(16, '\xff'),
                                                 "abcDeFGhijkLmnOp",
                                                 std::string(16, '\0')};
    const std::vector<std::string> text_values = {
            "00000000-0000-0000-0000-000000000000", "7f7f7f7f-7f7f-7f7f-7f7f-7f7f7f7f7f7f",
            "80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "61626344-6546-4768-696a-6b4c6d6e4f70", "00000000-0000-0000-0000-000000000000"};
    for (int carrier = 0; carrier < 3; ++carrier) {
        const bool preserve_binary = carrier == 1;
        const auto dst_type = DataTypeFactory::instance().create_data_type(
                carrier == 2 ? TYPE_VARBINARY : TYPE_STRING, true);
        auto converter = PhysicalToLogicalConverter::get_converter(
                &field, field.data_type, dst_type, nullptr, false, preserve_binary);
        ASSERT_TRUE(converter->support());
        ColumnPtr dst = dst_type->create_column();
        for (size_t batch = 0; batch < 2; ++batch) {
            auto values = ColumnFixedLengthObject::create(16);
            auto nulls = ColumnUInt8::create();
            for (size_t i = batch * 3; i < (batch + 1) * 3; ++i) {
                values->insert_data(raw_values[i].data(), 16);
                nulls->insert_value(i == 5);
            }
            ColumnPtr src = ColumnNullable::create(std::move(values), std::move(nulls));
            ASSERT_TRUE(converter->convert(src, field.data_type, dst_type, dst, false).ok());
            EXPECT_EQ(dst->size(), (batch + 1) * 3);
        }
        const auto& nullable = assert_cast<const ColumnNullable&>(*dst);
        for (size_t i = 0; i < raw_values.size(); ++i) {
            EXPECT_EQ(nullable.get_null_map_data()[i], i == 5);
            EXPECT_EQ(nullable.get_nested_column().get_data_at(i).to_string(),
                      carrier != 0 ? raw_values[i] : text_values[i]);
        }
    }
}

} // namespace doris::parquet

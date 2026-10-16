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
    for (const bool preserve_binary : {false, true}) {
        auto converter = PhysicalToLogicalConverter::get_converter(
                &field, field.data_type, field.data_type, nullptr, false, preserve_binary);
        ASSERT_TRUE(converter->support());
        ColumnPtr dst = field.data_type->create_column();
        for (size_t batch = 0; batch < 2; ++batch) {
            auto values = ColumnFixedLengthObject::create(16);
            auto nulls = ColumnUInt8::create();
            for (size_t i = batch * 3; i < (batch + 1) * 3; ++i) {
                values->insert_data(raw_values[i].data(), 16);
                nulls->insert_value(i == 5);
            }
            ColumnPtr src = ColumnNullable::create(std::move(values), std::move(nulls));
            ASSERT_TRUE(converter->convert(src, field.data_type, field.data_type, dst, false).ok());
            EXPECT_EQ(dst->size(), (batch + 1) * 3);
        }
        const auto& nullable = assert_cast<const ColumnNullable&>(*dst);
        for (size_t i = 0; i < raw_values.size(); ++i) {
            EXPECT_EQ(nullable.get_null_map_data()[i], i == 5);
            EXPECT_EQ(nullable.get_nested_column().get_data_at(i).to_string(),
                      preserve_binary ? raw_values[i] : text_values[i]);
        }
    }
}

} // namespace doris::parquet

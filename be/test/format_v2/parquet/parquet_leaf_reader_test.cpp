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

#include "format_v2/parquet/reader/parquet_leaf_reader.h"

#include <arrow/array/builder_binary.h>
#include <gtest/gtest.h>

#include <cmath>
#include <cstring>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"

namespace doris::format::parquet {
namespace {

std::shared_ptr<arrow::Array> fixed_binary_array(const std::vector<std::string>& values,
                                                 int byte_width) {
    auto type = arrow::fixed_size_binary(byte_width);
    arrow::FixedSizeBinaryBuilder builder(type, arrow::default_memory_pool());
    for (const auto& value : values) {
        EXPECT_TRUE(builder.Append(reinterpret_cast<const uint8_t*>(value.data())).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

ParquetLeafReader make_leaf_reader(ParquetTypeDescriptor descriptor, DataTypePtr type) {
    return ParquetLeafReader(nullptr, descriptor, std::move(type), "leaf", nullptr);
}

} // namespace

TEST(ParquetLeafReaderTest, DenseNullableFixedValuesAreSpacedBeforeSerde) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::INT32;
    auto type = make_nullable(std::make_shared<DataTypeInt32>());
    auto reader = make_leaf_reader(descriptor, type);

    const std::vector<int32_t> compact_values = {10, 30, 50};
    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::INT32;
    batch._fixed_values = reinterpret_cast<const uint8_t*>(compact_values.data());
    batch._values_written = compact_values.size();
    batch._read_dense_for_nullable = true;

    const NullMap null_map = {0, 1, 0, 1, 0};
    auto column = type->create_column();
    auto status = reader.append_values(batch, 5, &null_map, column);
    ASSERT_TRUE(status.ok()) << status;

    const auto& nullable = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable.size(), 5);
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_TRUE(nullable.is_null_at(1));
    EXPECT_FALSE(nullable.is_null_at(2));
    EXPECT_TRUE(nullable.is_null_at(3));
    EXPECT_FALSE(nullable.is_null_at(4));
    const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    EXPECT_EQ(nested.get_element(0), 10);
    EXPECT_EQ(nested.get_element(2), 30);
    EXPECT_EQ(nested.get_element(4), 50);
}

TEST(ParquetLeafReaderTest, DenseNullableFixedValuesRejectCountMismatch) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::INT32;
    auto type = make_nullable(std::make_shared<DataTypeInt32>());
    auto reader = make_leaf_reader(descriptor, type);

    const std::vector<int32_t> compact_values = {10, 30};
    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::INT32;
    batch._fixed_values = reinterpret_cast<const uint8_t*>(compact_values.data());
    batch._values_written = compact_values.size();
    batch._read_dense_for_nullable = true;

    const NullMap null_map = {0, 1, 0, 1, 0};
    auto column = type->create_column();
    auto status = reader.append_values(batch, 5, &null_map, column);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid dense nullable parquet values"), std::string::npos);
}

TEST(ParquetLeafReaderTest, Float16BinaryValuesAreConvertedToFloat) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY;
    descriptor.extra_type_info = ParquetExtraTypeInfo::FLOAT16;
    descriptor.fixed_length = 2;
    auto type = std::make_shared<DataTypeFloat32>();
    auto reader = make_leaf_reader(descriptor, type);

    auto half = [](uint16_t value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };

    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::FIXED_BINARY;
    batch._binary_chunks = {fixed_binary_array(
            {half(0x0000), half(0x8000), half(0x3E00), half(0x0001), half(0x7E00)}, 2)};
    batch._values_written = 5;

    auto column = type->create_column();
    auto status = reader.append_values(batch, 5, nullptr, column);
    ASSERT_TRUE(status.ok()) << status;

    const auto& floats = assert_cast<const ColumnFloat32&>(*column);
    ASSERT_EQ(floats.size(), 5);
    EXPECT_FLOAT_EQ(floats.get_element(0), 0.0F);
    EXPECT_TRUE(std::signbit(floats.get_element(1)));
    EXPECT_FLOAT_EQ(floats.get_element(2), 1.5F);
    EXPECT_NEAR(floats.get_element(3), 5.9604645e-8F, 1e-12F);
    EXPECT_TRUE(std::isnan(floats.get_element(4)));
}

TEST(ParquetLeafReaderTest, BinaryDenseNullableValuesAreSpacedWithNullRefs) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::BYTE_ARRAY;
    auto type = make_nullable(std::make_shared<DataTypeString>());
    auto reader = make_leaf_reader(descriptor, type);

    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append("aa").ok());
    ASSERT_TRUE(builder.Append("cc").ok());
    ASSERT_TRUE(builder.Append("ee").ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::BINARY;
    batch._binary_chunks = {array};
    batch._values_written = 3;
    batch._read_dense_for_nullable = true;

    const NullMap null_map = {0, 1, 0, 1, 0};
    auto column = type->create_column();
    auto status = reader.append_values(batch, 5, &null_map, column);
    ASSERT_TRUE(status.ok()) << status;

    const auto& nullable = assert_cast<const ColumnNullable&>(*column);
    const auto& strings = assert_cast<const ColumnString&>(nullable.get_nested_column());
    ASSERT_EQ(nullable.size(), 5);
    EXPECT_EQ(strings.get_data_at(0).to_string(), "aa");
    EXPECT_TRUE(nullable.is_null_at(1));
    EXPECT_EQ(strings.get_data_at(2).to_string(), "cc");
    EXPECT_TRUE(nullable.is_null_at(3));
    EXPECT_EQ(strings.get_data_at(4).to_string(), "ee");
}

TEST(ParquetLeafReaderTest, BinaryDenseNullableRejectsCountMismatch) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::BYTE_ARRAY;
    auto type = make_nullable(std::make_shared<DataTypeString>());
    auto reader = make_leaf_reader(descriptor, type);

    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append("only_one").ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::BINARY;
    batch._binary_chunks = {array};
    batch._values_written = 1;
    batch._read_dense_for_nullable = true;

    const NullMap null_map = {0, 1, 0};
    auto column = type->create_column();
    auto status = reader.append_values(batch, 3, &null_map, column);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid dense nullable parquet binary values"),
              std::string::npos);
}

} // namespace doris::format::parquet

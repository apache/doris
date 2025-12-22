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

#include "vec/exec/format/table/parquet_utils.h"

#include <gtest/gtest.h>

#include <cstring>

#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized::parquet_utils {

TEST(ParquetUtilsTest, JoinPath) {
    EXPECT_EQ("a.b.c", join_path({"a", "b", "c"}));
    EXPECT_EQ("", join_path({}));
}

TEST(ParquetUtilsTest, InsertNumericAndString) {
    {
        auto col = ColumnInt32::create();
        MutableColumnPtr ptr = col->get_ptr();
        insert_int32(ptr, 42);
        ASSERT_EQ(1, ptr->size());
        EXPECT_EQ(42, assert_cast<ColumnInt32&>(*ptr).get_data()[0]);
    }
    {
        auto col = ColumnInt64::create();
        MutableColumnPtr ptr = col->get_ptr();
        insert_int64(ptr, 123456789LL);
        ASSERT_EQ(1, ptr->size());
        EXPECT_EQ(123456789LL, assert_cast<ColumnInt64&>(*ptr).get_data()[0]);
    }
    {
        auto col = ColumnUInt8::create();
        MutableColumnPtr ptr = col->get_ptr();
        insert_bool(ptr, true);
        insert_bool(ptr, false);
        ASSERT_EQ(2, ptr->size());
        const auto& data = assert_cast<ColumnUInt8&>(*ptr).get_data();
        EXPECT_EQ(1, data[0]);
        EXPECT_EQ(0, data[1]);
    }
    {
        auto col = ColumnString::create();
        MutableColumnPtr ptr = col->get_ptr();
        insert_string(ptr, "abc");
        ASSERT_EQ(1, ptr->size());
        EXPECT_EQ("abc", assert_cast<ColumnString&>(*ptr).get_data_at(0).to_string());
    }
}

TEST(ParquetUtilsTest, InsertIntoNullable) {
    {
        auto nullable = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
        MutableColumnPtr ptr = nullable->get_ptr();
        insert_int32(ptr, 7);
        insert_null(ptr);
        ASSERT_EQ(2, ptr->size());
        const auto& nested_data =
                assert_cast<ColumnInt32&>(nullable->get_nested_column()).get_data();
        EXPECT_EQ(7, nested_data[0]);
        EXPECT_EQ(0, nested_data[1]);
        EXPECT_EQ(0, nullable->get_null_map_data()[0]);
        EXPECT_EQ(1, nullable->get_null_map_data()[1]);
    }
    {
        auto nullable = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
        MutableColumnPtr ptr = nullable->get_ptr();
        insert_string(ptr, "xyz");
        insert_null(ptr);
        ASSERT_EQ(2, ptr->size());
        EXPECT_EQ("xyz", nullable->get_nested_column().get_data_at(0).to_string());
        EXPECT_EQ("", nullable->get_nested_column().get_data_at(1).to_string());
        EXPECT_EQ(0, nullable->get_null_map_data()[0]);
        EXPECT_EQ(1, nullable->get_null_map_data()[1]);
    }
}

TEST(ParquetUtilsTest, TypeToString) {
    EXPECT_EQ("INT32", physical_type_to_string(tparquet::Type::INT32));
    EXPECT_EQ("UNKNOWN", physical_type_to_string(static_cast<tparquet::Type::type>(-1)));

    EXPECT_EQ("SNAPPY", compression_to_string(tparquet::CompressionCodec::SNAPPY));
    EXPECT_EQ("UNKNOWN", compression_to_string(static_cast<tparquet::CompressionCodec::type>(-1)));

    EXPECT_EQ("UINT_32", converted_type_to_string(tparquet::ConvertedType::UINT_32));
    EXPECT_EQ("UNKNOWN", converted_type_to_string(static_cast<tparquet::ConvertedType::type>(-1)));
}

TEST(ParquetUtilsTest, LogicalTypeToString) {
    {
        tparquet::SchemaElement element;
        tparquet::LogicalType logical;
        logical.__set_STRING(tparquet::StringType());
        element.__set_logicalType(logical);
        EXPECT_EQ("STRING", logical_type_to_string(element));
    }
    {
        tparquet::SchemaElement element;
        element.__set_converted_type(tparquet::ConvertedType::UTF8);
        EXPECT_EQ("UTF8", logical_type_to_string(element));
    }
    {
        tparquet::SchemaElement element;
        EXPECT_EQ("", logical_type_to_string(element));
    }
}

TEST(ParquetUtilsTest, EncodingsToString) {
    std::vector<tparquet::Encoding::type> encodings = {
            tparquet::Encoding::PLAIN, tparquet::Encoding::RLE_DICTIONARY,
            tparquet::Encoding::DELTA_BYTE_ARRAY, static_cast<tparquet::Encoding::type>(-1)};
    EXPECT_EQ("PLAIN,RLE_DICTIONARY,DELTA_BYTE_ARRAY,UNKNOWN", encodings_to_string(encodings));
}

TEST(ParquetUtilsTest, TryGetStatisticsEncodedValue) {
    std::string value;
    {
        tparquet::Statistics stats;
        stats.__set_min_value("min_value");
        stats.__set_min("min_deprecated");
        EXPECT_TRUE(try_get_statistics_encoded_value(stats, true, &value));
        EXPECT_EQ("min_value", value);
    }
    {
        tparquet::Statistics stats;
        stats.__set_min("min_only");
        EXPECT_TRUE(try_get_statistics_encoded_value(stats, true, &value));
        EXPECT_EQ("min_only", value);
    }
    {
        tparquet::Statistics stats;
        stats.__set_max_value("max_value");
        stats.__set_max("max_deprecated");
        EXPECT_TRUE(try_get_statistics_encoded_value(stats, false, &value));
        EXPECT_EQ("max_value", value);
    }
    {
        tparquet::Statistics stats;
        stats.__set_max("max_only");
        EXPECT_TRUE(try_get_statistics_encoded_value(stats, false, &value));
        EXPECT_EQ("max_only", value);
    }
    {
        tparquet::Statistics stats;
        value = "stale";
        EXPECT_FALSE(try_get_statistics_encoded_value(stats, true, &value));
        EXPECT_EQ("", value);
    }
}

TEST(ParquetUtilsTest, BytesToHexString) {
    std::string bytes;
    bytes.push_back(static_cast<char>(0x00));
    bytes.push_back(static_cast<char>(0x7F));
    bytes.push_back(static_cast<char>(0xFF));
    EXPECT_EQ("0x007FFF", bytes_to_hex_string(bytes));
}

TEST(ParquetUtilsTest, DecodeStatisticsValue) {
    auto tz = cctz::utc_time_zone();
    EXPECT_EQ("", decode_statistics_value(nullptr, tparquet::Type::BYTE_ARRAY, "", tz));
    EXPECT_EQ("0x616263", decode_statistics_value(nullptr, tparquet::Type::BYTE_ARRAY, "abc", tz));

    FieldSchema field;
    field.name = "col";
    field.parquet_schema.__set_type(tparquet::Type::INT32);
    field.parquet_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    field.data_type = std::make_shared<DataTypeInt32>();
    field.physical_type = tparquet::Type::INT32;

    Int32 value = 12345;
    std::string encoded(sizeof(Int32), '\0');
    memcpy(encoded.data(), &value, sizeof(Int32));
    EXPECT_EQ("12345", decode_statistics_value(&field, tparquet::Type::INT32, encoded, tz));
}

TEST(ParquetUtilsTest, BuildPathMap) {
    FieldSchema root;
    root.name = "root";
    FieldSchema child_a;
    child_a.name = "a";
    FieldSchema child_b;
    child_b.name = "b";
    FieldSchema child_c;
    child_c.name = "c";
    child_b.children.push_back(child_c);
    root.children.push_back(child_a);
    root.children.push_back(child_b);

    std::unordered_map<std::string, const FieldSchema*> map;
    build_path_map(root, "", &map);
    ASSERT_EQ(2, map.size());
    EXPECT_EQ("a", map["root.a"]->name);
    EXPECT_EQ("c", map["root.b.c"]->name);
}

} // namespace doris::vectorized::parquet_utils

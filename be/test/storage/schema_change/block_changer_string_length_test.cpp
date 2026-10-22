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

#include <cstring>
#include <string>

#include "common/status.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "storage/olap_define.h"
#include "storage/schema_change/schema_change.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

// Builds the new tablet column the same way as the tablet meta of a created tablet, whose
// VARCHAR length includes the 2-byte length prefix.
TabletColumn varchar_column(int32_t declared_length) {
    return {FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, FieldType::OLAP_FIELD_TYPE_VARCHAR,
            true, 1, declared_length + sizeof(OLAP_VARCHAR_MAX_LENGTH)};
}

TabletColumn char_column(int32_t declared_length) {
    return {FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, FieldType::OLAP_FIELD_TYPE_CHAR,
            true, 1, static_cast<size_t>(declared_length)};
}

} // namespace

TEST(BlockChangerStringLengthTest, AcceptsValuesWithinSchemaLength) {
    auto column = ColumnString::create();
    column->insert_data("abc", 3);
    column->insert_data("abcdef", 6);
    column->insert_data("", 0);
    EXPECT_TRUE(BlockChanger::check_string_length_within_schema(std::move(column),
                                                                varchar_column(6), "c")
                        .ok());
}

TEST(BlockChangerStringLengthTest, RejectsValueLongerThanSchemaLength) {
    auto column = ColumnString::create();
    column->insert_data("abc", 3);
    column->insert_data(R"({"a":1})", 7);
    Status st = BlockChanger::check_string_length_within_schema(std::move(column),
                                                                varchar_column(5), "j");
    EXPECT_EQ(st.code(), ErrorCode::DATA_QUALITY_ERROR);
    EXPECT_NE(st.to_string().find("exceeds the length of column j"), std::string::npos);
    EXPECT_NE(st.to_string().find("schema length: 5"), std::string::npos);
    EXPECT_NE(st.to_string().find("actual length: 7"), std::string::npos);
    EXPECT_NE(st.to_string().find(R"([{"a":1}])"), std::string::npos);
}

TEST(BlockChangerStringLengthTest, ExcludesVarcharLengthPrefix) {
    // VARCHAR(5) has tablet column length 7; values of 6 and 7 bytes must still be rejected.
    for (const auto* value : {"abcdef", "abcdefg"}) {
        auto column = ColumnString::create();
        column->insert_data(value, strlen(value));
        EXPECT_EQ(BlockChanger::check_string_length_within_schema(std::move(column),
                                                                  varchar_column(5), "c")
                          .code(),
                  ErrorCode::DATA_QUALITY_ERROR)
                << value;
    }
}

TEST(BlockChangerStringLengthTest, ChecksCharLength) {
    auto column = ColumnString::create();
    column->insert_data("abcde", 5);
    ColumnPtr ptr = std::move(column);
    EXPECT_TRUE(BlockChanger::check_string_length_within_schema(ptr, char_column(5), "c").ok());
    EXPECT_EQ(BlockChanger::check_string_length_within_schema(ptr, char_column(4), "c").code(),
              ErrorCode::DATA_QUALITY_ERROR);
}

TEST(BlockChangerStringLengthTest, IgnoresStringColumn) {
    // STRING has no declared length.
    auto column = ColumnString::create();
    column->insert_data("abcdefgh", 8);
    TabletColumn string_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                               FieldType::OLAP_FIELD_TYPE_STRING, true, 1, 1);
    EXPECT_TRUE(
            BlockChanger::check_string_length_within_schema(std::move(column), string_column, "c")
                    .ok());
}

TEST(BlockChangerStringLengthTest, ComparesByteLength) {
    // "中文" is 6 bytes in UTF-8, VARCHAR length is in bytes.
    auto column = ColumnString::create();
    column->insert_data("中文", 6);
    ColumnPtr ptr = std::move(column);
    EXPECT_TRUE(BlockChanger::check_string_length_within_schema(ptr, varchar_column(6), "c").ok());
    EXPECT_EQ(BlockChanger::check_string_length_within_schema(ptr, varchar_column(5), "c").code(),
              ErrorCode::DATA_QUALITY_ERROR);
}

TEST(BlockChangerStringLengthTest, SkipsNullRowsButChecksNonNullRows) {
    // The nested payload of a NULL row is ignored even if it is long.
    auto nested = ColumnString::create();
    nested->insert_data("zzzzzzzzzz", 10);
    nested->insert_data("ab", 2);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(1);
    null_map->insert_value(0);
    ColumnPtr nullable = ColumnNullable::create(std::move(nested), std::move(null_map));
    EXPECT_TRUE(
            BlockChanger::check_string_length_within_schema(nullable, varchar_column(2), "c").ok());
    EXPECT_EQ(BlockChanger::check_string_length_within_schema(nullable, varchar_column(1), "c")
                      .code(),
              ErrorCode::DATA_QUALITY_ERROR);
}

} // namespace doris

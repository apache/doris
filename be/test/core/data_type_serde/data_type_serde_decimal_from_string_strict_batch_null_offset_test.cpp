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

#include <array>

#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_decimal_serde.h"
#include "testutil/column_helper.h"

namespace doris {

// Same defect as DataTypeNumberSerDeFromStringStrictModeBatchTest, in the sibling decimal serde:
// a row's own bytes are chars[offsets[i-1]..offsets[i]), and an externally-marked null row must
// not desync the cursor used to read every later row's bytes. Scale 0 so parsed values compare
// directly against the raw stored integer.
TEST(DataTypeDecimalSerDeFromStringStrictModeBatchTest, SkipsNullRowsWithoutDesyncingOffsets) {
    auto str_col = ColumnHelper::create_column<DataTypeString>({"-", "2628"});
    const auto& col_str = assert_cast<const ColumnString&>(*str_col);
    constexpr std::array<NullMap::value_type, 2> null_map {1, 0};

    auto column_to = ColumnDecimal32::create(0, 0);
    DataTypeDecimalSerDe<TYPE_DECIMAL32> serde(9, 0);
    DataTypeSerDe::FormatOptions options;

    Status st = serde.from_string_strict_mode_batch(col_str, *column_to, options, null_map.data());
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(column_to->size(), 2);
    EXPECT_EQ(column_to->get_data()[1].value, 2628);
}

TEST(DataTypeDecimalSerDeFromStringStrictModeBatchTest, SkipsMultipleConsecutiveNullRows) {
    auto str_col = ColumnHelper::create_column<DataTypeString>({"-", "-", "-", "-", "-", "2628"});
    const auto& col_str = assert_cast<const ColumnString&>(*str_col);
    constexpr std::array<NullMap::value_type, 6> null_map {1, 1, 1, 1, 1, 0};

    auto column_to = ColumnDecimal32::create(0, 0);
    DataTypeDecimalSerDe<TYPE_DECIMAL32> serde(9, 0);
    DataTypeSerDe::FormatOptions options;

    Status st = serde.from_string_strict_mode_batch(col_str, *column_to, options, null_map.data());
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(column_to->size(), 6);
    EXPECT_EQ(column_to->get_data()[5].value, 2628);
}

// With no preceding null rows an invalid value is still rejected, and the error message
// quotes exactly that row's own bytes.
TEST(DataTypeDecimalSerDeFromStringStrictModeBatchTest, RejectsInvalidValueWithoutPrecedingNulls) {
    auto str_col = ColumnHelper::create_column<DataTypeString>({"-", "2628"});
    const auto& col_str = assert_cast<const ColumnString&>(*str_col);
    constexpr std::array<NullMap::value_type, 2> null_map {0, 0};

    auto column_to = ColumnDecimal32::create(0, 0);
    DataTypeDecimalSerDe<TYPE_DECIMAL32> serde(9, 0);
    DataTypeSerDe::FormatOptions options;

    Status st = serde.from_string_strict_mode_batch(col_str, *column_to, options, null_map.data());
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("parse number fail, string: '-'"), std::string::npos) << st;
}

// A null row whose nested slice is empty, as insert_default() produces, is skipped the same
// way as one that still carries bytes.
TEST(DataTypeDecimalSerDeFromStringStrictModeBatchTest, SkipsNullRowWithEmptyNestedSlice) {
    auto str_col = ColumnHelper::create_column<DataTypeString>({"", "2628"});
    const auto& col_str = assert_cast<const ColumnString&>(*str_col);
    constexpr std::array<NullMap::value_type, 2> null_map {1, 0};

    auto column_to = ColumnDecimal32::create(0, 0);
    DataTypeDecimalSerDe<TYPE_DECIMAL32> serde(9, 0);
    DataTypeSerDe::FormatOptions options;

    Status st = serde.from_string_strict_mode_batch(col_str, *column_to, options, null_map.data());
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(column_to->size(), 2);
    EXPECT_EQ(column_to->get_data()[1].value, 2628);
}

} // namespace doris

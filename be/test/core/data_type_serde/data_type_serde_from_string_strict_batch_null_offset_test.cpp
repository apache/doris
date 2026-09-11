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
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_number_serde.h"
#include "testutil/column_helper.h"

namespace doris {

// A row's own bytes are chars[offsets[i-1]..offsets[i]); a caller may mark a row null via an
// externally-supplied null_map without the underlying ColumnString slice being empty (this is
// exactly what if(cond, NULL, nullable_col) produces via ColumnNullable::apply_null_map).
// from_string_strict_mode_batch must read every row from its own offsets regardless of the
// preceding rows' null-map bits.
TEST(DataTypeNumberSerDeFromStringStrictModeBatchTest, SkipsNullRowsWithoutDesyncingOffsets) {
    auto str_col = ColumnHelper::create_column<DataTypeString>({"-", "2628"});
    const auto& col_str = assert_cast<const ColumnString&>(*str_col);
    constexpr std::array<NullMap::value_type, 2> null_map {1, 0};

    auto column_to = ColumnInt64::create();
    DataTypeNumberSerDe<TYPE_BIGINT> serde;
    DataTypeSerDe::FormatOptions options;

    Status st = serde.from_string_strict_mode_batch(col_str, *column_to, options, null_map.data());
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(column_to->size(), 2);
    EXPECT_EQ(column_to->get_data()[1], 2628);
}

TEST(DataTypeNumberSerDeFromStringStrictModeBatchTest, SkipsMultipleConsecutiveNullRows) {
    auto str_col = ColumnHelper::create_column<DataTypeString>({"-", "-", "-", "-", "-", "2628"});
    const auto& col_str = assert_cast<const ColumnString&>(*str_col);
    constexpr std::array<NullMap::value_type, 6> null_map {1, 1, 1, 1, 1, 0};

    auto column_to = ColumnInt64::create();
    DataTypeNumberSerDe<TYPE_BIGINT> serde;
    DataTypeSerDe::FormatOptions options;

    Status st = serde.from_string_strict_mode_batch(col_str, *column_to, options, null_map.data());
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(column_to->size(), 6);
    EXPECT_EQ(column_to->get_data()[5], 2628);
}

// With no preceding null rows an invalid value is still rejected, and the error message
// quotes exactly that row's own bytes.
TEST(DataTypeNumberSerDeFromStringStrictModeBatchTest, RejectsInvalidValueWithoutPrecedingNulls) {
    auto str_col = ColumnHelper::create_column<DataTypeString>({"-", "2628"});
    const auto& col_str = assert_cast<const ColumnString&>(*str_col);
    constexpr std::array<NullMap::value_type, 2> null_map {0, 0};

    auto column_to = ColumnInt64::create();
    DataTypeNumberSerDe<TYPE_BIGINT> serde;
    DataTypeSerDe::FormatOptions options;

    Status st = serde.from_string_strict_mode_batch(col_str, *column_to, options, null_map.data());
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("parse number fail, string: '-'"), std::string::npos) << st;
}

// A null row whose nested slice is empty, as insert_default() produces, is skipped the same
// way as one that still carries bytes.
TEST(DataTypeNumberSerDeFromStringStrictModeBatchTest, SkipsNullRowWithEmptyNestedSlice) {
    auto str_col = ColumnHelper::create_column<DataTypeString>({"", "2628"});
    const auto& col_str = assert_cast<const ColumnString&>(*str_col);
    constexpr std::array<NullMap::value_type, 2> null_map {1, 0};

    auto column_to = ColumnInt64::create();
    DataTypeNumberSerDe<TYPE_BIGINT> serde;
    DataTypeSerDe::FormatOptions options;

    Status st = serde.from_string_strict_mode_batch(col_str, *column_to, options, null_map.data());
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(column_to->size(), 2);
    EXPECT_EQ(column_to->get_data()[1], 2628);
}

} // namespace doris

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

#include "vec/exec/format/column_type_convert.h"

#include <gtest/gtest.h>

#include <limits>

#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class ColumnTypeConverterTest : public testing::Test {
public:
    ColumnTypeConverterTest() = default;
    virtual ~ColumnTypeConverterTest() = default;
};

TEST_F(ColumnTypeConverterTest, TestDateTimeV2ToNumericConversions) {
    using namespace doris::vectorized;
    auto make_datetimev2_col =
            [](const std::vector<std::tuple<int, int, int, int, int, int, int>>& datetimes) {
                auto col = ColumnDateTimeV2::create();
                for (const auto& [y, m, d, h, min, s, micro] : datetimes) {
                    DateV2Value<DateTimeV2ValueType> v;
                    v.unchecked_set_time(y, m, d, h, min, s, micro);
                    col->get_data().push_back(*reinterpret_cast<vectorized::UInt64*>(&v));
                }
                return col;
            };

    auto parse_datetimev2_str = [](const std::string& datetime_str) {
        UInt64 x = 0;
        ReadBuffer buf((char*)datetime_str.data(), datetime_str.size());
        bool ok = read_datetime_v2_text_impl(x, buf, 6);
        CHECK(ok) << "parse_datetimev2_str failed for: " << datetime_str;
        return x;
    };

    // 1. DATETIMEV2 -> BIGINT
    {
        TypeDescriptor src_type(TYPE_DATETIMEV2);
        auto dst_type = std::make_shared<DataTypeInt64>();
        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type);

        ASSERT_TRUE(converter->support());

        // 2024-01-01 00:00:00.123456
        auto src_col = make_datetimev2_col({{2024, 1, 1, 0, 0, 0, 123456}});
        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        auto& dst_data = static_cast<ColumnInt64&>(*mutable_dst).get_data();
        ASSERT_EQ(1, dst_data.size());
        EXPECT_EQ(1704067200123, dst_data[0]);
    }

    // 2. DATETIMEV2 -> INT
    {
        TypeDescriptor src_type(TYPE_DATETIMEV2);
        auto dst_type = std::make_shared<DataTypeInt32>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);
        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type);

        ASSERT_TRUE(converter->support());

        // 1970-01-01 00:00:00.000000
        // 3000-01-01 00:00:00.000000
        auto src_col = make_datetimev2_col({{1970, 1, 1, 0, 0, 0, 0}, {3000, 1, 1, 0, 0, 0, 0}});
        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();
        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());
        auto& nested_col = static_cast<ColumnInt32&>(nullable_col.get_nested_column());
        auto& dst_data = nested_col.get_data();

        ASSERT_EQ(2, nested_col.size());
        EXPECT_EQ(0, null_map[0]);
        ASSERT_EQ(0, dst_data[0]);
        EXPECT_EQ(1, null_map[1]);
    }

    // 3. DATETIMEV2 -> INT, non-nullable
    {
        TypeDescriptor src_type(TYPE_DATETIMEV2);
        auto dst_type = std::make_shared<DataTypeInt32>();
        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type);

        ASSERT_TRUE(converter->support());

        // 3000-01-01 00:00:00.000000（会溢出int32）
        auto src_col = make_datetimev2_col({{3000, 1, 1, 0, 0, 0, 0}});
        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_FALSE(st.ok());
    }

    {
        TypeDescriptor src_type(TYPE_DATETIMEV2);
        auto dst_type = std::make_shared<DataTypeInt64>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);
        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type);

        ASSERT_TRUE(converter->support());

        auto src_col = ColumnDateTimeV2::create();
        src_col->get_data().push_back(parse_datetimev2_str("2024-01-01 12:34:56.123456"));
        src_col->get_data().push_back(parse_datetimev2_str("1970-01-01 00:00:00.000000"));
        src_col->get_data().push_back(parse_datetimev2_str("3000-01-01 00:00:00.000000"));
        src_col->get_data().push_back(parse_datetimev2_str("1900-01-01 00:00:00.000000"));
        src_col->get_data().push_back(parse_datetimev2_str("1999-12-31 23:59:59.999999"));
        src_col->get_data().push_back(parse_datetimev2_str("2000-01-01 00:00:00.000000"));
        src_col->get_data().push_back(parse_datetimev2_str("2025-07-08 16:00:00.123456"));
        src_col->get_data().push_back(parse_datetimev2_str("2100-01-01 00:00:00.000000"));
        src_col->get_data().push_back(parse_datetimev2_str("9999-12-31 23:59:59.999999"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 12:00:00.000001"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 13:00:00.000002"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 14:00:00.000004"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 12:00:00"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 13:00:00"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 14:00:00"));

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();
        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(15, null_map.size());
        EXPECT_EQ(0, null_map[0]);
        EXPECT_EQ(0, null_map[1]);
        EXPECT_EQ(0, null_map[2]);
        EXPECT_EQ(0, null_map[3]);
        EXPECT_EQ(0, null_map[4]);
        EXPECT_EQ(0, null_map[5]);
        EXPECT_EQ(0, null_map[6]);
        EXPECT_EQ(0, null_map[7]);
        EXPECT_EQ(0, null_map[8]);
        EXPECT_EQ(0, null_map[9]);
        EXPECT_EQ(0, null_map[10]);
        EXPECT_EQ(0, null_map[11]);
        EXPECT_EQ(0, null_map[12]);
        EXPECT_EQ(0, null_map[13]);
        EXPECT_EQ(0, null_map[14]);

        auto& dst_data = static_cast<ColumnInt64&>(nullable_col.get_nested_column()).get_data();
        ASSERT_EQ(15, dst_data.size());
        EXPECT_EQ(1704112496123L, dst_data[0]);
        EXPECT_EQ(0L, dst_data[1]);
        EXPECT_EQ(32503680000000L, dst_data[2]);
        EXPECT_EQ(-2208988800000L, dst_data[3]);
        EXPECT_EQ(946684799999L, dst_data[4]);
        EXPECT_EQ(946684800000L, dst_data[5]);
        EXPECT_EQ(1751990400123, dst_data[6]);
        EXPECT_EQ(4102444800000L, dst_data[7]);
        EXPECT_EQ(253402300799999, dst_data[8]);
        EXPECT_EQ(1651406400000, dst_data[9]);
        EXPECT_EQ(1651410000000, dst_data[10]);
        EXPECT_EQ(1651413600000, dst_data[11]);
        EXPECT_EQ(1651406400000, dst_data[12]);
        EXPECT_EQ(1651410000000, dst_data[13]);
        EXPECT_EQ(1651413600000, dst_data[14]);
    }
}
} // namespace doris::vectorized
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

#include "vec/functions/cast/cast_to_timestamptz.h"

#include <sys/types.h>

#include "cast_test.h"
#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "testutil/datetime_ut_util.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_timestamptz.h"
#include "vec/functions/cast/cast_base.h"
#include "vec/functions/cast/cast_to_date.h"
#include "vec/runtime/timestamptz_value.h"

namespace doris::vectorized {
using namespace ut_type;

class CastTimeStampTzTest : public testing::Test {
public:
    void SetUp() override {
        TimezoneUtils::load_offsets_to_cache();
        _state._timezone_obj = cctz::fixed_time_zone(std::chrono::hours(8));
        time_zone = _state._timezone_obj;
        context._state = &_state;
        arguments = {0};
    }
    cctz::time_zone time_zone;
    MockRuntimeState _state;
    FunctionContext context;
    ColumnNumbers arguments;
    uint32_t result = 1;
};

TEST_F(CastTimeStampTzTest, from_string_strict_mode_to_timestamptz) {
    CastToImpl<CastModeType::StrictMode, DataTypeString, DataTypeTimeStampTz> cast;

    {
        auto block = ColumnHelper::create_block<DataTypeString>(
                {"2024-06-20 12:12:12+08:00", "2024-06-20 12:12:12-08:00",
                 "2024-06-20 12:12:12+00:00", "2024-06-20 12:12:12"});

        block.insert(
                ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeTimeStampTz>(), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows());

        EXPECT_TRUE(st.ok()) << st.to_string();

        const auto& col_res =
                assert_cast<const ColumnTimeStampTz&>(*block.get_by_position(result).column);

        EXPECT_EQ(TimestampTzValue {col_res.get_element(0)}.to_string(time_zone),
                  "2024-06-20 12:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(1)}.to_string(time_zone),
                  "2024-06-21 04:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(2)}.to_string(time_zone),
                  "2024-06-20 20:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(3)}.to_string(time_zone),
                  "2024-06-20 12:12:12.000000+08:00");
    }
    // error cast

    {
        auto block = ColumnHelper::create_block<DataTypeString>(
                {"2024-06-20 12:12:12+08:00", "2024-06-20 12:12:12-08:00",
                 "2024-06-20 12:12:12+00:00", "2024-06-20 25:12:12"});

        block.insert(
                ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeTimeStampTz>(), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows());

        EXPECT_FALSE(st.ok()) << st.to_string();
    }
}

TEST_F(CastTimeStampTzTest, from_string_non_strict_mode_to_timestamptz) {
    CastToImpl<CastModeType::NonStrictMode, DataTypeString, DataTypeTimeStampTz> cast;

    {
        auto block = ColumnHelper::create_block<DataTypeString>(
                {"2024-06-20 12:12:12+08:00", "2024-06-20 12:12:12-08:00",
                 "2024-06-20 12:12:12+00:00", "2024-06-20 12:12:12"});

        block.insert(
                ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeTimeStampTz>(), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows());

        EXPECT_TRUE(st.ok()) << st.to_string();

        const auto& column_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);

        const auto& col_res =
                assert_cast<const ColumnTimeStampTz&>(column_nullable.get_nested_column());

        EXPECT_EQ(TimestampTzValue {col_res.get_element(0)}.to_string(time_zone),
                  "2024-06-20 12:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(1)}.to_string(time_zone),
                  "2024-06-21 04:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(2)}.to_string(time_zone),
                  "2024-06-20 20:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(3)}.to_string(time_zone),
                  "2024-06-20 12:12:12.000000+08:00");
    }

    // error cast
    {
        auto block = ColumnHelper::create_block<DataTypeString>(
                {"2024-06-20 12:12:12+08:00", "2024-06-20 12:12:12-08:00",
                 "2024-06-20 12:12:12+00:00", "2024-06-20 25:12:12"});

        block.insert(ColumnWithTypeAndName {
                nullptr, make_nullable(std::make_shared<DataTypeTimeStampTz>()), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows());

        EXPECT_TRUE(st.ok()) << st.to_string();

        const auto& column_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);

        const auto& col_res =
                assert_cast<const ColumnTimeStampTz&>(column_nullable.get_nested_column());
        const auto& null_map = column_nullable.get_null_map_data();

        EXPECT_EQ(TimestampTzValue {col_res.get_element(0)}.to_string(time_zone),
                  "2024-06-20 12:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(1)}.to_string(time_zone),
                  "2024-06-21 04:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(2)}.to_string(time_zone),
                  "2024-06-20 20:12:12.000000+08:00");
        EXPECT_TRUE(null_map[3]);
    }
}

TEST_F(CastTimeStampTzTest, from_datetime_strict_mode_to_timestamptz) {
    CastToImpl<CastModeType::StrictMode, DataTypeDateTimeV2, DataTypeTimeStampTz> cast;

    {
        auto block = ColumnHelper::create_block<DataTypeDateTimeV2>(
                {make_datetime(2024, 6, 20, 12, 12, 12, 123456),
                 make_datetime(2024, 6, 20, 12, 12, 12, 0), make_datetime(1970, 1, 1, 0, 0, 0, 0),
                 make_datetime(2038, 1, 19, 3, 14, 7, 0)});

        block.insert(
                ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeTimeStampTz>(), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows());

        EXPECT_TRUE(st.ok()) << st.to_string();

        const auto& col_res =
                assert_cast<const ColumnTimeStampTz&>(*block.get_by_position(result).column);

        EXPECT_EQ(TimestampTzValue {col_res.get_element(0)}.to_string(time_zone),
                  "2024-06-20 12:12:12.123456+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(1)}.to_string(time_zone),
                  "2024-06-20 12:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(2)}.to_string(time_zone),
                  "1970-01-01 00:00:00.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(3)}.to_string(time_zone),
                  "2038-01-19 03:14:07.000000+08:00");
    }

    // error cast
    {
        auto block = ColumnHelper::create_block<DataTypeDateTimeV2>(
                {make_datetime(0, 0, 0, 12, 12, 12, 123456),
                 make_datetime(2024, 6, 20, 12, 12, 12, 0), make_datetime(1970, 1, 1, 0, 0, 0, 0),
                 make_datetime(2038, 1, 19, 3, 14, 7, 0)});

        block.insert(
                ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeTimeStampTz>(), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows());

        EXPECT_FALSE(st.ok()) << st.to_string();
    }
}

TEST_F(CastTimeStampTzTest, from_datetime_non_strict_mode_to_timestamptz) {
    CastToImpl<CastModeType::NonStrictMode, DataTypeDateTimeV2, DataTypeTimeStampTz> cast;

    {
        auto block = ColumnHelper::create_block<DataTypeDateTimeV2>(
                {make_datetime(2024, 6, 20, 12, 12, 12, 123456),
                 make_datetime(2024, 6, 20, 12, 12, 12, 0), make_datetime(1970, 1, 1, 0, 0, 0, 0),
                 make_datetime(2038, 1, 19, 3, 14, 7, 0)});

        block.insert(ColumnWithTypeAndName {
                nullptr, make_nullable(std::make_shared<DataTypeTimeStampTz>()), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows(), nullptr);

        EXPECT_TRUE(st.ok()) << st.to_string();

        const auto& column_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);

        const auto& col_res =
                assert_cast<const ColumnTimeStampTz&>(column_nullable.get_nested_column());

        EXPECT_EQ(TimestampTzValue {col_res.get_element(0)}.to_string(time_zone),
                  "2024-06-20 12:12:12.123456+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(1)}.to_string(time_zone),
                  "2024-06-20 12:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(2)}.to_string(time_zone),
                  "1970-01-01 00:00:00.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(3)}.to_string(time_zone),
                  "2038-01-19 03:14:07.000000+08:00");
    }

    //error cast
    {
        auto block = ColumnHelper::create_block<DataTypeDateTimeV2>(
                {make_datetime(0, 0, 0, 12, 12, 12, 123456),
                 make_datetime(2024, 6, 20, 12, 12, 12, 0), make_datetime(1970, 1, 1, 0, 0, 0, 0),
                 make_datetime(2038, 1, 19, 3, 14, 7, 0)});

        block.insert(ColumnWithTypeAndName {
                nullptr, make_nullable(std::make_shared<DataTypeTimeStampTz>()), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows(), nullptr);

        EXPECT_TRUE(st.ok()) << st.to_string();

        const auto& column_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);

        const auto& col_res =
                assert_cast<const ColumnTimeStampTz&>(column_nullable.get_nested_column());
        const auto& null_map = column_nullable.get_null_map_data();

        EXPECT_TRUE(null_map[0]);
        EXPECT_EQ(TimestampTzValue {col_res.get_element(1)}.to_string(time_zone),
                  "2024-06-20 12:12:12.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(2)}.to_string(time_zone),
                  "1970-01-01 00:00:00.000000+08:00");
        EXPECT_EQ(TimestampTzValue {col_res.get_element(3)}.to_string(time_zone),
                  "2038-01-19 03:14:07.000000+08:00");
    }
}

TEST_F(CastTimeStampTzTest, from_timestamptz_strict_mode_to_datetime) {
    CastToImpl<CastModeType::StrictMode, DataTypeTimeStampTz, DataTypeDateTimeV2> cast;

    {
        auto block = ColumnHelper::create_block<DataTypeTimeStampTz>(
                {make_timestamptz(2024, 6, 20, 12, 12, 12, 0),
                 make_timestamptz(2024, 6, 20, 04, 12, 12, 0),
                 make_timestamptz(2024, 6, 20, 20, 12, 12, 0),
                 make_timestamptz(2024, 6, 20, 12, 12, 12, 0)});

        block.insert(
                ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeDateTimeV2>(), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows());

        EXPECT_TRUE(st.ok()) << st.to_string();

        const auto& col_res =
                assert_cast<const ColumnDateTimeV2&>(*block.get_by_position(result).column);

        EXPECT_EQ(col_res.get_element(0), make_datetime(2024, 6, 20, 20, 12, 12, 0));
        EXPECT_EQ(col_res.get_element(1), make_datetime(2024, 6, 20, 12, 12, 12, 0));
        EXPECT_EQ(col_res.get_element(2), make_datetime(2024, 6, 21, 04, 12, 12, 0));
        EXPECT_EQ(col_res.get_element(3), make_datetime(2024, 6, 20, 20, 12, 12, 0));
    }

    // error cast

    {
        auto block = ColumnHelper::create_block<DataTypeTimeStampTz>(
                {make_timestamptz(2024, 6, 20, 12, 12, 12, 0),
                 make_timestamptz(2024, 6, 20, 04, 12, 12, 0),
                 make_timestamptz(2024, 6, 20, 20, 12, 12, 0),
                 make_timestamptz(0, 0, 0, 0, 0, 0, 0)}); // invalid datetime

        block.insert(
                ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeDateTimeV2>(), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows());

        EXPECT_FALSE(st.ok()) << st.to_string();
    }
}

TEST_F(CastTimeStampTzTest, from_timestamptz_non_strict_mode_to_datetime) {
    CastToImpl<CastModeType::NonStrictMode, DataTypeTimeStampTz, DataTypeDateTimeV2> cast;

    {
        auto block = ColumnHelper::create_block<DataTypeTimeStampTz>(
                {make_timestamptz(2024, 6, 20, 12, 12, 12, 0),
                 make_timestamptz(2024, 6, 20, 04, 12, 12, 0),
                 make_timestamptz(2024, 6, 20, 20, 12, 12, 0),
                 make_timestamptz(2024, 6, 20, 12, 12, 12, 0)});

        block.insert(ColumnWithTypeAndName {
                nullptr, make_nullable(std::make_shared<DataTypeDateTimeV2>()), "result"});

        auto st = cast.execute_impl(&context, block, arguments, result, block.rows(), nullptr);

        EXPECT_TRUE(st.ok()) << st.to_string();

        const auto& column_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);

        const auto& col_res =
                assert_cast<const ColumnDateTimeV2&>(column_nullable.get_nested_column());

        EXPECT_EQ(col_res.get_element(0), make_datetime(2024, 6, 20, 20, 12, 12, 0));
        EXPECT_EQ(col_res.get_element(1), make_datetime(2024, 6, 20, 12, 12, 12, 0));
        EXPECT_EQ(col_res.get_element(2), make_datetime(2024, 6, 21, 04, 12, 12, 0));
        EXPECT_EQ(col_res.get_element(3), make_datetime(2024, 6, 20, 20, 12, 12, 0));
    }

    // error cast
    {
        auto block = ColumnHelper::create_block<DataTypeTimeStampTz>(
                {make_timestamptz(2024, 6, 20, 12, 12, 12, 0),
                 make_timestamptz(2024, 6, 20, 04, 12, 12, 0),
                 make_timestamptz(2024, 6, 20, 20, 12, 12, 0),
                 make_timestamptz(0, 0, 0, 0, 0, 0, 0)}); // invalid datetime
        block.insert(ColumnWithTypeAndName {
                nullptr, make_nullable(std::make_shared<DataTypeDateTimeV2>()), "result"});
        auto st = cast.execute_impl(&context, block, arguments, result, block.rows(), nullptr);
        EXPECT_TRUE(st.ok()) << st.to_string();
        const auto& column_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);
        const auto& col_res =
                assert_cast<const ColumnDateTimeV2&>(column_nullable.get_nested_column());
        const auto& null_map = column_nullable.get_null_map_data();
        EXPECT_EQ(col_res.get_element(0), make_datetime(2024, 6, 20, 20, 12, 12, 0));
        EXPECT_EQ(col_res.get_element(1), make_datetime(2024, 6, 20, 12, 12, 12, 0));
        EXPECT_EQ(col_res.get_element(2), make_datetime(2024, 6, 21, 04, 12, 12, 0));
        EXPECT_TRUE(null_map[3]);
    }
}

} // namespace doris::vectorized

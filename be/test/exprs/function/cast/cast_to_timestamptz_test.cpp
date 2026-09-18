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

#include "exprs/function/cast/cast_to_timestamptz.h"

#include <sys/types.h>

#include "core/column/column_nullable.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/data_type_timestamp_ns_serde.h"
#include "core/value/timestamptz_value.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_test.h"
#include "exprs/function/cast/cast_to_date.h"
#include "exprs/function/cast/cast_to_timestamp_ns.h"
#include "exprs/function/cast/cast_wrapper_decls.h"
#include "testutil/column_helper.h"
#include "testutil/datetime_ut_util.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
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

TEST_F(CastTimeStampTzTest, timestamp_ns_and_timestamptz_round_trip) {
    const auto make_timestamp_ns = [](std::string_view text) {
        TimeStampNsValue value;
        EXPECT_TRUE(parse_timestamp_ns(StringRef {text.data(), text.size()}, &value).ok());
        return value;
    };

    auto timestamp_ns_block = ColumnHelper::create_block<DataTypeTimeStampNs>(
            {make_timestamp_ns("1677-09-21 00:12:43.145224192"),
             make_timestamp_ns("1969-12-31 23:59:59.999999499"),
             make_timestamp_ns("1969-12-31 23:59:59.999999500"),
             make_timestamp_ns("2024-06-20 12:12:12.123456789"),
             make_timestamp_ns("2262-04-11 23:47:16.854775807")});
    timestamp_ns_block.insert(
            ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeTimeStampTz>(6), "result"});

    auto to_timestamptz = CastWrapper::create_timestamptz_wrapper(
            &context, timestamp_ns_block.get_by_position(0).type);
    auto status = to_timestamptz(&context, timestamp_ns_block, arguments, result,
                                 timestamp_ns_block.rows(), nullptr);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& nullable_timestamptz =
            assert_cast<const ColumnNullable&>(*timestamp_ns_block.get_by_position(result).column);
    const auto& timestamptz_column =
            assert_cast<const ColumnTimeStampTz&>(nullable_timestamptz.get_nested_column());
    for (size_t i = 0; i < timestamp_ns_block.rows(); ++i) {
        EXPECT_FALSE(nullable_timestamptz.is_null_at(i));
    }
    EXPECT_EQ(TimestampTzValue {timestamptz_column.get_element(0)}.to_string(time_zone),
              "1677-09-21 00:12:43.145224+08:00");
    EXPECT_EQ(TimestampTzValue {timestamptz_column.get_element(1)}.to_string(time_zone),
              "1969-12-31 23:59:59.999999+08:00");
    EXPECT_EQ(TimestampTzValue {timestamptz_column.get_element(2)}.to_string(time_zone),
              "1970-01-01 00:00:00.000000+08:00");
    EXPECT_EQ(TimestampTzValue {timestamptz_column.get_element(3)}.to_string(time_zone),
              "2024-06-20 12:12:12.123457+08:00");
    EXPECT_EQ(TimestampTzValue {timestamptz_column.get_element(4)}.to_string(time_zone),
              "2262-04-11 23:47:16.854776+08:00");

    CastToImpl<CastModeType::StrictMode, DataTypeTimeStampTz, DataTypeTimeStampNs> to_timestamp_ns;
    auto timestamptz_block = ColumnHelper::create_block<DataTypeTimeStampTz>(
            {make_timestamptz(2024, 6, 20, 4, 12, 12, 123456),
             make_timestamptz(1969, 12, 31, 15, 59, 59, 999999)});
    timestamptz_block.get_by_position(0).type = std::make_shared<DataTypeTimeStampTz>(6);
    timestamptz_block.insert(
            ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeTimeStampNs>(), "result"});

    status = to_timestamp_ns.execute_impl(&context, timestamptz_block, arguments, result,
                                          timestamptz_block.rows());
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& timestamp_ns_column = assert_cast<const ColumnTimeStampNs&>(
            *timestamptz_block.get_by_position(result).column);
    EXPECT_EQ(timestamp_ns_column.get_element(0).to_string(), "2024-06-20 12:12:12.123456000");
    EXPECT_EQ(timestamp_ns_column.get_element(1).to_string(), "1969-12-31 23:59:59.999999000");
}

TEST_F(CastTimeStampTzTest, timestamptz_to_timestamp_ns_range_overflow) {
    const auto create_source_block = [] {
        auto block = ColumnHelper::create_block<DataTypeTimeStampTz>(
                {make_timestamptz(1677, 9, 20, 16, 12, 43, 145224),
                 make_timestamptz(1677, 9, 20, 16, 12, 43, 145225),
                 make_timestamptz(2262, 4, 11, 15, 47, 16, 854775),
                 make_timestamptz(2262, 4, 11, 15, 47, 16, 854776)});
        block.get_by_position(0).type = std::make_shared<DataTypeTimeStampTz>(6);
        block.insert(
                ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeTimeStampNs>(), "result"});
        return block;
    };

    {
        CastToImpl<CastModeType::NonStrictMode, DataTypeTimeStampTz, DataTypeTimeStampNs> cast;
        auto block = create_source_block();
        const auto status = cast.execute_impl(&context, block, arguments, result, block.rows());
        ASSERT_TRUE(status.ok()) << status.to_string();

        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);
        const auto& values = assert_cast<const ColumnTimeStampNs&>(nullable.get_nested_column());
        EXPECT_TRUE(nullable.is_null_at(0));
        EXPECT_FALSE(nullable.is_null_at(1));
        EXPECT_FALSE(nullable.is_null_at(2));
        EXPECT_TRUE(nullable.is_null_at(3));
        EXPECT_EQ(values.get_element(1).to_string(), "1677-09-21 00:12:43.145225000");
        EXPECT_EQ(values.get_element(2).to_string(), "2262-04-11 23:47:16.854775000");
    }

    {
        CastToImpl<CastModeType::StrictMode, DataTypeTimeStampTz, DataTypeTimeStampNs> cast;
        auto block = create_source_block();
        const auto status = cast.execute_impl(&context, block, arguments, result, block.rows());
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("can not cast timestamptz"), std::string::npos)
                << status.to_string();
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

} // namespace doris

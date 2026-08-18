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

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamptz.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "testutil/column_helper.h"
#include "testutil/datetime_ut_util.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/timezone_utils.h"

namespace doris {

class FunctionTimezoneHourMinuteTest : public testing::Test {
public:
    void SetUp() override {
        TimezoneUtils::load_offsets_to_cache();
        TimezoneUtils::load_timezones_to_cache();
        context._state = &_state;
        arguments = {0};
        result = 1;
    }

    void set_session_timezone(const cctz::time_zone& tz) { _state._timezone_obj = tz; }

    void check_result(const std::string& func_name, const Block& block,
                      const std::vector<int64_t>& expected) {
        auto return_type = std::make_shared<DataTypeInt64>();
        FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
                func_name, block.get_columns_with_type_and_name(), return_type);
        ASSERT_NE(func, nullptr);
        Block input_block = block;
        input_block.insert({nullptr, return_type, "result"});
        auto st = func->execute(&context, input_block, arguments, result, input_block.rows());
        ASSERT_TRUE(st.ok()) << st.to_string();
        // Constant input may produce a const result column; materialize it
        // before inspecting elements.
        auto result_col = input_block.get_by_position(result).column->convert_to_full_column_if_const();
        const auto& col = assert_cast<const ColumnInt64&>(*result_col);
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_element(i), expected[i]) << "at row " << i;
        }
    }

    MockRuntimeState _state;
    FunctionContext context;
    ColumnNumbers arguments;
    uint32_t result;
};

TEST_F(FunctionTimezoneHourMinuteTest, fixed_offset_shanghai) {
    // Asia/Shanghai has a fixed UTC+08:00 offset without DST, so the offset
    // part of the session timezone is the same for every instant.
    set_session_timezone(cctz::fixed_time_zone(std::chrono::hours(8)));

    auto block = ColumnHelper::create_block<DataTypeTimeStampTz>(
            {make_timestamptz(2024, 1, 15, 12, 0, 0, 0),
             make_timestamptz(2024, 7, 15, 12, 0, 0, 0)});

    check_result("timezone_hour", block, {8, 8});
    check_result("timezone_minute", block, {0, 0});
}

TEST_F(FunctionTimezoneHourMinuteTest, dst_new_york) {
    // America/New_York switches between EST (UTC-05:00) in winter and
    // EDT (UTC-04:00) in summer, which is reflected in the returned offset.
    cctz::time_zone tz;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("America/New_York", tz));
    set_session_timezone(tz);

    auto winter_block = ColumnHelper::create_block<DataTypeTimeStampTz>(
            {make_timestamptz(2024, 1, 15, 12, 0, 0, 0)});
    auto summer_block = ColumnHelper::create_block<DataTypeTimeStampTz>(
            {make_timestamptz(2024, 7, 15, 12, 0, 0, 0)});

    check_result("timezone_hour", winter_block, {-5});
    check_result("timezone_minute", winter_block, {0});
    check_result("timezone_hour", summer_block, {-4});
    check_result("timezone_minute", summer_block, {0});
}

TEST_F(FunctionTimezoneHourMinuteTest, fractional_offsets) {
    // Trino returns truncated integer values for fractional offsets:
    // timezone_hour(UTC-04:30) = -4 and timezone_minute(UTC-04:30) = -30.
    set_session_timezone(cctz::fixed_time_zone(std::chrono::seconds(-4 * 3600 - 30 * 60)));
    auto block = ColumnHelper::create_block<DataTypeTimeStampTz>(
            {make_timestamptz(2024, 6, 20, 12, 0, 0, 0)});
    check_result("timezone_hour", block, {-4});
    check_result("timezone_minute", block, {-30});

    // Nepal Standard Time (UTC+05:45).
    set_session_timezone(cctz::fixed_time_zone(std::chrono::seconds(5 * 3600 + 45 * 60)));
    check_result("timezone_hour", block, {5});
    check_result("timezone_minute", block, {45});
}

TEST_F(FunctionTimezoneHourMinuteTest, const_input) {
    // TIMESTAMPTZ stores a UTC instant without the input zone; even when the
    // value was produced by CAST with an explicit zone (here '2024-01-15
    // 12:00:00-04:30'), the extracted offset is the session zone's offset.
    set_session_timezone(cctz::fixed_time_zone(std::chrono::hours(8)));

    auto inner = ColumnTimeStampTz::create();
    inner->insert_value(make_timestamptz(2024, 1, 15, 16, 30, 0, 0));
    auto const_col = ColumnConst::create(std::move(inner), 3);
    Block block;
    block.insert({std::move(const_col), std::make_shared<DataTypeTimeStampTz>(), "arg"});

    check_result("timezone_hour", block, {8, 8, 8});
    check_result("timezone_minute", block, {0, 0, 0});
}

TEST_F(FunctionTimezoneHourMinuteTest, session_zone_wins_over_input_zone) {
    // The input instant is noon in UTC-04:30, i.e. 16:30 UTC. Trino would
    // return -4/-30 from the input zone; Doris stores only the UTC instant
    // and therefore returns the session zone offset (America/New_York in
    // winter: -5/0).
    cctz::time_zone tz;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("America/New_York", tz));
    set_session_timezone(tz);

    auto block = ColumnHelper::create_block<DataTypeTimeStampTz>(
            {make_timestamptz(2024, 1, 15, 16, 30, 0, 0)});

    check_result("timezone_hour", block, {-5});
    check_result("timezone_minute", block, {0});
}

TEST_F(FunctionTimezoneHourMinuteTest, nullable_input) {
    set_session_timezone(cctz::fixed_time_zone(std::chrono::hours(8)));

    auto nested = ColumnTimeStampTz::create();
    nested->insert_value(make_timestamptz(2024, 1, 15, 12, 0, 0, 0));
    nested->insert_value(make_timestamptz(2024, 1, 15, 12, 0, 0, 0));
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(1);
    auto nullable_col = ColumnNullable::create(std::move(nested), std::move(null_map));
    Block block;
    block.insert({std::move(nullable_col), make_nullable(std::make_shared<DataTypeTimeStampTz>()),
                  "arg"});

    auto return_type = make_nullable(std::make_shared<DataTypeInt64>());
    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            "timezone_hour", block.get_columns_with_type_and_name(), return_type);
    ASSERT_NE(func, nullptr);
    block.insert({nullptr, return_type, "result"});
    auto st = func->execute(&context, block, arguments, result, block.rows());
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto& col = assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);
    const auto& data = assert_cast<const ColumnInt64&>(col.get_nested_column());
    ASSERT_EQ(col.size(), 2);
    EXPECT_EQ(data.get_element(0), 8);
    EXPECT_FALSE(col.is_null_at(0));
    EXPECT_TRUE(col.is_null_at(1));
}

} // namespace doris

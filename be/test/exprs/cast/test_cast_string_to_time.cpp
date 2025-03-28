
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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <limits>
#include <type_traits>
// #include <format>

#include "common/exception.h"
#include "common/expected.h"
#include "olap/olap_common.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/test_util.h"
#include "udf/udf.h"
#include "util/string_parser.hpp"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_time.h"
#include "vec/functions/function_cast.h"
#include "vec/runtime/time_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
class CastStringToTimeTest : public ::testing::Test {
    static void SetUpTestSuite() { TimezoneUtils::load_timezones_to_cache(); }

    void test_cast(FunctionContext& fn_context, int scale, const string& str, bool expect_null,
                   const double& expected, bool expect_throw = false) {
        auto data_type_str = DataTypeFactory::instance().create_data_type(TypeIndex::String);
        auto data_type_time = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_TIMEV2, 0, scale);
        const auto* data_type_time_real_type_ptr =
                assert_cast<const DataTypeTimeV2*>(data_type_time.get());

        Block block;

        auto col_str = ColumnString::create();
        col_str->insert_data(str.data(), str.size());
        block.insert({std::move(col_str), data_type_str, "col_str_cast_to_timev2"});

        auto col_time = data_type_time->create_column();
        block.insert({std::move(col_time), data_type_time, "col_time"});

        ColumnNumbers args {0};
        Status status;
        if (expect_throw) {
            EXPECT_THROW((status = vectorized::StringParsing<DataTypeTimeV2, vectorized::NameCast>::
                                  execute(&fn_context, block, args, 1, block.rows(), true)),
                         Exception);
            return;
        } else {
            status = vectorized::StringParsing<DataTypeTimeV2, vectorized::NameCast>::execute(
                    &fn_context, block, args, 1, block.rows(), false);
        }
        EXPECT_TRUE(status.ok());
        const auto* cast_res_col_nullable =
                assert_cast<const ColumnNullable*>(block.get_by_position(1).column.get());
        EXPECT_EQ(1, cast_res_col_nullable->size());

        const auto* cast_res_col_time = assert_cast<const ColumnFloat64*>(
                cast_res_col_nullable->get_nested_column_ptr().get());

        if (expect_null) {
            if (!cast_res_col_nullable->is_null_at(0)) {
                auto real_value =
                        data_type_time_real_type_ptr->to_string(cast_res_col_time->get_element(0));
                std::cerr << "cast result is not null, but expect null, " << real_value
                          << std::endl;
            }
            EXPECT_TRUE(cast_res_col_nullable->is_null_at(0));
            return;
        }
        EXPECT_FALSE(cast_res_col_nullable->is_null_at(0));
        auto actual_value = cast_res_col_time->get_element(0);
        std::cerr << "cast result: " << std::fixed << actual_value << ", expect: " << std::fixed
                  << expected << std::endl;
        EXPECT_EQ(actual_value, expected);
    }
};

TEST_F(CastStringToTimeTest, normal_cases) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    state._enable_ansi_mode = false;
    FunctionContext fn_context;
    fn_context._state = &state;

    int scale = 6;

    // test hhh:mm:ss.fraction
    {
        double expected =
                TimeValue::make_time(TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                                     TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        std::string time_str =
                fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                            TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, hhh:mm:ss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        time_str =
                fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                            TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, -hhh:mm:ss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        // no delimiter
        time_str = fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, hhmmss.fraction, test string: " << time_str
                  << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, -hhmmss.fraction, test string: " << time_str
                  << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }
    // test hhh:mm:ss.fraction, special values
    {
        int64_t hour = 23, minute = 59, second = 59;
        double expected = TimeValue::make_time(hour, minute, second, 5);
        std::string test_str("23:59:59.000005");
        std::cerr << "-----test cast string to timev2 special value, hhh:mm:ss.fraction, test "
                     "string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        test_str = "-23:59:59.000005";
        std::cerr << "-----test cast string to timev2 special value, -hhh:mm:ss.fraction, test "
                     "string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, minus_expected, false);

        // no delimiter
        test_str = "235959.000005";
        std::cerr
                << "-----test cast string to timev2 special value, hhhmmss.fraction, test string: "
                << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, expected, false);

        // test minus value
        test_str = "-235959.000005";
        std::cerr
                << "-----test cast string to timev2 special value, -hhhmmss.fraction, test string: "
                << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, minus_expected, false);
    }
    // test hhh:mm:ss.fraction, special values
    {
        int64_t hour = 23, minute = 59, second = 59;
        auto expected = TimeValue::make_time(hour, minute, second, 500000);
        std::string test_str = "23:59:59.5";
        std::cerr << "-----test cast string to timev2 special value, hhh:mm:ss.fraction, test "
                     "string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        test_str = "-23:59:59.5";
        std::cerr << "-----test cast string to timev2 special value, -hhh:mm:ss.fraction, test "
                     "string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, minus_expected, false);

        // no delimiter
        test_str = "235959.5";
        std::cerr
                << "-----test cast string to timev2 special value, hhhmmss.fraction, test string: "
                << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, expected, false);

        // test minus value
        test_str = "-235959.5";
        std::cerr
                << "-----test cast string to timev2 special value, -hhhmmss.fraction, test string: "
                << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, minus_expected, false);
    }

    // test hhh:mm:ss.
    {
        double expected =
                TimeValue::make_time(TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                                     TimeValue::MAX_TIME_SECONDS, 0);
        std::string time_str =
                fmt::format("{}:{}:{}.", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                            TimeValue::MAX_TIME_SECONDS);
        std::cerr << "-----test cast string to timev2, hhh:mm:ss., test string: " << time_str
                  << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        time_str = fmt::format("-{}:{}:{}.", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               TimeValue::MAX_TIME_SECONDS);
        std::cerr << "-----test cast string to timev2, -hhh:mm:ss., test string: " << time_str
                  << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        // no delimiter
        time_str = fmt::format("{}{}{}.", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               TimeValue::MAX_TIME_SECONDS);
        std::cerr << "-----test cast string to timev2, hhmmss., test string: " << time_str
                  << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        time_str = fmt::format("-{}{}{}.", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               TimeValue::MAX_TIME_SECONDS);
        std::cerr << "-----test cast string to timev2, -hhmmss., test string: " << time_str
                  << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }
    // test hhh:mm:ss.fraction, extreme long but valid microseconds
    {
        int microsecond = 123456;
        double expected =
                TimeValue::make_time(TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                                     TimeValue::MAX_TIME_SECONDS, microsecond);
        std::string time_str =
                fmt::format("{}:{}:{}.{}{}{}{}{}{}{}{}{}{}", TimeValue::MAX_TIME_HOURS,
                            TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS, microsecond,
                            microsecond, microsecond, microsecond, microsecond, microsecond,
                            microsecond, microsecond, microsecond, microsecond);
        std::cerr << "-----test cast string to timev2 extreme long but valid microseconds, "
                     "hhh:mm:ss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        time_str = fmt::format("-{}:{}:{}.{}{}{}{}{}{}{}{}{}{}", TimeValue::MAX_TIME_HOURS,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               microsecond, microsecond, microsecond, microsecond, microsecond,
                               microsecond, microsecond, microsecond, microsecond, microsecond);
        std::cerr << "-----test cast string to timev2 extreme long but valid microseconds, "
                     "-hhh:mm:ss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        // no delimiter
        time_str = fmt::format("{}{}{}.{}{}{}{}{}{}{}{}{}{}", TimeValue::MAX_TIME_HOURS,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               microsecond, microsecond, microsecond, microsecond, microsecond,
                               microsecond, microsecond, microsecond, microsecond, microsecond);
        std::cerr << "-----test cast string to timev2 extreme long but valid microseconds, "
                     "hhhmmss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        time_str = fmt::format("-{}{}{}.{}{}{}{}{}{}{}{}{}{}", TimeValue::MAX_TIME_HOURS,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               microsecond, microsecond, microsecond, microsecond, microsecond,
                               microsecond, microsecond, microsecond, microsecond, microsecond);
        std::cerr << "-----test cast string to timev2 extreme long but valid microseconds, "
                     "-hhhmmss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }

    // test hhh:mm:ss.fraction, smaller scale
    scale = 2;
    {
        double expected =
                TimeValue::make_time(TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                                     TimeValue::MAX_TIME_SECONDS, 990000);
        auto minus_expected = -expected;

        std::string time_str =
                fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                            TimeValue::MAX_TIME_SECONDS, 99);
        std::cerr
                << "-----test cast string to timev2 small scale, hhh:mm:ss.fraction, test string: "
                << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        time_str = fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS, 99);
        std::cerr
                << "-----test cast string to timev2 small scale, -hhh:mm:ss.fraction, test string: "
                << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        // no delimiter
        time_str = fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               TimeValue::MAX_TIME_SECONDS, 99);
        std::cerr << "-----test cast string to timev2 small scale, hhmmss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               TimeValue::MAX_TIME_SECONDS, 99);
        std::cerr << "-----test cast string to timev2 small scale, -hhmmss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }

    scale = 6;

    // test h:m:s.fraction
    {
        int64_t hour = 1, minute = 2, second = 3;
        double expected = TimeValue::make_time(hour, minute, second, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, h:m:s.fraction, test string: 1:2:3.999999\n";
        test_cast(fn_context, scale, "1:2:3.999999", false, expected, false);

        auto minus_expected = -expected;
        std::cerr
                << "-----test cast string to timev2, -h:m:s.fraction, test string: -1:2:3.999999\n";
        test_cast(fn_context, scale, "-1:2:3.999999", false, minus_expected, false);
    }

    // test "hh:mm.fraction"
    {
        auto expected = TimeValue::make_time(TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                                             0, MAX_MICROSECOND);
        auto time_str = fmt::format("{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                    TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, hh:mm.fraction, test string: " << time_str
                  << "\n";
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        time_str = fmt::format("-{}:{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, -hh:mm.fraction, test string: " << time_str
                  << "\n";
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }
    // test "h:m.fraction"
    {
        int64_t hour = 1, minute = 2;
        double expected = TimeValue::make_time(hour, minute, 0, MAX_MICROSECOND);
        std::string time_str = fmt::format("{}:{}.{}", hour, minute, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, h:m.fraction, test string: " << time_str
                  << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        time_str = fmt::format("-{}:{}.{}", hour, minute, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, -h:m.fraction, test string: " << time_str
                  << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }

    // test mmss.fraction
    {
        auto expected = TimeValue::make_time(0, TimeValue::MAX_TIME_MINUTES,
                                             TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        auto time_str = fmt::format("{}{}.{}", TimeValue::MAX_TIME_MINUTES,
                                    TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, mmss.fraction, test string: " << time_str
                  << "\n";
        test_cast(fn_context, scale, time_str, false, expected, false);

        auto minus_expected = -expected;
        time_str = fmt::format("-{}{}.{}", TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, -mmss.fraction, test string: " << time_str
                  << "\n";
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }

    // "ss.fraction"
    {
        auto expected = TimeValue::make_time(0, 0, TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        auto time_str = fmt::format("{}.{}", TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, ss.fraction, test string: " << time_str
                  << "\n";
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        time_str = fmt::format("-{}.{}", TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2, -ss.fraction, test string: " << time_str
                  << "\n";
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        expected = TimeValue::make_time(0, 0, 0, MAX_MICROSECOND);
        time_str = ".999999";
        std::cerr << "-----test cast string to timev2, ss.fraction, special test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, expected, false);
        // test minus value
        minus_expected = -expected;
        time_str = "-.999999";
        std::cerr << "-----test cast string to timev2, -ss.fraction, special test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }
}

TEST_F(CastStringToTimeTest, overflow) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    state._enable_ansi_mode = false;
    FunctionContext fn_context;
    fn_context._state = &state;

    int scale = 6;
    auto microsecond_need_round = MAX_MICROSECOND * 10 + 5;

    auto test_func = [&](bool strict_mode) {
        // test hhh:mm:ss.fraction, hour overflow
        {
            std::string time_str = fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS + 1,
                                               TimeValue::MAX_TIME_MINUTES,
                                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 hhh:mm:ss.fraction, hour overflow, "
                         "test string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // test minus value
            time_str = fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS + 1,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 -hhh:mm:ss.fraction, hour overflow, "
                         "test string: "
                      << time_str << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // no delimiter
            time_str = fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS + 1,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 hhhmmss.fraction hour overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // test minus value
            time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS + 1,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 -hhhmmss.fraction, hour overflow, "
                         "test string: "
                      << time_str << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hhh:mm:ss.fraction, minute overflow
        {
            std::string time_str = fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                               TimeValue::MAX_TIME_MINUTES + 1,
                                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr
                    << "-----test cast string to timev2 hhh:mm:ss.fraction, minute overflow, test "
                       "string: "
                    << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // test minus value
            time_str = fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES + 1, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr
                    << "-----test cast string to timev2 -hhh:mm:ss.fraction, minute overflow, test "
                       "string: "
                    << time_str << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // no delimiter
            time_str = fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES + 1, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 hhhmmss.fraction minute overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // test minus value
            time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES + 1, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 -hhhmmss.fraction, minute overflow, test "
                         "string: "
                      << time_str << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hhh:mm:ss.fraction, second overflow
        {
            std::string time_str = fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                               TimeValue::MAX_TIME_MINUTES,
                                               TimeValue::MAX_TIME_SECONDS + 1, MAX_MICROSECOND);
            std::cerr
                    << "-----test cast string to timev2 hhh:mm:ss.fraction, second overflow, test "
                       "string: "
                    << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // test minus value
            time_str = fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS + 1,
                                   MAX_MICROSECOND);
            std::cerr
                    << "-----test cast string to timev2 -hhh:mm:ss.fraction, second overflow, test "
                       "string: "
                    << time_str << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // no delimiter
            time_str =
                    fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                                TimeValue::MAX_TIME_SECONDS + 1, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 hhhmmss.fraction second overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // test minus value
            time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS + 1,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 -hhhmmss.fraction, second overflow, test "
                         "string: "
                      << time_str << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hhh:mm:ss.fraction, round and overflow
        {
            std::string time_str = fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                               TimeValue::MAX_TIME_MINUTES,
                                               TimeValue::MAX_TIME_SECONDS, microsecond_need_round);
            std::cerr << "-----test cast string to timev2 hhh:mm:ss.fraction, round microsecond "
                         "overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // test minus value
            time_str = fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   microsecond_need_round);
            std::cerr << "-----test cast string to timev2 -hhh:mm:ss.fraction, round microsecond "
                         "overflow, test "
                         "string: "
                      << time_str << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // no delimiter
            time_str =
                    fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                                TimeValue::MAX_TIME_SECONDS, microsecond_need_round);
            std::cerr << "-----test cast string to timev2 hhhmmss.fraction round microsecond "
                         "overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // test minus value
            time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   microsecond_need_round);
            std::cerr << "-----test cast string to timev2 -hhhmmss.fraction, round microsecond "
                         "overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // smaller scale
            time_str = fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS, 99);
            std::cerr << "-----test cast string to timev2 hhh:mm:ss.fraction, round microsecond "
                         "overflow, "
                         "test string: "
                      << time_str << std::endl;
            test_cast(fn_context, 1, time_str, true, {}, strict_mode);

            // minuse value
            time_str = fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS, 99);
            std::cerr << "-----test cast string to timev2 -hhh:mm:ss.fraction, round microsecond "
                         "overflow, "
                         "test string: "
                      << time_str << std::endl;
            test_cast(fn_context, 1, time_str, true, {}, strict_mode);

            // no delimiter
            time_str = fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS, 99);
            std::cerr << "-----test cast string to timev2 hhhmmss.fraction, round microsecond "
                         "overflow, "
                         "test string: "
                      << time_str << std::endl;
            test_cast(fn_context, 1, time_str, true, {}, strict_mode);

            // minus value
            time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS, 99);
            std::cerr << "-----test cast string to timev2 -hhhmmss.fraction, round microsecond "
                         "overflow, "
                         "test string: "
                      << time_str << std::endl;
            test_cast(fn_context, 1, time_str, true, {}, strict_mode);
        }

        // hh:mm.fraction, hour overflow
        {
            auto time_str = fmt::format("{}:{}.{}", TimeValue::MAX_TIME_HOURS + 1,
                                        TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 hh::mm.fraction hour overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // test minus value
            time_str = fmt::format("-{}:{}.{}", TimeValue::MAX_TIME_HOURS + 1,
                                   TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 -hh::mm.fraction hour overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }

        // hh:mm.fraction, minute overflow
        {
            auto time_str = fmt::format("{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                        TimeValue::MAX_TIME_MINUTES + 1, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 hh::mm.fraction minute overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // test minus value
            time_str = fmt::format("-{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES + 1, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 -hh::mm.fraction minute overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }

        // test mmss.fraction, minute overflow
        {
            auto time_str = fmt::format("{}{}.{}", TimeValue::MAX_TIME_MINUTES + 1,
                                        TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, mmss.fraction, minute overflow, test "
                         "string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}{}.{}", TimeValue::MAX_TIME_MINUTES + 1,
                                   TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, -mmss.fraction, minute overflow, test "
                         "string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test mmss.fraction, second overflow
        {
            auto time_str = fmt::format("{}{}.{}", TimeValue::MAX_TIME_MINUTES,
                                        TimeValue::MAX_TIME_SECONDS + 1, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, mmss.fraction, second overflow, test "
                         "string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}{}.{}", TimeValue::MAX_TIME_MINUTES,
                                   TimeValue::MAX_TIME_SECONDS + 1, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, -mmss.fraction, second overflow, test "
                         "string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
    };
    state._enable_ansi_mode = false;
    test_func(false);
    state._enable_ansi_mode = true;
    test_func(true);
}

TEST_F(CastStringToTimeTest, round) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    state._enable_ansi_mode = false;
    FunctionContext fn_context;
    fn_context._state = &state;

    int scale = 6;
    auto microsecond_no_need_round = MAX_MICROSECOND * 10 + 4;
    auto microsecond_need_round = MAX_MICROSECOND * 10 + 5;

    // test hhh:mm:ss.fraction
    {
        double expected = TimeValue::make_time(TimeValue::MAX_TIME_HOURS, 0, 0, 0);
        double expected_no_round =
                TimeValue::make_time(TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                                     TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
        std::string time_str = fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS - 1,
                                           TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                           microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, hhh:mm:ss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // no round
        time_str =
                fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                            TimeValue::MAX_TIME_SECONDS, microsecond_no_need_round);
        std::cerr << "-----test cast string to timev2 no round, hhh:mm:ss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected_no_round, false);

        // test minus value
        auto minus_expected = -expected;
        time_str = fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS - 1,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, -hhh:mm:ss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        // no round
        auto minus_expected_no_round = -expected_no_round;
        time_str =
                fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                            TimeValue::MAX_TIME_SECONDS, microsecond_no_need_round);
        std::cerr << "-----test cast string to timev2 no round, hhh:mm:ss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected_no_round, false);

        // no delimiter
        time_str =
                fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS - 1, TimeValue::MAX_TIME_MINUTES,
                            TimeValue::MAX_TIME_SECONDS, microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, hhmmss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // no round
        time_str = fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               TimeValue::MAX_TIME_SECONDS, microsecond_no_need_round);
        std::cerr << "-----test cast string to timev2 no round, hhmmss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected_no_round, false);

        // test minus value
        time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS - 1,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, -hhmmss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        // no round
        time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               TimeValue::MAX_TIME_SECONDS, microsecond_no_need_round);
        std::cerr << "-----test cast string to timev2 no round, -hhmmss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected_no_round, false);
    }
    // test hhh:mm:ss.fraction, special values
    {
        int64_t hour = 23, minute = 59, second = 59;
        double expected = TimeValue::make_time(hour, minute, second, 5);
        auto minus_expected = -expected;

        auto round_expected = TimeValue::make_time(hour, minute, second, 6);
        auto round_expected_minus = -round_expected;

        // no round
        std::string test_str("23:59:59.0000054");
        std::cerr << "-----test cast string to timev2 no round, hhh:mm:ms.fraction, test string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, expected, false);

        // no round, minus value
        test_str = "-23:59:59.0000054";
        std::cerr << "-----test cast string to timev2 no round, -hhh:mm:ss.fraction, test string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, minus_expected, false);

        // round
        test_str = "23:59:59.0000055";
        std::cerr << "-----test cast string to timev2 round, hhh:mm:ss.fraction, test string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, round_expected, false);

        // round, minus
        test_str = "-23:59:59.0000055";
        std::cerr << "-----test cast string to timev2 round, -hhh:mm:ss.fraction, test string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, round_expected_minus, false);

        // no delimiter
        // no round
        test_str = "235959.0000054";
        std::cerr << "-----test cast string to timev2 no round, hhhmmms.fraction, test string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, expected, false);

        // no round, minus value
        test_str = "-235959.0000054";
        std::cerr << "-----test cast string to timev2 no round, -hhhmmss.fraction, test string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, minus_expected, false);

        // round
        test_str = "235959.0000055";
        std::cerr << "-----test cast string to timev2 round, hhhmmss.fraction, test string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, round_expected, false);

        // round, minus
        test_str = "-235959.0000055";
        std::cerr << "-----test cast string to timev2 round, -hhhmmss.fraction, test string: "
                  << test_str << "\n";
        test_cast(fn_context, scale, test_str, false, round_expected_minus, false);
    }

    // test hhh:mm:ss.fraction, extreme long but valid microseconds
    {
        double expected = TimeValue::make_time(TimeValue::MAX_TIME_HOURS, 0, 0, 0);
        auto minus_expected = -expected;

        std::string time_str = fmt::format(
                "{}:{}:{}.{}{}{}{}{}{}{}{}{}{}", TimeValue::MAX_TIME_HOURS - 1,
                TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND,
                MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND,
                MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2 round extreme long but valid microseconds, "
                     "hhh:mm:ss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        time_str = fmt::format("-{}:{}:{}.{}{}{}{}{}{}{}{}{}{}", TimeValue::MAX_TIME_HOURS - 1,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND,
                               MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND,
                               MAX_MICROSECOND, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2 round extreme long but valid microseconds, "
                     "-hhh:mm:ss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        // no delimiter
        time_str = fmt::format("{}{}{}.{}{}{}{}{}{}{}{}{}{}", TimeValue::MAX_TIME_HOURS - 1,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND,
                               MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND,
                               MAX_MICROSECOND, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2 round extreme long but valid microseconds, "
                     "hhhmmss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        time_str = fmt::format("-{}{}{}.{}{}{}{}{}{}{}{}{}{}", TimeValue::MAX_TIME_HOURS - 1,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND,
                               MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND, MAX_MICROSECOND,
                               MAX_MICROSECOND, MAX_MICROSECOND);
        std::cerr << "-----test cast string to timev2 round extreme long but valid microseconds, "
                     "-hhhmmss.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }

    // test hhh:mm:ss.fraction, smaller scale
    scale = 2;
    {
        double expected =
                TimeValue::make_time(TimeValue::MAX_TIME_HOURS - 1, TimeValue::MAX_TIME_MINUTES,
                                     TimeValue::MAX_TIME_SECONDS, 990000);
        auto minus_expected = -expected;

        double round_expected = TimeValue::make_time(TimeValue::MAX_TIME_HOURS, 0, 0, 0);
        auto round_minus_expected = -round_expected;

        int fraction_number_no_round = 994;
        int fraction_number_need_round = 995;

        // no round
        std::string time_str = fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS - 1,
                                           TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                           fraction_number_no_round);
        std::cerr << "-----test cast string to timev2 no round small scale, hhh:mm:ss.fraction, "
                     "test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // no round, test minus value
        time_str = fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS - 1,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               fraction_number_no_round);
        std::cerr << "-----test cast string to timev2 no round small scale, -hhh:mm:ss.fraction, "
                     "test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        // no round, no delimiter
        time_str =
                fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS - 1, TimeValue::MAX_TIME_MINUTES,
                            TimeValue::MAX_TIME_SECONDS, fraction_number_no_round);
        std::cerr << "-----test cast string to timev2 no round small scale, hhmmss.fraction, test "
                     "string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // no round, test minus value
        time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS - 1,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               fraction_number_no_round);
        std::cerr << "-----test cast string to timev2 no round small scale, -hhmmss.fraction, test "
                     "string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        ////
        // round
        time_str = fmt::format("{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS - 1,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               fraction_number_need_round);
        std::cerr << "-----test cast string to timev2 round small scale, hhh:mm:ss.fraction, test "
                     "string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, round_expected, false);

        // round, test minus value
        time_str = fmt::format("-{}:{}:{}.{}", TimeValue::MAX_TIME_HOURS - 1,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               fraction_number_need_round);
        std::cerr << "-----test cast string to timev2 round small scale, -hhh:mm:ss.fraction, test "
                     "string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, round_minus_expected, false);

        // round, no delimiter
        time_str =
                fmt::format("{}{}{}.{}", TimeValue::MAX_TIME_HOURS - 1, TimeValue::MAX_TIME_MINUTES,
                            TimeValue::MAX_TIME_SECONDS, fraction_number_need_round);
        std::cerr << "-----test cast string to timev2 round small scale, hhmmss.fraction, test "
                     "string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, round_expected, false);

        // round, test minus value
        time_str = fmt::format("-{}{}{}.{}", TimeValue::MAX_TIME_HOURS - 1,
                               TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               fraction_number_need_round);
        std::cerr << "-----test cast string to timev2 round small scale, -hhmmss.fraction, test "
                     "string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, round_minus_expected, false);
    }

    scale = 6;
    // test h:m:s.fraction
    {
        int64_t hour = 1, minute = 2, second = 3;
        double expected = TimeValue::make_time(hour, minute, second + 1, 0);
        std::cerr << "-----test cast string to timev2 round, h:m:s.fraction, test string: "
                     "1:2:3.9999995\n";
        test_cast(fn_context, scale, "1:2:3.9999995", false, expected, false);

        auto minus_expected = -expected;
        std::cerr << "-----test cast string to timev2 round, -h:m:s.fraction, test string: "
                     "-1:2:3.9999995\n";
        test_cast(fn_context, scale, "-1:2:3.9999995", false, minus_expected, false);
    }

    // test "hh:mm.fraction"
    {
        auto expected =
                TimeValue::make_time(TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES, 1, 0);
        auto time_str = fmt::format("{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                    TimeValue::MAX_TIME_MINUTES, microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, hh:mm.fraction, test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        time_str = fmt::format("-{}:{}.{}", TimeValue::MAX_TIME_HOURS, TimeValue::MAX_TIME_MINUTES,
                               microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, -hh:mm.fraction, test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }
    // test "h:m.fraction"
    {
        int64_t hour = 1, minute = 2;
        double expected = TimeValue::make_time(hour, minute, 1, 0);
        std::string time_str = fmt::format("{}:{}.{}", hour, minute, microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, h:m.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        time_str = fmt::format("-{}:{}.{}", hour, minute, microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, -h:m.fraction, test string: "
                  << time_str << std::endl;
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }

    // test mmss.fraction
    {
        auto expected = TimeValue::make_time(1, 0, 0, 0);
        auto time_str = fmt::format("{}{}.{}", TimeValue::MAX_TIME_MINUTES,
                                    TimeValue::MAX_TIME_SECONDS, microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, mmss.fraction, test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, expected, false);

        auto minus_expected = -expected;
        time_str = fmt::format("-{}{}.{}", TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                               microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, -mmss.fraction, test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }

    // "ss.fraction"
    {
        auto expected = TimeValue::make_time(0, 1, 0, 0);
        auto time_str = fmt::format("{}.{}", TimeValue::MAX_TIME_SECONDS, microsecond_need_round);
        std::cerr << "-----test cast string to timev2, ss.fraction, test string: " << time_str
                  << "\n";
        test_cast(fn_context, scale, time_str, false, expected, false);

        // test minus value
        auto minus_expected = -expected;
        time_str = fmt::format("-{}.{}", TimeValue::MAX_TIME_SECONDS, microsecond_need_round);
        std::cerr << "-----test cast string to timev2 round, -ss.fraction, test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        // no round
        expected = TimeValue::make_time(0, 0, 0, MAX_MICROSECOND);
        time_str = ".9999994";
        std::cerr << "-----test cast string to timev2 no round, ss.fraction, special test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, expected, false);
        // test minus value
        minus_expected = -expected;
        time_str = "-.9999994";
        std::cerr << "-----test cast string to timev2, -ss.fraction, special test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, minus_expected, false);

        // round
        expected = TimeValue::make_time(0, 0, 1, 0);
        time_str = ".9999995";
        std::cerr << "-----test cast string to timev2 no round, ss.fraction, special test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, expected, false);
        // test minus value
        minus_expected = -expected;
        time_str = "-.9999995";
        std::cerr << "-----test cast string to timev2, -ss.fraction, special test string: "
                  << time_str << "\n";
        test_cast(fn_context, scale, time_str, false, minus_expected, false);
    }
}
TEST_F(CastStringToTimeTest, abnormal_cases) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    state._enable_ansi_mode = false;
    FunctionContext fn_context;
    fn_context._state = &state;

    int scale = 6;

    auto test_func = [&](bool strict_mode) {
        // empty string
        test_cast(fn_context, scale, "", true, {}, strict_mode);
        // not number
        test_cast(fn_context, scale, "abc", true, {}, strict_mode);

        // test hhh:mm:ss.fraction, hour not all number
        {
            std::string time_str = fmt::format("{}a:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                               TimeValue::MAX_TIME_MINUTES,
                                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fractionaa, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // minus value
            time_str = fmt::format("-{}a:{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, -hhh:mm:ss.fractionaa, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // no seperator
            time_str = fmt::format("{}a{}{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr
                    << "-----test cast string to timev2 abnormal, hhhmmss.fractionaa, test string: "
                    << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // no seperator, minus value
            time_str = fmt::format("-{}a{}{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, -hhhmmss.fractionaa, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hhh:mm:ss.fraction, minute not all number
        {
            std::string time_str = fmt::format("{}:{}a:{}.{}", TimeValue::MAX_TIME_HOURS,
                                               TimeValue::MAX_TIME_MINUTES,
                                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fractionaa, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // minus value
            time_str = fmt::format("-{}:{}a:{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, -hhh:mm:ss.fractionaa, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hhh:mm:ss.fraction, second not all number
        {
            std::string time_str = fmt::format("{}:{}:{}a.{}", TimeValue::MAX_TIME_HOURS,
                                               TimeValue::MAX_TIME_MINUTES,
                                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fractionaa, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // minus value
            time_str = fmt::format("-{}:{}:{}a.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, -hhh:mm:ss.fractionaa, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hhh:mm:ss.fraction, microsecond not all number
        {
            std::string time_str = fmt::format("{}:{}:{}.{}a", TimeValue::MAX_TIME_HOURS,
                                               TimeValue::MAX_TIME_MINUTES,
                                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fractionaa, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("{}:{}:{}.a{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fractionaa, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // minus value
            time_str = fmt::format("-{}:{}:{}.{}a", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, -hhh:mm:ss.fractionaa, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }

        int64_t int_overflow_value = std::numeric_limits<int>::max() + 1L;
        int64_t int_underflow_value = std::numeric_limits<int>::min() - 1L;
        // test hhh:mm:ss.fraction, hour int overflow
        {
            std::string time_str =
                    fmt::format("{}:{}:{}.{}", int_overflow_value, TimeValue::MAX_TIME_MINUTES,
                                TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fraction,  hour int "
                         "overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
            // no seperator
            time_str = fmt::format("{}.{}", int_overflow_value, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhhmmss.fraction,  hour int "
                         "overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("{}:{}:{}.{}", int_underflow_value, TimeValue::MAX_TIME_MINUTES,
                                   TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fraction, hour int "
                         "underflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
            // no seperator
            time_str = fmt::format("{}.{}", int_underflow_value, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhhmmss.fraction, hour int "
                         "underflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hhh:mm:ss.fraction, minute int overflow
        {
            std::string time_str = fmt::format("{}:{}:{}.{}", 1, int_overflow_value,
                                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fraction,  minute "
                         "int overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
            // minus hour
            time_str = fmt::format("-{}:{}:{}.{}", 1, int_overflow_value,
                                   TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fraction, minute int "
                         "overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("{}:{}:{}.{}", 1, int_underflow_value,
                                   TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fraction, minute int "
                         "underflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}:{}:{}.{}", 1, int_underflow_value,
                                   TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fraction, minute int "
                         "underflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hhh:mm:ss.fraction, second int overflow
        {
            std::string time_str =
                    fmt::format("{}:{}:{}.{}", 1, 1, int_overflow_value, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fraction,  second "
                         "int overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
            // minus hour
            time_str = fmt::format("-{}:{}:{}.{}", 1, 1, int_overflow_value, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fraction, second int "
                         "overflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("{}:{}:{}.{}", 1, 1, int_underflow_value, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fraction, second int "
                         "underflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}:{}:{}.{}", 1, 1, int_underflow_value, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.fraction, second int "
                         "underflow, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }

        // test hhh:mm:ss.fraction, minus minute
        {
            std::string time_str = fmt::format("{}:-{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                               TimeValue::MAX_TIME_MINUTES,
                                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:-mm:ss.fraction, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}:-{}:{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, -hhh:-mm:ss.fraction, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hhh:mm:ss.fraction, minus second
        {
            std::string time_str = fmt::format("{}:{}:-{}.{}", TimeValue::MAX_TIME_HOURS,
                                               TimeValue::MAX_TIME_MINUTES,
                                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:-ss.fraction, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}:{}:-{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, -hhh:mm:-ss.fraction, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hhh:mm:ss.fraction, minus microsecond
        {
            std::string time_str = fmt::format("{}:{}:{}.-{}", TimeValue::MAX_TIME_HOURS,
                                               TimeValue::MAX_TIME_MINUTES,
                                               TimeValue::MAX_TIME_SECONDS, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhh:mm:ss.-fraction, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}:{}:{}.-{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, -hhh:mm:ss.-fraction, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            // no delimiter
            time_str = fmt::format("{}{}{}.-{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, hhhmmss.-fraction, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}{}{}.-{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, TimeValue::MAX_TIME_SECONDS,
                                   MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2 abnormal, -hhhmmss.-fraction, test "
                         "string: "
                      << time_str << std::endl;
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }

        // test hh:mm.fraction, hour not all number
        {
            auto time_str = fmt::format("{}a:{}.{}", TimeValue::MAX_TIME_HOURS,
                                        TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, hha:mm.fraction, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}a:{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, -hha:mm.fraction, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hh:mm.fraction, minute not all number
        {
            auto time_str = fmt::format("{}:{}a.{}", TimeValue::MAX_TIME_HOURS,
                                        TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, hh:mma.fraction, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
            time_str = fmt::format("{}:a{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, hh:amm.fraction, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}:{}a.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, -hha:mm.fraction, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hh:mm.fraction, microsecond not all number
        {
            auto time_str = fmt::format("{}:{}.{}a", TimeValue::MAX_TIME_HOURS,
                                        TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, hh:mm.fractiona, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
            time_str = fmt::format("{}:{}.a{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, hh:mm.afraction, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}:{}.{}a", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, -hh:mm.fractiona, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }

        // test hh:mm.fraction, minus minute
        {
            auto time_str = fmt::format("{}:-{}.{}", TimeValue::MAX_TIME_HOURS,
                                        TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, hh:-mm.fraction, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}:-{}.{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, -hh:-mm.fraction, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
        // test hh:mm.fraction, minus microsecond
        {
            auto time_str = fmt::format("{}:{}.-{}", TimeValue::MAX_TIME_HOURS,
                                        TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, hh:mm.-fraction, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);

            time_str = fmt::format("-{}:{}.-{}", TimeValue::MAX_TIME_HOURS,
                                   TimeValue::MAX_TIME_MINUTES, MAX_MICROSECOND);
            std::cerr << "-----test cast string to timev2, -hh:mm.-fraction, test string: "
                      << time_str << "\n";
            test_cast(fn_context, scale, time_str, true, {}, strict_mode);
        }
    };

    state._enable_ansi_mode = false;
    test_func(false);
    state._enable_ansi_mode = true;
    test_func(true);
}
} // namespace doris::vectorized

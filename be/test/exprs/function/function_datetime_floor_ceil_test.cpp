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

#include <memory>
#include <utility>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamptz.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "testutil/datetime_ut_util.h"
#include "testutil/function_utils.h"

namespace doris {

namespace {

struct TestCase {
    const char* zone;
    const char* function;
    int period = 1;
    TimestampTzValue input;
    TimestampTzValue origin;
    TimestampTzValue expected;
};

void check_round(const TestCase& c) {
    const auto type = std::make_shared<DataTypeTimeStampTz>(6);
    const auto int_type = std::make_shared<DataTypeInt32>();
    SCOPED_TRACE(c.zone);
    auto input = ColumnTimeStampTz::create();
    input->insert_value(c.input);
    auto origin = ColumnTimeStampTz::create();
    origin->insert_value(c.origin);
    auto period = ColumnInt32::create();
    period->insert_value(c.period);
    Block block({{std::move(input), type, "input"},
                 {std::move(period), int_type, "period"},
                 {std::move(origin), type, "origin"},
                 {nullptr, type, "result"}});
    auto function = SimpleFunctionFactory::instance().get_function(
            c.function,
            {block.get_by_position(0), block.get_by_position(1), block.get_by_position(2)}, type);
    ASSERT_NE(function, nullptr);
    FunctionUtils utils(type, {type, int_type, type}, false);
    ASSERT_TRUE(cctz::load_time_zone(c.zone, &utils._state->_timezone_obj));
    auto* context = utils.get_fn_ctx();
    ASSERT_TRUE(function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    ASSERT_TRUE(function->execute(context, block, {0, 1, 2}, 3, 1).ok());
    ASSERT_TRUE(function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    ASSERT_TRUE(function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    const auto& result = assert_cast<const ColumnTimeStampTz&>(*block.get_by_position(3).column);
    EXPECT_EQ(result.get_element(0), c.expected);
}

} // namespace

TEST(TimestampTzFloorTest, SelectNearestGapBoundary) {
    const TestCase cases[] = {
            {.zone = "Australia/Lord_Howe",
             .function = "hour_floor",
             .input = make_timestamptz(2024, 10, 5, 15, 35, 0, 123456),
             .origin = make_timestamptz(2024, 10, 4, 15, 45, 0, 123456),
             .expected = make_timestamptz(2024, 10, 5, 14, 45, 0, 123456)},
            {.zone = "Australia/Lord_Howe",
             .function = "hour_ceil",
             .period = 1,
             .input = make_timestamptz(2024, 10, 5, 15, 35, 0, 123456),
             .origin = make_timestamptz(2024, 10, 4, 15, 45, 0, 123456),
             .expected = make_timestamptz(2024, 10, 5, 15, 45, 0, 123456)},
            {.zone = "Australia/Lord_Howe",
             .function = "minute_floor",
             .period = 20,
             .input = make_timestamptz(2024, 10, 5, 15, 50, 0, 123456),
             .origin = make_timestamptz(2024, 10, 4, 15, 45, 0, 123456),
             .expected = make_timestamptz(2024, 10, 5, 15, 45, 0, 123456)},
            {.zone = "Australia/Lord_Howe",
             .function = "minute_ceil",
             .period = 20,
             .input = make_timestamptz(2024, 10, 5, 15, 29, 0, 123456),
             .origin = make_timestamptz(2024, 10, 4, 15, 45, 0, 123456),
             .expected = make_timestamptz(2024, 10, 5, 15, 35, 0, 123456)},
            {.zone = "Australia/Lord_Howe",
             .function = "hour_ceil",
             .period = 1,
             .input = make_timestamptz(2024, 10, 5, 15, 45, 0, 123456),
             .origin = make_timestamptz(2024, 10, 4, 15, 45, 0, 123456),
             .expected = make_timestamptz(2024, 10, 5, 15, 45, 0, 123456)},
            {.zone = "Australia/Lord_Howe",
             .function = "minute_floor",
             .period = 20,
             .input = make_timestamptz(2024, 10, 5, 15, 45, 0, 123456),
             .origin = make_timestamptz(2024, 10, 4, 15, 45, 0, 123456),
             .expected = make_timestamptz(2024, 10, 5, 15, 45, 0, 123456)},
            {.zone = "Africa/Monrovia",
             .function = "minute_floor",
             .input = make_timestamptz(1972, 1, 7, 0, 44, 40, 123456),
             .origin = make_timestamptz(1972, 1, 6, 0, 44, 30, 0),
             .expected = make_timestamptz(1972, 1, 7, 0, 44, 30, 0)},
            {.zone = "America/New_York",
             .function = "hour_floor",
             .input = make_timestamptz(2024, 11, 3, 6, 35, 0, 123456),
             .origin = make_timestamptz(2024, 11, 2, 4, 0, 0, 0),
             .expected = make_timestamptz(2024, 11, 3, 6, 0, 0, 0)},
    };
    for (const auto& c : cases) {
        check_round(c);
    }
}

TEST(TimestampTzFloorTest, FutureRulesAndYearLimit) {
    const TestCase cases[] = {
            {.zone = "Australia/Lord_Howe",
             .function = "minute_ceil",
             .period = 20,
             .input = make_timestamptz(2824, 10, 5, 15, 29, 0, 123456),
             .origin = make_timestamptz(2824, 10, 4, 15, 45, 0, 123456),
             .expected = make_timestamptz(2824, 10, 5, 15, 35, 0, 123456)},
            {.zone = "Asia/Kathmandu",
             .function = "hour_floor",
             .input = make_timestamptz(2824, 10, 5, 18, 35, 0, 123456),
             .origin = make_timestamptz(2824, 10, 4, 18, 15, 0, 0),
             .expected = make_timestamptz(2824, 10, 5, 18, 15, 0, 0)},
            {.zone = "America/New_York",
             .function = "year_ceil",
             .input = make_timestamptz(9999, 3, 14, 7, 15, 0, 123456),
             .origin = make_timestamptz(9998, 3, 14, 6, 30, 0, 123456),
             .expected = make_timestamptz(9999, 3, 14, 7, 30, 0, 123456)},
            {.zone = "America/New_York",
             .function = "year_ceil",
             .input = make_timestamptz(9999, 3, 14, 7, 30, 0, 123456),
             .origin = make_timestamptz(9998, 3, 14, 6, 30, 0, 123456),
             .expected = make_timestamptz(9999, 3, 14, 7, 30, 0, 123456)},
    };
    for (const auto& c : cases) {
        check_round(c);
    }
}

} // namespace doris

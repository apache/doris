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
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
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

void check_round(const TestCase& c, int constants, size_t rows) {
    const auto type = std::make_shared<DataTypeTimeStampTz>(6);
    const auto int_type = std::make_shared<DataTypeInt32>();
    SCOPED_TRACE(c.zone);
    auto input = ColumnTimeStampTz::create();
    auto origin = ColumnTimeStampTz::create();
    auto period = ColumnInt32::create();
    for (size_t i = 0; i < rows; ++i) {
        input->insert_value(c.input);
        origin->insert_value(c.origin);
        period->insert_value(c.period);
    }
    ColumnPtr period_column = std::move(period);
    ColumnPtr origin_column = std::move(origin);
    if (constants & 1) {
        period_column = ColumnConst::create(period_column->clone_resized(1), rows);
    }
    if (constants & 2) {
        origin_column = ColumnConst::create(origin_column->clone_resized(1), rows);
    }
    SCOPED_TRACE(constants);
    SCOPED_TRACE(rows);
    Block block({{std::move(input), type, "input"},
                 {std::move(period_column), int_type, "period"},
                 {std::move(origin_column), type, "origin"},
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
    ASSERT_TRUE(function->execute(context, block, {0, 1, 2}, 3, rows).ok());
    ASSERT_TRUE(function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    ASSERT_TRUE(function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    const auto& result = assert_cast<const ColumnTimeStampTz&>(*block.get_by_position(3).column);
    for (size_t i = 0; i < rows; ++i) {
        EXPECT_EQ(result.get_element(i), c.expected);
    }
}

enum class RoundShape { UNARY, PERIOD, ANCHOR, PERIOD_ANCHOR, VARIABLE };

void round_column(const char* zone, const char* name, const std::vector<TimestampTzValue>& values,
                  int period, TimestampTzValue origin, RoundShape shape, bool nullable,
                  ColumnPtr& output) {
    DataTypePtr type = std::make_shared<DataTypeTimeStampTz>(6);
    auto input = ColumnTimeStampTz::create();
    auto periods = ColumnInt32::create();
    auto origins = ColumnTimeStampTz::create();
    for (const auto& value : values) {
        input->insert_value(value);
        periods->insert_value(period);
        origins->insert_value(origin);
    }
    ColumnPtr input_column = std::move(input);
    if (nullable) {
        type = make_nullable(type);
        auto nulls = ColumnUInt8::create(values.size(), 0);
        // Invalid physical payloads under NULL must not enter Block preparation.
        nulls->get_data()[0] = 1;
        input_column = ColumnNullable::create(input_column, std::move(nulls));
    }
    const auto date_type = std::make_shared<DataTypeTimeStampTz>(6);
    const auto int_type = std::make_shared<DataTypeInt32>();
    Block block({{input_column, type, "input"}});
    ColumnNumbers arguments {0};
    DataTypes argument_types {type};
    if (shape != RoundShape::UNARY && shape != RoundShape::ANCHOR) {
        ColumnPtr column = std::move(periods);
        if (shape != RoundShape::VARIABLE) {
            column = ColumnConst::create(column->clone_resized(1), values.size());
        }
        block.insert({column, int_type, "period"});
        arguments.push_back(block.columns() - 1);
        argument_types.push_back(int_type);
    }
    if (shape == RoundShape::ANCHOR || shape == RoundShape::PERIOD_ANCHOR ||
        shape == RoundShape::VARIABLE) {
        ColumnPtr column = std::move(origins);
        if (shape != RoundShape::VARIABLE) {
            column = ColumnConst::create(column->clone_resized(1), values.size());
        }
        block.insert({column, date_type, "origin"});
        arguments.push_back(block.columns() - 1);
        argument_types.push_back(date_type);
    }
    auto function = SimpleFunctionFactory::instance().get_function(
            name, block.get_columns_with_type_and_name(), type);
    ASSERT_NE(function, nullptr);
    FunctionUtils utils(type, argument_types, false);
    ASSERT_TRUE(cctz::load_time_zone(zone, &utils._state->_timezone_obj));
    auto* context = utils.get_fn_ctx();
    ASSERT_TRUE(function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    const auto result = block.columns();
    block.insert({nullptr, type, "result"});
    ASSERT_TRUE(function->execute(context, block, arguments, result, values.size()).ok());
    ASSERT_TRUE(function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    ASSERT_TRUE(function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    output = block.get_by_position(result).column;
}

void compare_round_columns(const char* zone, const char* name,
                           const std::vector<TimestampTzValue>& values, int period,
                           TimestampTzValue origin, bool nullable = false) {
    SCOPED_TRACE(zone);
    SCOPED_TRACE(name);
    SCOPED_TRACE(period);
    ColumnPtr expected;
    round_column(zone, name, values, period, origin, RoundShape::VARIABLE, nullable, expected);
    ASSERT_NE(expected.get(), nullptr);
    for (const auto shape :
         {RoundShape::UNARY, RoundShape::PERIOD, RoundShape::ANCHOR, RoundShape::PERIOD_ANCHOR}) {
        if ((shape == RoundShape::UNARY || shape == RoundShape::ANCHOR) && period != 1) {
            continue;
        }
        if ((shape == RoundShape::UNARY || shape == RoundShape::PERIOD) &&
            origin != TimestampTzValue::FIRST_DAY) {
            continue;
        }
        ColumnPtr actual;
        round_column(zone, name, values, period, origin, shape, nullable, actual);
        ASSERT_NE(actual.get(), nullptr);
        for (size_t row = 0; row < values.size(); ++row) {
            SCOPED_TRACE(row);
            EXPECT_EQ(actual->compare_at(row, row, *expected, 1), 0);
        }
    }
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
            {.zone = "Africa/Cairo",
             .function = "month_floor",
             .input = make_timestamptz(2010, 4, 29, 22, 15, 0, 0),
             .origin = make_timestamptz(2010, 1, 30, 22, 0, 0, 0),
             .expected = make_timestamptz(2010, 3, 30, 22, 0, 0, 0)},
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
        for (int constants = 0; constants < 4; ++constants) {
            check_round(c, constants, 1);
            check_round(c, constants, 2);
        }
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
        for (int constants = 0; constants < 4; ++constants) {
            check_round(c, constants, 1);
            check_round(c, constants, 2);
        }
    }
}

TEST(TimestampTzFloorTest, ConstantGridMatchesVariableGrid) {
    const auto first = TimestampTzValue::FIRST_DAY;
    std::vector<TimestampTzValue> dates;
    for (int year : {1900, 2024, 2824}) {
        for (int month : {1, 3, 7, 10, 11}) {
            dates.push_back(make_timestamptz(year, month, 1, 12, 34, 56, 123456));
        }
    }
    for (const char* zone : {"America/New_York", "Australia/Lord_Howe", "UTC", "Etc/GMT-5"}) {
        for (const char* name : {"year_floor", "quarter_floor", "month_floor", "week_floor",
                                 "day_floor", "hour_floor", "minute_floor"}) {
            for (int period : {1, 100, 10000}) {
                compare_round_columns(zone, name, dates, period, first);
            }
        }
        for (const char* name : {"year_ceil", "quarter_ceil", "month_ceil", "week_ceil", "day_ceil",
                                 "hour_ceil", "minute_ceil"}) {
            for (int period : {1, 100}) {
                compare_round_columns(zone, name, dates, period, first);
            }
        }
    }
}

TEST(TimestampTzFloorTest, ConstantGridGapAndOverlapBlocks) {
    // A single Block spans both sides of the gap, both overlap offsets, and disjoint years.
    const std::vector<TimestampTzValue> dates {make_timestamptz(2024, 3, 10, 6, 59, 59, 999999),
                                               make_timestamptz(2024, 3, 10, 7, 15, 0, 123456),
                                               make_timestamptz(2024, 3, 10, 7, 45, 0, 123456),
                                               make_timestamptz(2024, 10, 5, 15, 29, 0, 123456),
                                               make_timestamptz(2024, 10, 5, 15, 35, 0, 123456),
                                               make_timestamptz(2024, 10, 5, 15, 45, 0, 123456),
                                               make_timestamptz(2024, 10, 5, 15, 50, 0, 123456),
                                               make_timestamptz(2024, 11, 3, 5, 35, 0, 123456),
                                               make_timestamptz(2024, 11, 3, 6, 35, 0, 123456),
                                               make_timestamptz(2824, 10, 5, 15, 29, 0, 123456),
                                               make_timestamptz(2824, 10, 5, 15, 50, 0, 123456)};
    for (const char* zone : {"America/New_York", "Australia/Lord_Howe"}) {
        for (const char* name : {"minute_floor", "minute_ceil", "hour_floor", "hour_ceil"}) {
            for (int period : {1, 20, 10000}) {
                compare_round_columns(zone, name, dates, period,
                                      make_timestamptz(2024, 10, 4, 15, 45, 0, 123456));
            }
        }
    }
    const std::vector<TimestampTzValue> year_limit {
            make_timestamptz(9999, 3, 14, 7, 15, 0, 123456),
            make_timestamptz(9999, 3, 14, 7, 30, 0, 123456)};
    compare_round_columns("America/New_York", "year_ceil", year_limit, 1,
                          make_timestamptz(9998, 3, 14, 6, 30, 0, 123456));
}

TEST(TimestampTzFloorTest, ConstantGridSkipsNullPayloads) {
    const std::vector<TimestampTzValue> dates {TimestampTzValue(),
                                               make_timestamptz(2024, 7, 1, 12, 34, 56, 123456),
                                               make_timestamptz(2024, 1, 1, 5, 0, 0, 0)};
    for (const char* zone : {"America/New_York", "UTC"}) {
        compare_round_columns(zone, "year_floor", dates, 1, TimestampTzValue::FIRST_DAY, true);
        compare_round_columns(zone, "year_floor", dates, 10000, TimestampTzValue::FIRST_DAY, true);
    }
}

} // namespace doris

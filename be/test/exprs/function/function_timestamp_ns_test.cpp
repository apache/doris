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

#include <limits>

#include "exprs/function/function_test_util.h"
#include "util/timezone_utils.h"

namespace doris {
using namespace ut_type;

TEST(TimestampNsFunctionTest, calendar_extract_and_format) {
    TimezoneUtils::load_timezones_to_cache();

    const InputTypeSet one_argument = {{PrimitiveType::TYPE_TIMESTAMP_NS}};

    EXPECT_TRUE((check_function<DataTypeInt16, true>(
                         "year", one_argument,
                         {{{std::string("1677-09-21 00:12:43.145224192")}, int16_t(1677)},
                          {{std::string("1970-01-01 00:00:00.000000000")}, int16_t(1970)},
                          {{std::string("2262-04-11 23:47:16.854775807")}, int16_t(2262)}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeInt32, true>(
                         "to_days", one_argument,
                         {{{std::string("1677-09-21 00:12:43.145224192")}, int32_t(612776)},
                          {{std::string("1970-01-01 00:00:00.000000000")}, int32_t(719528)},
                          {{std::string("2262-04-11 23:47:16.854775807")}, int32_t(826279)}})
                         .ok()));

    const InputTypeSet format_arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                           Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE(
            (check_function<DataTypeString, true>("date_format", format_arguments,
                                                  {{{std::string("2024-02-29 12:34:56.123456789"),
                                                     std::string("%Y-%m-%d %H:%i:%s.%f")},
                                                    std::string("2024-02-29 12:34:56.123456")}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeString, true>("date_format", format_arguments,
                                                  {{{std::string("2024-02-29 12:34:56.123456789"),
                                                     std::string("%Y-%m-%d %H:%i:%s.%n")},
                                                    std::string("2024-02-29 12:34:56.123456789")}})
                     .ok()));
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "date_format", format_arguments,
                         {{{std::string("1970-01-01 00:00:00.000000001"), std::string("%n")},
                           std::string("000000001")}})
                         .ok()));
    EXPECT_TRUE((
            check_function<DataTypeString, true>(
                    "time_format", format_arguments,
                    {{{std::string("2024-02-29 12:34:56.123456789"), std::string("%H:%i:%s.%f|%n")},
                      std::string("12:34:56.123456|123456789")}})
                    .ok()));

    const InputTypeSet datetimev2_format_arguments = {{PrimitiveType::TYPE_DATETIMEV2, 6},
                                                      Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "date_format", datetimev2_format_arguments,
                         {{{std::string("2024-02-29 12:34:56.123456"), std::string("%f|%n")},
                           std::string("123456|123456000")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "time_format", datetimev2_format_arguments,
                         {{{std::string("2024-02-29 12:34:56.123456"), std::string("%f|%n")},
                           std::string("123456|123456000")}})
                         .ok()));

    const InputTypeSet timev2_format_arguments = {{PrimitiveType::TYPE_TIMEV2, 6},
                                                  Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "time_format", timev2_format_arguments,
                         {{{std::string("12:34:56.123456"), std::string("%f|%n")},
                           std::string("123456|123456000")}})
                         .ok()));

    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "year_month", one_argument,
                         {{{std::string("2024-02-29 12:34:56.123456789")}, std::string("2024-02")}})
                         .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeString, true>("day_microsecond", one_argument,
                                                  {{{std::string("2024-02-29 12:34:56.123456789")},
                                                    std::string("29 12:34:56.123456")}})
                     .ok()));
    EXPECT_TRUE((check_function<DataTypeInt64, true>(
                         "second_timestamp", one_argument,
                         {{{std::string("1970-01-01 00:00:00.999999999")}, int64_t(-28800)}})
                         .ok()));
}

TEST(TimestampNsFunctionTest, nanosecond_extract) {
    const InputTypeSet one_argument = {{PrimitiveType::TYPE_TIMESTAMP_NS}};

    EXPECT_TRUE((check_function<DataTypeInt32, true>(
                         "nanosecond", one_argument,
                         {{{std::string("1677-09-21 00:12:43.145224192")}, int32_t(145224192)},
                          {{std::string("1969-12-31 23:59:59.999999999")}, int32_t(999999999)},
                          {{std::string("1970-01-01 00:00:00.000000000")}, int32_t(0)},
                          {{std::string("2262-04-11 23:47:16.854775807")}, int32_t(854775807)}})
                         .ok()));
}

TEST(TimestampNsFunctionTest, additional_calendar_functions) {
    const InputTypeSet one_argument = {{PrimitiveType::TYPE_TIMESTAMP_NS}};
    EXPECT_TRUE((check_function<DataTypeInt16, true>(
                         "year_of_week", one_argument,
                         {{{std::string("1677-09-21 00:12:43.145224192")}, int16_t(1677)},
                          {{std::string("2005-01-01 23:59:59.999999999")}, int16_t(2004)},
                          {{std::string("2008-12-30 00:00:00.000000001")}, int16_t(2009)},
                          {{std::string("2262-04-11 23:47:16.854775807")}, int16_t(2262)}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeInt32, true>(
                         "time_to_sec", one_argument,
                         {{{std::string("1677-09-21 00:12:43.145224192")}, int32_t(763)},
                          {{std::string("2024-02-29 12:34:56.123456789")}, int32_t(45296)},
                          {{std::string("2262-04-11 23:47:16.854775807")}, int32_t(85636)}})
                         .ok()));

    const InputTypeSet months_between_arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                                   {PrimitiveType::TYPE_TIMESTAMP_NS},
                                                   {PrimitiveType::TYPE_BOOLEAN}};
    EXPECT_TRUE((check_function<DataTypeFloat64, true>(
                         "months_between", months_between_arguments,
                         {{{std::string("2024-03-31 23:59:59.999999999"),
                            std::string("2024-02-29 00:00:00.000000001"), uint8_t(1)},
                           double(1)},
                          {{std::string("2024-02-29 12:34:56.123456789"),
                            std::string("2024-02-29 00:00:00.000000001"), uint8_t(0)},
                           double(0)}})
                         .ok()));

    const InputTypeSet relative_day_arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                                 {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE((check_function<DataTypeDateV2, true>(
                         "next_day", relative_day_arguments,
                         {{{std::string("2024-02-29 12:34:56.123456789"), std::string("MON")},
                           std::string("2024-03-04")},
                          {{std::string("2262-04-11 23:47:16.854775807"), std::string("MON")},
                           std::string("2262-04-14")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeDateV2, true>(
                         "previous_day", relative_day_arguments,
                         {{{std::string("1677-09-21 00:12:43.145224192"), std::string("MON")},
                           std::string("1677-09-20")},
                          {{std::string("2024-02-29 12:34:56.123456789"), std::string("MON")},
                           std::string("2024-02-26")}})
                         .ok()));
}

TEST(TimestampNsFunctionTest, datetimev2_and_timev2_format_nanosecond_scales) {
    const InputTypeSet datetimev2_scale3_format_arguments = {{PrimitiveType::TYPE_DATETIMEV2, 3},
                                                             Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "date_format", datetimev2_scale3_format_arguments,
                         {{{std::string("2024-02-29 12:34:56.123"), std::string("%n")},
                           std::string("123000000")}})
                         .ok()));

    const InputTypeSet datetimev2_scale0_format_arguments = {{PrimitiveType::TYPE_DATETIMEV2, 0},
                                                             Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "date_format", datetimev2_scale0_format_arguments,
                         {{{std::string("2024-02-29 12:34:56"), std::string("%n")},
                           std::string("000000000")}})
                         .ok()));

    const InputTypeSet timev2_scale3_format_arguments = {{PrimitiveType::TYPE_TIMEV2, 3},
                                                         Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE(
            (check_function<DataTypeString, true>(
                     "time_format", timev2_scale3_format_arguments,
                     {{{std::string("12:34:56.123"), std::string("%n")}, std::string("123000000")}})
                     .ok()));

    const InputTypeSet timev2_scale0_format_arguments = {{PrimitiveType::TYPE_TIMEV2, 0},
                                                         Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "time_format", timev2_scale0_format_arguments,
                         {{{std::string("12:34:56"), std::string("%n")}, std::string("000000000")}})
                         .ok()));
}

TEST(TimestampNsFunctionTest, from_unixtime_formats_nanoseconds) {
    TimezoneUtils::load_timezones_to_cache();

    const InputTypeSet decimal_arguments = {{PrimitiveType::TYPE_DECIMAL128I, 9, 21},
                                            Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE((check_function<DataTypeString, true>("from_unixtime_new", decimal_arguments,
                                                      {{{DECIMAL128V3(1565080737, 123456789, 9),
                                                         std::string("%Y-%m-%d %H:%i:%s.%f")},
                                                        std::string("2019-08-06 16:38:57.123457")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "from_unixtime_new", decimal_arguments,
                         {{{DECIMAL128V3(1565080737, 123456789, 9), std::string("%n")},
                           std::string("123456789")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeString, true>("from_unixtime_new", decimal_arguments,
                                                      {{{DECIMAL128V3(1565080737, 123456499, 9),
                                                         std::string("%Y-%m-%d %H:%i:%s.%f")},
                                                        std::string("2019-08-06 16:38:57.123456")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeString, true>("from_unixtime_new", decimal_arguments,
                                                      {{{DECIMAL128V3(1565080737, 123456500, 9),
                                                         std::string("%Y-%m-%d %H:%i:%s.%f")},
                                                        std::string("2019-08-06 16:38:57.123457")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeString, true>("from_unixtime_new", decimal_arguments,
                                                      {{{DECIMAL128V3(1565080737, 999999500, 9),
                                                         std::string("%Y-%m-%d %H:%i:%s.%f")},
                                                        std::string("2019-08-06 16:38:58.000000")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "from_unixtime_new", decimal_arguments,
                         {{{DECIMAL128V3(0, 999999500, 9), std::string("%s.%f")},
                           std::string("01.000000")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "from_unixtime_new", decimal_arguments,
                         {{{DECIMAL128V3(0, 999999500, 9), std::string("%s.%n")},
                           std::string("00.999999500")}})
                         .ok()));
    EXPECT_FALSE((check_function<DataTypeString, true>(
                          "from_unixtime_new", decimal_arguments,
                          {{{DECIMAL128V3(0, 1, 9), std::string("%f|%n")}, std::string("unused")}},
                          -1, -1, true)
                          .ok()));

    // During a rolling upgrade, an old FE can still send the two-argument DECIMAL64(18,6)
    // signature. Keep that path registered alongside the new nanosecond DECIMAL128 path.
    const InputTypeSet legacy_decimal_arguments = {{PrimitiveType::TYPE_DECIMAL64, 6, 18},
                                                   Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "from_unixtime_new", legacy_decimal_arguments,
                         {{{DECIMAL64(1565080737, 123456, 6), std::string("%f")},
                           std::string("123456")}})
                         .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeString, true>(
                     "from_unixtime_new", legacy_decimal_arguments,
                     {{{DECIMAL64(0, 999999, 6), std::string("%s.%f")}, std::string("00.999999")}})
                     .ok()));
    EXPECT_FALSE((check_function<DataTypeString, true>(
                          "from_unixtime_new", legacy_decimal_arguments,
                          {{{DECIMAL64(1565080737, 123456, 6), std::string("%f|%n")},
                            std::string("unused")}},
                          -1, -1, true)
                          .ok()));

    const InputTypeSet integer_arguments = {PrimitiveType::TYPE_BIGINT,
                                            Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE((check_function<DataTypeString, true>(
                         "from_unixtime_new", integer_arguments,
                         {{{int64_t(0), std::string("%n")}, std::string("000000000")}})
                         .ok()));
    EXPECT_FALSE(
            (check_function<DataTypeString, true>(
                     "from_unixtime_new", integer_arguments,
                     {{{int64_t(0), std::string("%f|%n")}, std::string("unused")}}, -1, -1, true)
                     .ok()));
}

TEST(TimestampNsFunctionTest, datetime_to_timestamp_keeps_datetimev2_overload) {
    TimezoneUtils::load_timezones_to_cache();

    const InputTypeSet datetimev2_arguments = {{PrimitiveType::TYPE_DATETIMEV2, 6}};
    EXPECT_TRUE((check_function<DataTypeInt64, true>(
                         "second_timestamp", datetimev2_arguments,
                         {{{std::string("1970-01-01 08:00:00.999999")}, int64_t(0)}})
                         .ok()));

    const InputTypeSet timestamp_ns_arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS}};
    EXPECT_TRUE((check_function<DataTypeInt64, true>(
                         "second_timestamp", timestamp_ns_arguments,
                         {{{std::string("1970-01-01 08:00:00.999999999")}, int64_t(0)}})
                         .ok()));
}

TEST(TimestampNsFunctionTest, arithmetic_preserves_nanoseconds_and_checks_range) {
    const InputTypeSet subday_arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                           PrimitiveType::TYPE_BIGINT};

    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "seconds_add", subday_arguments,
                         {{{std::string("1969-12-31 23:59:59.999999999"), int64_t(1)},
                           std::string("1970-01-01 00:00:00.999999999")},
                          {{std::string("1970-01-01 00:00:00.000000001"), int64_t(-1)},
                           std::string("1969-12-31 23:59:59.000000001")}})
                         .ok()));

    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "microseconds_add", subday_arguments,
                         {{{std::string("1677-09-21 00:12:43.145224192"), int64_t(1)},
                           std::string("1677-09-21 00:12:43.145225192")},
                          {{std::string("1970-01-01 00:00:00.999999999"), int64_t(1)},
                           std::string("1970-01-01 00:00:01.000000999")}})
                         .ok()));

    const InputTypeSet calendar_arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                             PrimitiveType::TYPE_INT};
    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "months_add", calendar_arguments,
                         {{{std::string("2024-01-31 12:34:56.123456789"), int32_t(1)},
                           std::string("2024-02-29 12:34:56.123456789")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "years_add", calendar_arguments,
                         {{{std::string("2024-02-29 12:34:56.123456789"), int32_t(1)},
                           std::string("2025-02-28 12:34:56.123456789")}})
                         .ok()));

    const InputTypeSet time_arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                         {PrimitiveType::TYPE_TIMEV2, 6}};
    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "add_time", time_arguments,
                         {{{std::string("1969-12-31 23:59:59.999999999"),
                            std::string("00:00:00.000001")},
                           std::string("1970-01-01 00:00:00.000000999")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "sub_time", time_arguments,
                         {{{std::string("1970-01-01 00:00:00.000000001"),
                            std::string("00:00:00.000001")},
                           std::string("1969-12-31 23:59:59.999999001")}})
                         .ok()));

    EXPECT_FALSE((check_function<DataTypeTimeStampNs, true>(
                          "seconds_add", subday_arguments,
                          {{{std::string("2262-04-11 23:47:16.854775807"), int64_t(1)},
                            std::string("unused")}},
                          -1, -1, true)
                          .ok()));
    EXPECT_FALSE((check_function<DataTypeTimeStampNs, true>(
                          "months_add", calendar_arguments,
                          {{{std::string("1677-09-21 00:12:43.145224192"), int32_t(-1)},
                            std::string("unused")}},
                          -1, -1, true)
                          .ok()));
    EXPECT_FALSE((check_function<DataTypeTimeStampNs, true>(
                          "years_add", calendar_arguments,
                          {{{std::string("2262-04-11 23:47:16.854775807"), int32_t(1)},
                            std::string("unused")}},
                          -1, -1, true)
                          .ok()));
}

TEST(TimestampNsFunctionTest, nanosecond_arithmetic) {
    const InputTypeSet arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS}, PrimitiveType::TYPE_BIGINT};

    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "nanoseconds_add", arguments,
                         {{{std::string("1969-12-31 23:59:59.999999999"), int64_t(1)},
                           std::string("1970-01-01 00:00:00.000000000")},
                          {{std::string("1970-01-01 00:00:00.000000001"), int64_t(-2)},
                           std::string("1969-12-31 23:59:59.999999999")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "nanoseconds_sub", arguments,
                         {{{std::string("1970-01-01 00:00:00.000000000"), int64_t(1)},
                           std::string("1969-12-31 23:59:59.999999999")},
                          {{std::string("1969-12-31 23:59:59.999999999"), int64_t(-1)},
                           std::string("1970-01-01 00:00:00.000000000")},
                          {{std::string("1677-09-21 00:12:43.145224192"),
                            std::numeric_limits<int64_t>::min()},
                           std::string("1970-01-01 00:00:00.000000000")}})
                         .ok()));

    EXPECT_FALSE((check_function<DataTypeTimeStampNs, true>(
                          "nanoseconds_add", arguments,
                          {{{std::string("2262-04-11 23:47:16.854775807"), int64_t(1)},
                            std::string("unused")}},
                          -1, -1, true)
                          .ok()));
    EXPECT_FALSE((check_function<DataTypeTimeStampNs, true>(
                          "nanoseconds_sub", arguments,
                          {{{std::string("1677-09-21 00:12:43.145224192"), int64_t(1)},
                            std::string("unused")}},
                          -1, -1, true)
                          .ok()));
}

TEST(TimestampNsFunctionTest, differences_keep_submicrosecond_ordering) {
    const InputTypeSet arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                    {PrimitiveType::TYPE_TIMESTAMP_NS}};

    EXPECT_TRUE(
            (check_function<DataTypeTimeV2, true>("timediff", arguments,
                                                  {{{std::string("1970-01-01 00:00:00.000000000"),
                                                     std::string("1969-12-31 23:59:59.999999999")},
                                                    std::string("0.0")},
                                                   {{std::string("2024-02-29 12:34:56.123456789"),
                                                     std::string("2024-02-29 12:34:55.000000001")},
                                                    std::string("00:00:01.123456")}},
                                                  6)
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("microseconds_diff", arguments,
                                                 {{{std::string("1970-01-01 00:00:00.000001999"),
                                                    std::string("1970-01-01 00:00:00.000000000")},
                                                   int64_t(1)},
                                                  {{std::string("1969-12-31 23:59:59.999999999"),
                                                    std::string("1970-01-01 00:00:00.000000000")},
                                                   int64_t(0)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("months_diff", arguments,
                                                 {{{std::string("2024-03-29 12:34:56.123456789"),
                                                    std::string("2024-02-29 12:34:56.123456789")},
                                                   int64_t(1)},
                                                  {{std::string("2024-02-29 12:34:56.123456789"),
                                                    std::string("2024-03-29 12:34:56.123456789")},
                                                   int64_t(-1)}})
                     .ok()));
}

// Keep the two argument orders and all diff units in one matrix to make semantic drift visible.
// NOLINTNEXTLINE(readability-function-size)
TEST(TimestampNsFunctionTest, mixed_datetimev2_differences_do_not_narrow_datetimev2) {
    const InputTypeSet timestamp_ns_first = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                             {PrimitiveType::TYPE_DATETIMEV2, 6}};
    const InputTypeSet datetimev2_first = {{PrimitiveType::TYPE_DATETIMEV2, 6},
                                           {PrimitiveType::TYPE_TIMESTAMP_NS}};

    EXPECT_TRUE(
            (check_function<DataTypeInt32, true>("datediff", timestamp_ns_first,
                                                 {{{std::string("2024-01-02 00:00:00.000000001"),
                                                    std::string("2024-01-01 23:59:59.999999")},
                                                   int32_t(1)},
                                                  {{std::string("2262-04-11 23:47:16.854775807"),
                                                    std::string("9999-05-11 23:47:16.000000")},
                                                   int32_t(-2825911)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt32, true>("datediff", datetimev2_first,
                                                 {{{std::string("2024-01-01 23:59:59.999999"),
                                                    std::string("2024-01-02 00:00:00.000000001")},
                                                   int32_t(-1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeTimeV2, true>("timediff", timestamp_ns_first,
                                                  {{{std::string("2024-01-02 00:00:00.000001789"),
                                                     std::string("2024-01-02 00:00:00.000000")},
                                                    std::string("00:00:00.000001")}},
                                                  6)
                     .ok()));

    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("years_diff", timestamp_ns_first,
                                                 {{{std::string("2024-01-01 00:00:00.000000001"),
                                                    std::string("2023-01-01 00:00:00.000000")},
                                                   int64_t(1)},
                                                  {{std::string("2262-04-11 23:47:16.854775807"),
                                                    std::string("9999-05-11 23:47:16.000000")},
                                                   int64_t(-7737)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("quarters_diff", timestamp_ns_first,
                                                 {{{std::string("2024-04-01 00:00:00.000000001"),
                                                    std::string("2024-01-01 00:00:00.000000")},
                                                   int64_t(1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("months_diff", timestamp_ns_first,
                                                 {{{std::string("2024-02-01 00:00:00.000000001"),
                                                    std::string("2024-01-01 00:00:00.000000")},
                                                   int64_t(1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("weeks_diff", timestamp_ns_first,
                                                 {{{std::string("2024-01-08 00:00:00.000000001"),
                                                    std::string("2024-01-01 00:00:00.000000")},
                                                   int64_t(1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("days_diff", timestamp_ns_first,
                                                 {{{std::string("2024-01-02 00:00:00.000000001"),
                                                    std::string("2024-01-01 00:00:00.000000")},
                                                   int64_t(1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("hours_diff", timestamp_ns_first,
                                                 {{{std::string("2024-01-01 01:00:00.000000001"),
                                                    std::string("2024-01-01 00:00:00.000000")},
                                                   int64_t(1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("minutes_diff", timestamp_ns_first,
                                                 {{{std::string("2024-01-01 00:01:00.000000001"),
                                                    std::string("2024-01-01 00:00:00.000000")},
                                                   int64_t(1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("seconds_diff", timestamp_ns_first,
                                                 {{{std::string("2024-01-01 00:00:01.000000001"),
                                                    std::string("2024-01-01 00:00:00.000000")},
                                                   int64_t(1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("milliseconds_diff", timestamp_ns_first,
                                                 {{{std::string("2024-01-01 00:00:00.001000001"),
                                                    std::string("2024-01-01 00:00:00.000000")},
                                                   int64_t(1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("microseconds_diff", timestamp_ns_first,
                                                 {{{std::string("2024-01-01 00:00:00.000001001"),
                                                    std::string("2024-01-01 00:00:00.000000")},
                                                   int64_t(1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("nanoseconds_diff", timestamp_ns_first,
                                                 {{{std::string("2024-01-01 00:00:00.000000001"),
                                                    std::string("2024-01-01 00:00:00.000000")},
                                                   int64_t(1)}})
                     .ok()));
    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("nanoseconds_diff", datetimev2_first,
                                                 {{{std::string("2024-01-01 00:00:00.000000"),
                                                    std::string("2024-01-01 00:00:00.000000001")},
                                                   int64_t(-1)}})
                     .ok()));
}

TEST(TimestampNsFunctionTest, nullif_compares_mixed_datetimev2_without_narrowing) {
    const InputTypeSet arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                    {PrimitiveType::TYPE_DATETIMEV2, 6}};
    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "nullif", arguments,
                         {{{std::string("2024-01-01 00:00:00.123456000"),
                            std::string("2024-01-01 00:00:00.123456")},
                           Null()},
                          {{std::string("2262-04-11 23:47:16.854775807"),
                            std::string("9999-05-11 23:47:16.000000")},
                           std::string("2262-04-11 23:47:16.854775807")}})
                         .ok()));
}

TEST(TimestampNsFunctionTest, nanosecond_difference) {
    const InputTypeSet arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                    {PrimitiveType::TYPE_TIMESTAMP_NS}};

    EXPECT_TRUE(
            (check_function<DataTypeInt64, true>("nanoseconds_diff", arguments,
                                                 {{{std::string("1970-01-01 00:00:00.000000001"),
                                                    std::string("1969-12-31 23:59:59.999999999")},
                                                   int64_t(2)},
                                                  {{std::string("1969-12-31 23:59:59.999999999"),
                                                    std::string("1970-01-01 00:00:00.000000001")},
                                                   int64_t(-2)},
                                                  {{std::string("2262-04-11 23:47:16.854775807"),
                                                    std::string("1970-01-01 00:00:00.000000000")},
                                                   std::numeric_limits<int64_t>::max()},
                                                  {{std::string("1677-09-21 00:12:43.145224192"),
                                                    std::string("1970-01-01 00:00:00.000000000")},
                                                   std::numeric_limits<int64_t>::min()}})
                     .ok()));
    EXPECT_FALSE(
            (check_function<DataTypeInt64, true>("nanoseconds_diff", arguments,
                                                 {{{std::string("2262-04-11 23:47:16.854775807"),
                                                    std::string("1677-09-21 00:12:43.145224192")},
                                                   int64_t(0)}},
                                                 -1, -1, true)
                     .ok()));
}

TEST(TimestampNsFunctionTest, convert_tz_preserves_nanoseconds) {
    TimezoneUtils::clear_timezone_caches();
    TimezoneUtils::load_timezones_to_cache();

    const InputTypeSet arguments = {PrimitiveType::TYPE_TIMESTAMP_NS, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_VARCHAR};
    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "convert_tz", arguments,
                         {{{std::string("2024-02-29 12:34:56.123456789"),
                            std::string("Asia/Shanghai"), std::string("UTC")},
                           std::string("2024-02-29 04:34:56.123456789")}})
                         .ok()));
}

TEST(TimestampNsFunctionTest, trunc_floor_and_ceil) {
    const InputTypeSet trunc_arguments = {{PrimitiveType::TYPE_TIMESTAMP_NS},
                                          Consted {PrimitiveType::TYPE_VARCHAR}};
    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "date_trunc", trunc_arguments,
                         {{{std::string("2024-02-29 12:34:56.123456789"), std::string("second")},
                           std::string("2024-02-29 12:34:56.000000000")}})
                         .ok()));

    const InputTypeSet one_argument = {{PrimitiveType::TYPE_TIMESTAMP_NS}};
    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "second_floor", one_argument,
                         {{{std::string("1969-12-31 23:59:59.999999999")},
                           std::string("1969-12-31 23:59:59.000000000")},
                          {{std::string("1970-01-01 00:00:00.000000001")},
                           std::string("1970-01-01 00:00:00.000000000")}})
                         .ok()));
    EXPECT_TRUE((check_function<DataTypeTimeStampNs, true>(
                         "second_ceil", one_argument,
                         {{{std::string("1969-12-31 23:59:59.999999999")},
                           std::string("1970-01-01 00:00:00.000000000")}})
                         .ok()));

    EXPECT_FALSE((check_function<DataTypeTimeStampNs, true>(
                          "date_trunc", trunc_arguments,
                          {{{std::string("1677-09-21 00:12:43.145224192"), std::string("day")},
                            std::string("unused")}},
                          -1, -1, true)
                          .ok()));
    EXPECT_FALSE((check_function<DataTypeTimeStampNs, true>(
                          "second_ceil", one_argument,
                          {{{std::string("2262-04-11 23:47:16.854775807")}, std::string("unused")}},
                          -1, -1, true)
                          .ok()));
}

} // namespace doris

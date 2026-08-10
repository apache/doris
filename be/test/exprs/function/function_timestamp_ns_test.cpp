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

#include "exprs/function/function_test_util.h"

namespace doris {
using namespace ut_type;

TEST(TimestampNsFunctionTest, calendar_extract_and_format) {
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

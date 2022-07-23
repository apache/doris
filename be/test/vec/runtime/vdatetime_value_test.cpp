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

#include "vec/runtime/vdatetime_value.h"

#include <gtest/gtest.h>

#include <any>
#include <string>

namespace doris::vectorized {

TEST(VDateTimeValueTest, date_v2_to_uint32_test) {
    uint16_t year = 2022;
    uint8_t month = 5;
    uint8_t day = 24;

    DateV2Value<DateV2ValueType> date_v2;
    date_v2.set_time(year, month, day, 0, 0, 0, 0);

    EXPECT_TRUE(date_v2.year() == year);
    EXPECT_TRUE(date_v2.month() == month);
    EXPECT_TRUE(date_v2.day() == day);
    EXPECT_TRUE(date_v2.to_date_int_val() == ((year << 9) | (month << 5) | day));
    EXPECT_TRUE(date_v2.hour() == 0);
    EXPECT_TRUE(date_v2.minute() == 0);
    EXPECT_TRUE(date_v2.second() == 0);
}

TEST(VDateTimeValueTest, datetime_v2_to_uint64_test) {
    uint16_t year = 2022;
    uint8_t month = 5;
    uint8_t day = 24;
    uint8_t hour = 23;
    uint8_t minute = 0;
    uint8_t second = 0;
    uint32_t microsecond = 999999;

    DateV2Value<DateTimeV2ValueType> datetime_v2;
    datetime_v2.set_time(year, month, day, hour, minute, second, microsecond);

    EXPECT_TRUE(datetime_v2.year() == year);
    EXPECT_TRUE(datetime_v2.month() == month);
    EXPECT_TRUE(datetime_v2.day() == day);
    EXPECT_TRUE(datetime_v2.to_date_int_val() ==
                (((uint64_t)year << 46) | ((uint64_t)month << 42) | ((uint64_t)day << 37) |
                 ((uint64_t)hour << 32) | ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
                 (uint64_t)microsecond));
    EXPECT_TRUE(datetime_v2.hour() == hour);
    EXPECT_TRUE(datetime_v2.minute() == minute);
    EXPECT_TRUE(datetime_v2.second() == second);
    EXPECT_TRUE(datetime_v2.microsecond() == microsecond);
}

TEST(VDateTimeValueTest, date_v2_from_uint32_test) {
    {
        uint16_t year = 2022;
        uint8_t month = 5;
        uint8_t day = 24;

        DateV2Value<DateV2ValueType> date_v2;
        date_v2.from_date((uint32_t)((year << 9) | (month << 5) | day));

        EXPECT_TRUE(date_v2.year() == year);
        EXPECT_TRUE(date_v2.month() == month);
        EXPECT_TRUE(date_v2.day() == day);
        EXPECT_TRUE(date_v2.to_date_int_val() == ((year << 9) | (month << 5) | day));
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
    }
    {
        uint16_t year = 2022;
        uint8_t month = 5;
        uint8_t day = 24;

        uint32_t ui32 = (uint32_t)((year << 9) | (month << 5) | day);
        auto date_v2 = (DateV2Value<DateV2ValueType>&)ui32;

        EXPECT_TRUE(date_v2.year() == year);
        EXPECT_TRUE(date_v2.month() == month);
        EXPECT_TRUE(date_v2.day() == day);
        EXPECT_TRUE(date_v2.to_date_int_val() == ((year << 9) | (month << 5) | day));
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
    }
}

TEST(VDateTimeValueTest, datetime_v2_from_uint64_test) {
    {
        uint16_t year = 2022;
        uint8_t month = 5;
        uint8_t day = 24;
        uint8_t hour = 23;
        uint8_t minute = 0;
        uint8_t second = 0;
        uint32_t microsecond = 999999;

        DateV2Value<DateTimeV2ValueType> datetime_v2;
        datetime_v2.from_datetime((uint64_t)(((uint64_t)year << 46) | ((uint64_t)month << 42) |
                                             ((uint64_t)day << 37) | ((uint64_t)hour << 32) |
                                             ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
                                             (uint64_t)microsecond));

        EXPECT_TRUE(datetime_v2.year() == year);
        EXPECT_TRUE(datetime_v2.month() == month);
        EXPECT_TRUE(datetime_v2.day() == day);
        EXPECT_TRUE(datetime_v2.to_date_int_val() ==
                    (uint64_t)(((uint64_t)year << 46) | ((uint64_t)month << 42) |
                               ((uint64_t)day << 37) | ((uint64_t)hour << 32) |
                               ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
                               (uint64_t)microsecond));
        EXPECT_TRUE(datetime_v2.hour() == hour);
        EXPECT_TRUE(datetime_v2.minute() == minute);
        EXPECT_TRUE(datetime_v2.second() == second);
        EXPECT_TRUE(datetime_v2.microsecond() == microsecond);
    }
    {
        uint16_t year = 2022;
        uint8_t month = 5;
        uint8_t day = 24;
        uint8_t hour = 12;
        uint8_t minute = 0;
        uint8_t second = 0;
        uint32_t microsecond = 123000;

        DateV2Value<DateTimeV2ValueType> datetime_v2;
        datetime_v2.from_datetime((uint64_t)(((uint64_t)year << 46) | ((uint64_t)month << 42) |
                                             ((uint64_t)day << 37) | ((uint64_t)hour << 32) |
                                             ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
                                             (uint64_t)microsecond));

        EXPECT_TRUE(datetime_v2.year() == year);
        EXPECT_TRUE(datetime_v2.month() == month);
        EXPECT_TRUE(datetime_v2.day() == day);
        EXPECT_TRUE(datetime_v2.to_date_int_val() ==
                    (uint64_t)(((uint64_t)year << 46) | ((uint64_t)month << 42) |
                               ((uint64_t)day << 37) | ((uint64_t)hour << 32) |
                               ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
                               (uint64_t)microsecond));
        EXPECT_TRUE(datetime_v2.hour() == hour);
        EXPECT_TRUE(datetime_v2.minute() == minute);
        EXPECT_TRUE(datetime_v2.second() == second);
        EXPECT_TRUE(datetime_v2.microsecond() == microsecond);
    }
}

TEST(VDateTimeValueTest, date_v2_from_date_format_str_test) {
    uint16_t year = 2022;
    uint8_t month = 5;
    uint8_t day = 24;

    {
        DateV2Value<DateV2ValueType> date_v2;
        std::string origin_date = "2022-05-24";
        std::string date_format = "%Y-%m-%d";
        EXPECT_TRUE(date_v2.from_date_format_str(date_format.data(), date_format.size(),
                                                 origin_date.data(), origin_date.size()));

        EXPECT_TRUE(date_v2.year() == year);
        EXPECT_TRUE(date_v2.month() == month);
        EXPECT_TRUE(date_v2.day() == day);
        EXPECT_TRUE(date_v2.to_date_int_val() == ((year << 9) | (month << 5) | day));
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
    }

    {
        DateV2Value<DateV2ValueType> date_v2;
        std::string origin_date = "2022-05-24 10:10:00";
        std::string date_format = "%Y-%m-%d";
        EXPECT_TRUE(date_v2.from_date_format_str(date_format.data(), date_format.size(),
                                                 origin_date.data(), origin_date.size()));

        EXPECT_TRUE(date_v2.year() == year);
        EXPECT_TRUE(date_v2.month() == month);
        EXPECT_TRUE(date_v2.day() == day);
        EXPECT_TRUE(date_v2.to_date_int_val() == ((year << 9) | (month << 5) | day));
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
    }
}

TEST(VDateTimeValueTest, datetime_v2_from_date_format_str_test) {
    uint16_t year = 2022;
    uint8_t month = 5;
    uint8_t day = 24;

    {
        DateV2Value<DateTimeV2ValueType> datetime_v2;
        std::string origin_date = "2022-05-24 00:00:00";
        std::string date_format = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(datetime_v2.from_date_format_str(date_format.data(), date_format.size(),
                                                     origin_date.data(), origin_date.size()));

        EXPECT_TRUE(datetime_v2.year() == year);
        EXPECT_TRUE(datetime_v2.month() == month);
        EXPECT_TRUE(datetime_v2.day() == day);
        EXPECT_TRUE(datetime_v2.hour() == 0);
        EXPECT_TRUE(datetime_v2.minute() == 0);
        EXPECT_TRUE(datetime_v2.second() == 0);
        EXPECT_TRUE(datetime_v2.microsecond() == 0);
    }

    {
        DateV2Value<DateTimeV2ValueType> datetime_v2;
        std::string origin_date = "2022-05-24 00:00:00";
        std::string date_format = "%Y-%m-%d %H:%i:%s.%f";
        EXPECT_TRUE(datetime_v2.from_date_format_str(date_format.data(), date_format.size(),
                                                     origin_date.data(), origin_date.size()));

        EXPECT_TRUE(datetime_v2.year() == year);
        EXPECT_TRUE(datetime_v2.month() == month);
        EXPECT_TRUE(datetime_v2.day() == day);
        EXPECT_TRUE(datetime_v2.hour() == 0);
        EXPECT_TRUE(datetime_v2.minute() == 0);
        EXPECT_TRUE(datetime_v2.second() == 0);
        EXPECT_TRUE(datetime_v2.microsecond() == 0);
    }

    {
        DateV2Value<DateTimeV2ValueType> datetime_v2;
        std::string origin_date = "2022-05-24 00:00:00.123";
        std::string date_format = "%Y-%m-%d %H:%i:%s.%f";
        EXPECT_TRUE(datetime_v2.from_date_format_str(date_format.data(), date_format.size(),
                                                     origin_date.data(), origin_date.size()));

        EXPECT_TRUE(datetime_v2.year() == year);
        EXPECT_TRUE(datetime_v2.month() == month);
        EXPECT_TRUE(datetime_v2.day() == day);
        EXPECT_TRUE(datetime_v2.hour() == 0);
        EXPECT_TRUE(datetime_v2.minute() == 0);
        EXPECT_TRUE(datetime_v2.second() == 0);
        EXPECT_TRUE(datetime_v2.microsecond() == 123000);
    }

    {
        DateV2Value<DateTimeV2ValueType> datetime_v2;
        std::string origin_date = "2022-05-24 00:00:00.123456";
        std::string date_format = "%Y-%m-%d %H:%i:%s.%f";
        EXPECT_TRUE(datetime_v2.from_date_format_str(date_format.data(), date_format.size(),
                                                     origin_date.data(), origin_date.size()));

        EXPECT_TRUE(datetime_v2.year() == year);
        EXPECT_TRUE(datetime_v2.month() == month);
        EXPECT_TRUE(datetime_v2.day() == day);
        EXPECT_TRUE(datetime_v2.hour() == 0);
        EXPECT_TRUE(datetime_v2.minute() == 0);
        EXPECT_TRUE(datetime_v2.second() == 0);
        EXPECT_TRUE(datetime_v2.microsecond() == 123456);
    }
}

TEST(VDateTimeValueTest, date_diff_test) {
    {
        DateV2Value<DateV2ValueType> date_v2_1;
        std::string origin_date1 = "2022-05-24";
        std::string date_format1 = "%Y-%m-%d";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        DateV2Value<DateV2ValueType> date_v2_2;
        std::string origin_date2 = "2022-06-24";
        std::string date_format2 = "%Y-%m-%d";
        EXPECT_TRUE(date_v2_2.from_date_format_str(date_format2.data(), date_format2.size(),
                                                   origin_date2.data(), origin_date2.size()));

        EXPECT_TRUE(datetime_diff<TimeUnit::DAY>(date_v2_1, date_v2_2) == 31);
        EXPECT_TRUE(datetime_diff<TimeUnit::YEAR>(date_v2_1, date_v2_2) == 0);
        EXPECT_TRUE(datetime_diff<TimeUnit::MONTH>(date_v2_1, date_v2_2) == 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::HOUR>(date_v2_1, date_v2_2) == 31 * 24);
        EXPECT_TRUE(datetime_diff<TimeUnit::MINUTE>(date_v2_1, date_v2_2) == 31 * 24 * 60);
        EXPECT_TRUE(datetime_diff<TimeUnit::SECOND>(date_v2_1, date_v2_2) == 31 * 24 * 60 * 60);
    }

    {
        DateV2Value<DateTimeV2ValueType> date_v2_1;
        std::string origin_date1 = "2022-05-24 01:00:00";
        std::string date_format1 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        DateV2Value<DateTimeV2ValueType> date_v2_2;
        std::string origin_date2 = "2022-06-24 01:00:01";
        std::string date_format2 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_2.from_date_format_str(date_format2.data(), date_format2.size(),
                                                   origin_date2.data(), origin_date2.size()));

        EXPECT_TRUE(datetime_diff<TimeUnit::DAY>(date_v2_1, date_v2_2) == 31);
        EXPECT_TRUE(datetime_diff<TimeUnit::YEAR>(date_v2_1, date_v2_2) == 0);
        EXPECT_TRUE(datetime_diff<TimeUnit::MONTH>(date_v2_1, date_v2_2) == 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::HOUR>(date_v2_1, date_v2_2) == 31 * 24);
        EXPECT_TRUE(datetime_diff<TimeUnit::MINUTE>(date_v2_1, date_v2_2) == 31 * 24 * 60);
        EXPECT_TRUE(datetime_diff<TimeUnit::SECOND>(date_v2_1, date_v2_2) == 31 * 24 * 60 * 60 + 1);
    }

    {
        DateV2Value<DateV2ValueType> date_v2_1;
        std::string origin_date1 = "2022-05-24";
        std::string date_format1 = "%Y-%m-%d";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        DateV2Value<DateTimeV2ValueType> date_v2_2;
        std::string origin_date2 = "2022-06-24 01:00:01";
        std::string date_format2 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_2.from_date_format_str(date_format2.data(), date_format2.size(),
                                                   origin_date2.data(), origin_date2.size()));

        EXPECT_TRUE(datetime_diff<TimeUnit::DAY>(date_v2_1, date_v2_2) == 31);
        EXPECT_TRUE(datetime_diff<TimeUnit::YEAR>(date_v2_1, date_v2_2) == 0);
        EXPECT_TRUE(datetime_diff<TimeUnit::MONTH>(date_v2_1, date_v2_2) == 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::HOUR>(date_v2_1, date_v2_2) == 31 * 24 + 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::MINUTE>(date_v2_1, date_v2_2) == 31 * 24 * 60 + 60);
        EXPECT_TRUE(datetime_diff<TimeUnit::SECOND>(date_v2_1, date_v2_2) ==
                    31 * 24 * 60 * 60 + 3601);
    }

    {
        DateV2Value<DateV2ValueType> date_v2_1;
        std::string origin_date1 = "2022-05-24";
        std::string date_format1 = "%Y-%m-%d";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        VecDateTimeValue date_v2_2;
        std::string origin_date2 = "2022-06-24 00:00:00";
        std::string date_format2 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_2.from_date_format_str(date_format2.data(), date_format2.size(),
                                                   origin_date2.data(), origin_date2.size()));

        EXPECT_TRUE(datetime_diff<TimeUnit::DAY>(date_v2_1, date_v2_2) == 31);
        EXPECT_TRUE(datetime_diff<TimeUnit::YEAR>(date_v2_1, date_v2_2) == 0);
        EXPECT_TRUE(datetime_diff<TimeUnit::MONTH>(date_v2_1, date_v2_2) == 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::HOUR>(date_v2_1, date_v2_2) == 31 * 24);
        EXPECT_TRUE(datetime_diff<TimeUnit::MINUTE>(date_v2_1, date_v2_2) == 31 * 24 * 60);
        EXPECT_TRUE(datetime_diff<TimeUnit::SECOND>(date_v2_1, date_v2_2) == 31 * 24 * 60 * 60);
    }

    {
        DateV2Value<DateTimeV2ValueType> date_v2_1;
        std::string origin_date1 = "2022-05-24 00:00:00.000";
        std::string date_format1 = "%Y-%m-%d %H:%i:%s.%f";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        VecDateTimeValue date_v2_2;
        std::string origin_date2 = "2022-06-24 00:00:00";
        std::string date_format2 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_2.from_date_format_str(date_format2.data(), date_format2.size(),
                                                   origin_date2.data(), origin_date2.size()));

        EXPECT_TRUE(datetime_diff<TimeUnit::DAY>(date_v2_1, date_v2_2) == 31);
        EXPECT_TRUE(datetime_diff<TimeUnit::YEAR>(date_v2_1, date_v2_2) == 0);
        EXPECT_TRUE(datetime_diff<TimeUnit::MONTH>(date_v2_1, date_v2_2) == 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::HOUR>(date_v2_1, date_v2_2) == 31 * 24);
        EXPECT_TRUE(datetime_diff<TimeUnit::MINUTE>(date_v2_1, date_v2_2) == 31 * 24 * 60);
        EXPECT_TRUE(datetime_diff<TimeUnit::SECOND>(date_v2_1, date_v2_2) == 31 * 24 * 60 * 60);
    }

    {
        VecDateTimeValue date_v2_1;
        std::string origin_date1 = "2022-05-24 00:00:00";
        std::string date_format1 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        DateV2Value<DateV2ValueType> date_v2_2;
        std::string origin_date2 = "2022-06-24";
        std::string date_format2 = "%Y-%m-%d";
        EXPECT_TRUE(date_v2_2.from_date_format_str(date_format2.data(), date_format2.size(),
                                                   origin_date2.data(), origin_date2.size()));

        EXPECT_TRUE(datetime_diff<TimeUnit::DAY>(date_v2_1, date_v2_2) == 31);
        EXPECT_TRUE(datetime_diff<TimeUnit::YEAR>(date_v2_1, date_v2_2) == 0);
        EXPECT_TRUE(datetime_diff<TimeUnit::MONTH>(date_v2_1, date_v2_2) == 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::HOUR>(date_v2_1, date_v2_2) == 31 * 24);
        EXPECT_TRUE(datetime_diff<TimeUnit::MINUTE>(date_v2_1, date_v2_2) == 31 * 24 * 60);
        EXPECT_TRUE(datetime_diff<TimeUnit::SECOND>(date_v2_1, date_v2_2) == 31 * 24 * 60 * 60);
    }

    {
        VecDateTimeValue date_v2_1;
        std::string origin_date1 = "2022-05-24 00:00:00";
        std::string date_format1 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        DateV2Value<DateTimeV2ValueType> date_v2_2;
        std::string origin_date2 = "2022-06-24 00:00:00.000000";
        std::string date_format2 = "%Y-%m-%d %H:%i:%s.%f";
        EXPECT_TRUE(date_v2_2.from_date_format_str(date_format2.data(), date_format2.size(),
                                                   origin_date2.data(), origin_date2.size()));

        EXPECT_TRUE(datetime_diff<TimeUnit::DAY>(date_v2_1, date_v2_2) == 31);
        EXPECT_TRUE(datetime_diff<TimeUnit::YEAR>(date_v2_1, date_v2_2) == 0);
        EXPECT_TRUE(datetime_diff<TimeUnit::MONTH>(date_v2_1, date_v2_2) == 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::HOUR>(date_v2_1, date_v2_2) == 31 * 24);
        EXPECT_TRUE(datetime_diff<TimeUnit::MINUTE>(date_v2_1, date_v2_2) == 31 * 24 * 60);
        EXPECT_TRUE(datetime_diff<TimeUnit::SECOND>(date_v2_1, date_v2_2) == 31 * 24 * 60 * 60);
    }

    {
        DateV2Value<DateV2ValueType> date_v2_1;
        std::string origin_date1 = "2022-05-24";
        std::string date_format1 = "%Y-%m-%d";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        VecDateTimeValue date_v2_2;
        std::string origin_date2 = "2022-06-24 06:00:00";
        std::string date_format2 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_2.from_date_format_str(date_format2.data(), date_format2.size(),
                                                   origin_date2.data(), origin_date2.size()));

        EXPECT_TRUE(datetime_diff<TimeUnit::DAY>(date_v2_1, date_v2_2) == 31);
        EXPECT_TRUE(datetime_diff<TimeUnit::YEAR>(date_v2_1, date_v2_2) == 0);
        EXPECT_TRUE(datetime_diff<TimeUnit::MONTH>(date_v2_1, date_v2_2) == 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::HOUR>(date_v2_1, date_v2_2) == 31 * 24 + 6);
        EXPECT_TRUE(datetime_diff<TimeUnit::MINUTE>(date_v2_1, date_v2_2) == (31 * 24 + 6) * 60);
        EXPECT_TRUE(datetime_diff<TimeUnit::SECOND>(date_v2_1, date_v2_2) ==
                    (31 * 24 + 6) * 60 * 60);
    }

    {
        VecDateTimeValue date_v2_1;
        std::string origin_date1 = "2022-05-24";
        std::string date_format1 = "%Y-%m-%d";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        VecDateTimeValue date_v2_2;
        std::string origin_date2 = "2022-06-24 06:00:00";
        std::string date_format2 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_2.from_date_format_str(date_format2.data(), date_format2.size(),
                                                   origin_date2.data(), origin_date2.size()));

        EXPECT_TRUE(datetime_diff<TimeUnit::DAY>(date_v2_1, date_v2_2) == 31);
        EXPECT_TRUE(datetime_diff<TimeUnit::YEAR>(date_v2_1, date_v2_2) == 0);
        EXPECT_TRUE(datetime_diff<TimeUnit::MONTH>(date_v2_1, date_v2_2) == 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::HOUR>(date_v2_1, date_v2_2) == 31 * 24 + 6);
        EXPECT_TRUE(datetime_diff<TimeUnit::MINUTE>(date_v2_1, date_v2_2) == (31 * 24 + 6) * 60);
        EXPECT_TRUE(datetime_diff<TimeUnit::SECOND>(date_v2_1, date_v2_2) ==
                    (31 * 24 + 6) * 60 * 60);
    }

    {
        VecDateTimeValue date_v2_1;
        std::string origin_date1 = "2022-05-24 06:00:00";
        std::string date_format1 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        VecDateTimeValue date_v2_2;
        std::string origin_date2 = "2022-06-24 06:00:00";
        std::string date_format2 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_2.from_date_format_str(date_format2.data(), date_format2.size(),
                                                   origin_date2.data(), origin_date2.size()));

        EXPECT_TRUE(datetime_diff<TimeUnit::DAY>(date_v2_1, date_v2_2) == 31);
        EXPECT_TRUE(datetime_diff<TimeUnit::YEAR>(date_v2_1, date_v2_2) == 0);
        EXPECT_TRUE(datetime_diff<TimeUnit::MONTH>(date_v2_1, date_v2_2) == 1);
        EXPECT_TRUE(datetime_diff<TimeUnit::HOUR>(date_v2_1, date_v2_2) == 31 * 24);
        EXPECT_TRUE(datetime_diff<TimeUnit::MINUTE>(date_v2_1, date_v2_2) == 31 * 24 * 60);
        EXPECT_TRUE(datetime_diff<TimeUnit::SECOND>(date_v2_1, date_v2_2) == 31 * 24 * 60 * 60);
    }
}

} // namespace doris::vectorized

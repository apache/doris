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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest_pred_impl.h"

namespace doris::vectorized {

TEST(VDateTimeValueTest, date_v2_to_uint32_test) {
    uint16_t year = 2022;
    uint8_t month = 5;
    uint8_t day = 24;

    DateV2Value<DateV2ValueType> date_v2;
    date_v2.unchecked_set_time(year, month, day, 0, 0, 0, 0);

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
    datetime_v2.unchecked_set_time(year, month, day, hour, minute, second, microsecond);

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
        date_v2.unchecked_set_time(year, month, day, 0, 0, 0, 0);

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
        datetime_v2.unchecked_set_time(year, month, day, hour, minute, second, microsecond);

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
        auto ui64 = (uint64_t)(((uint64_t)year << 46) | ((uint64_t)month << 42) |
                               ((uint64_t)day << 37) | ((uint64_t)hour << 32) |
                               ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
                               (uint64_t)microsecond);
        datetime_v2 = (DateV2Value<DateTimeV2ValueType>&)ui64;

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

    {
        VecDateTimeValue date_v2_1;
        std::string origin_date1 = "2022-05-24 06:00:00";
        std::string date_format1 = "%Y-%m-%d %H:%i:%s";
        EXPECT_TRUE(date_v2_1.from_date_format_str(date_format1.data(), date_format1.size(),
                                                   origin_date1.data(), origin_date1.size()));

        VecDateTimeValue date_v2_2;
        std::string origin_date2 = "2022-06-24 06:00:00.123 AM";
        std::string date_format2 = "%Y-%m-%d %h:%i:%s.%f %p";
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

TEST(VDateTimeValueTest, date_v2_to_string_test) {
    uint16_t year = 2022;
    uint8_t month = 5;
    uint8_t day = 24;
    uint8_t hour = 23;
    uint8_t minute = 50;
    uint8_t second = 50;
    uint32_t ms = 555000;

    {
        DateV2Value<DateV2ValueType> date_v2;
        date_v2.unchecked_set_time(year, month, day, 0, 0, 0, 0);

        char buf[30];
        int len = date_v2.to_buffer(buf);
        EXPECT_TRUE(std::string(buf, len) == std::string("2022-05-24"));
    }

    {
        DateV2Value<DateTimeV2ValueType> date_v2;
        date_v2.unchecked_set_time(year, month, day, hour, minute, second, ms);

        char buf[30];
        int len = date_v2.to_buffer(buf);
        EXPECT_TRUE(std::string(buf, len) == std::string("2022-05-24 23:50:50.555000"));
    }

    {
        DateV2Value<DateTimeV2ValueType> date_v2;
        date_v2.unchecked_set_time(year, month, day, hour, minute, second, ms);

        char buf[30];
        int len = date_v2.to_buffer(buf, 3);
        EXPECT_TRUE(std::string(buf, len) == std::string("2022-05-24 23:50:50.555"));
    }

    {
        DateV2Value<DateTimeV2ValueType> date_v2;
        date_v2.unchecked_set_time(year, month, day, hour, minute, second, ms);

        char buf[30];
        int len = date_v2.to_buffer(buf, 2);
        EXPECT_TRUE(std::string(buf, len) == std::string("2022-05-24 23:50:50.55"));
    }

    {
        DateV2Value<DateTimeV2ValueType> date_v2;
        date_v2.unchecked_set_time(year, month, day, hour, minute, second, ms);

        char buf[30];
        int len = date_v2.to_buffer(buf, 6);
        EXPECT_TRUE(std::string(buf, len) == std::string("2022-05-24 23:50:50.555000"));
    }

    {
        DateV2Value<DateTimeV2ValueType> date_v2;
        date_v2.unchecked_set_time(year, month, day, hour, minute, second, 0);

        char buf[30];
        int len = date_v2.to_buffer(buf);
        EXPECT_TRUE(std::string(buf, len) == std::string("2022-05-24 23:50:50"));
    }
}

TEST(VDateTimeValueTest, date_v2_daynr_test) {
    {
        DateV2Value<DateV2ValueType> date_v2;
        // 1970/01/01
        EXPECT_TRUE(date_v2.get_date_from_daynr(719528));
        EXPECT_TRUE(date_v2.year() == 1970);
        EXPECT_TRUE(date_v2.month() == 1);
        EXPECT_TRUE(date_v2.day() == 1);
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
        EXPECT_TRUE(date_v2.microsecond() == 0);
        EXPECT_TRUE(doris::calc_daynr(1970, 1, 1) == 719528);
        EXPECT_TRUE(date_day_offset_dict::get().get_dict_init());
        EXPECT_TRUE(date_day_offset_dict::get().can_speed_up_calc_daynr(1970));
        EXPECT_TRUE(date_day_offset_dict::get().can_speed_up_daynr_to_date(719528));
    }

    {
        DateV2Value<DateV2ValueType> date_v2;
        // 1969/12/31
        EXPECT_TRUE(date_v2.get_date_from_daynr(719527));
        EXPECT_TRUE(date_v2.year() == 1969);
        EXPECT_TRUE(date_v2.month() == 12);
        EXPECT_TRUE(date_v2.day() == 31);
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
        EXPECT_TRUE(date_v2.microsecond() == 0);
        EXPECT_TRUE(doris::calc_daynr(1969, 12, 31) == 719527);
        EXPECT_TRUE(date_day_offset_dict::get().get_dict_init());
        EXPECT_TRUE(date_day_offset_dict::get().can_speed_up_calc_daynr(1969));
        EXPECT_TRUE(date_day_offset_dict::get().can_speed_up_daynr_to_date(719527));
    }

    {
        DateV2Value<DateV2ValueType> date_v2;
        // 1900/01/01
        EXPECT_TRUE(date_v2.get_date_from_daynr(693961));
        EXPECT_TRUE(date_v2.year() == 1900);
        EXPECT_TRUE(date_v2.month() == 1);
        EXPECT_TRUE(date_v2.day() == 1);
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
        EXPECT_TRUE(date_v2.microsecond() == 0);
        EXPECT_TRUE(doris::calc_daynr(1900, 1, 1) == 693961);
        EXPECT_TRUE(date_day_offset_dict::get().get_dict_init());
        EXPECT_TRUE(date_day_offset_dict::get().can_speed_up_calc_daynr(1900));
        EXPECT_TRUE(date_day_offset_dict::get().can_speed_up_daynr_to_date(693961));
    }

    {
        DateV2Value<DateV2ValueType> date_v2;
        // 1899/12/31
        EXPECT_TRUE(date_v2.get_date_from_daynr(693960));
        EXPECT_TRUE(date_v2.year() == 1899);
        EXPECT_TRUE(date_v2.month() == 12);
        EXPECT_TRUE(date_v2.day() == 31);
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
        EXPECT_TRUE(date_v2.microsecond() == 0);
        EXPECT_TRUE(doris::calc_daynr(1899, 12, 31) == 693960);
        EXPECT_TRUE(date_day_offset_dict::get().get_dict_init());
        EXPECT_FALSE(date_day_offset_dict::get().can_speed_up_calc_daynr(1899));
        EXPECT_FALSE(date_day_offset_dict::get().can_speed_up_daynr_to_date(693960));
    }

    {
        DateV2Value<DateV2ValueType> date_v2;
        // 2039/12/31
        EXPECT_TRUE(date_v2.get_date_from_daynr(745094));
        EXPECT_TRUE(date_v2.year() == 2039);
        EXPECT_TRUE(date_v2.month() == 12);
        EXPECT_TRUE(date_v2.day() == 31);
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
        EXPECT_TRUE(date_v2.microsecond() == 0);
        EXPECT_TRUE(doris::calc_daynr(2039, 12, 31) == 745094);
        EXPECT_TRUE(date_day_offset_dict::get().get_dict_init());
        EXPECT_TRUE(date_day_offset_dict::get().can_speed_up_calc_daynr(2039));
        EXPECT_TRUE(date_day_offset_dict::get().can_speed_up_daynr_to_date(745094));
    }

    {
        DateV2Value<DateV2ValueType> date_v2;
        // 2040/01/01
        EXPECT_TRUE(date_v2.get_date_from_daynr(745095));
        EXPECT_TRUE(date_v2.year() == 2040);
        EXPECT_TRUE(date_v2.month() == 1);
        EXPECT_TRUE(date_v2.day() == 1);
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
        EXPECT_TRUE(date_v2.microsecond() == 0);
        EXPECT_TRUE(doris::calc_daynr(2040, 01, 01) == 745095);
        EXPECT_TRUE(date_day_offset_dict::get().get_dict_init());
        EXPECT_FALSE(date_day_offset_dict::get().can_speed_up_calc_daynr(2040));
        EXPECT_FALSE(date_day_offset_dict::get().can_speed_up_daynr_to_date(745095));
    }

    {
        DateV2Value<DateV2ValueType> date_v2;
        // 0000/01/01
        EXPECT_TRUE(date_v2.get_date_from_daynr(1));
        EXPECT_TRUE(date_v2.year() == 0);
        EXPECT_TRUE(date_v2.month() == 1);
        EXPECT_TRUE(date_v2.day() == 1);
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
        EXPECT_TRUE(date_v2.microsecond() == 0);
        EXPECT_TRUE(doris::calc_daynr(0, 01, 01) == 1);
        EXPECT_TRUE(date_day_offset_dict::get().get_dict_init());
        EXPECT_FALSE(date_day_offset_dict::get().can_speed_up_calc_daynr(0));
        EXPECT_FALSE(date_day_offset_dict::get().can_speed_up_daynr_to_date(1));
    }

    {
        DateV2Value<DateV2ValueType> date_v2;
        // Invalid date 0000/00/01
        EXPECT_TRUE(date_v2.year() == 0);
        EXPECT_TRUE(date_v2.month() == 0);
        EXPECT_TRUE(date_v2.day() == 0);
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
        EXPECT_TRUE(date_v2.microsecond() == 0);
        EXPECT_TRUE(doris::calc_daynr(0, 0, 1) == 0);
    }

    {
        DateV2Value<DateV2ValueType> date_v2;
        // 9999/12/31
        EXPECT_TRUE(date_v2.get_date_from_daynr(3652424));
        EXPECT_TRUE(date_v2.year() == 9999);
        EXPECT_TRUE(date_v2.month() == 12);
        EXPECT_TRUE(date_v2.day() == 31);
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
        EXPECT_TRUE(date_v2.microsecond() == 0);
        EXPECT_TRUE(doris::calc_daynr(9999, 12, 31) == 3652424);
        EXPECT_TRUE(date_day_offset_dict::get().get_dict_init());
        EXPECT_FALSE(date_day_offset_dict::get().can_speed_up_calc_daynr(9999));
        EXPECT_FALSE(date_day_offset_dict::get().can_speed_up_daynr_to_date(3652424));
    }

    {
        DateV2Value<DateV2ValueType> date_v2;
        // Invalid date 10000/01/01
        EXPECT_FALSE(date_v2.get_date_from_daynr(3652425));
        EXPECT_TRUE(date_v2.year() == 0);
        EXPECT_TRUE(date_v2.month() == 0);
        EXPECT_TRUE(date_v2.day() == 0);
        EXPECT_TRUE(date_v2.hour() == 0);
        EXPECT_TRUE(date_v2.minute() == 0);
        EXPECT_TRUE(date_v2.second() == 0);
        EXPECT_TRUE(date_v2.microsecond() == 0);
        EXPECT_TRUE(doris::calc_daynr(10000, 01, 01) == 3652425);
        EXPECT_TRUE(date_day_offset_dict::get().get_dict_init());
        EXPECT_FALSE(date_day_offset_dict::get().can_speed_up_calc_daynr(10000));
        EXPECT_FALSE(date_day_offset_dict::get().can_speed_up_daynr_to_date(3652425));
    }
}

TEST(VDateTimeValueTest, date_v2_from_date_format_str_with_all_space) {
    auto test_all_space = [](const std::string& format_str) {
        std::string date_str = "   ";
        {
            DateV2Value<DateTimeV2ValueType> date;
            EXPECT_FALSE(date.from_date_format_str(format_str.data(), format_str.size(),
                                                   date_str.data(), date_str.size()));
        }

        {
            DateV2Value<DateV2ValueType> date;
            EXPECT_FALSE(date.from_date_format_str(format_str.data(), format_str.size(),
                                                   date_str.data(), date_str.size()));
        }

        {
            VecDateTimeValue date;
            date._type = TIME_DATE;
            EXPECT_FALSE(date.from_date_format_str(format_str.data(), format_str.size(),
                                                   date_str.data(), date_str.size()));
        }

        {
            VecDateTimeValue date;
            date._type = TIME_DATETIME;
            EXPECT_FALSE(date.from_date_format_str(format_str.data(), format_str.size(),
                                                   date_str.data(), date_str.size()));
        }
    };

    test_all_space("%Y-%m-%d %H:%i:%s.%f");
    test_all_space("%Y");
    test_all_space("%Y-%m-%d");
    test_all_space("%Y-%m-%d %H:%i:%s");
    test_all_space("%Y-%m-%d %H:%i:%s.%f %p");
    for (char ch = 'a'; ch <= 'z'; ch++) {
        std::string fomat_str = "%" + std::string(1, ch);
        test_all_space(fomat_str);
    }
    for (char ch = 'A'; ch <= 'Z'; ch++) {
        std::string fomat_str = "%" + std::string(1, ch);
        test_all_space(fomat_str);
    }
}

TEST(VDateTimeValueTest, datetime_diff_test) {
    // Test case 1: DATE to DATE - Different years, months, days
    {
        DateV2Value<DateV2ValueType> date1;
        std::string date_str1 = "2020-01-15";
        std::string format = "%Y-%m-%d";
        EXPECT_TRUE(date1.from_date_format_str(format.data(), format.size(), date_str1.data(),
                                               date_str1.size()));

        DateV2Value<DateV2ValueType> date2;
        std::string date_str2 = "2023-08-20";
        EXPECT_TRUE(date2.from_date_format_str(format.data(), format.size(), date_str2.data(),
                                               date_str2.size()));

        // Test all time units for DATE to DATE
        EXPECT_EQ(datetime_diff<TimeUnit::YEAR>(date1, date2), 3);
        EXPECT_EQ(datetime_diff<TimeUnit::MONTH>(date1, date2), 3 * 12 + 7);
        EXPECT_EQ(datetime_diff<TimeUnit::WEEK>(date1, date2), 187); // Approximately
        EXPECT_EQ(datetime_diff<TimeUnit::DAY>(date1, date2), 1313);
        EXPECT_EQ(datetime_diff<TimeUnit::HOUR>(date1, date2), 1313 * 24);
        EXPECT_EQ(datetime_diff<TimeUnit::MINUTE>(date1, date2), 1313 * 24 * 60);
        EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(date1, date2), 1313 * 24 * 60 * 60);
        EXPECT_EQ(datetime_diff<TimeUnit::MILLISECOND>(date1, date2), 1313 * 24 * 60 * 60 * 1000LL);
        EXPECT_EQ(datetime_diff<TimeUnit::MICROSECOND>(date1, date2),
                  1313 * 24 * 60 * 60 * 1000000LL);
    }

    // Test case 2: DATETIME to DATETIME - Testing rounding consistency across units
    {
        // Test 2.1: Hour rounding - less than 1 hour should truncate to 0
        {
            DateV2Value<DateTimeV2ValueType> dt1;
            std::string dt_str1 = "2023-05-10 10:00:00.000000";
            std::string format = "%Y-%m-%d %H:%i:%s.%f";
            EXPECT_TRUE(dt1.from_date_format_str(format.data(), format.size(), dt_str1.data(),
                                                 dt_str1.size()));

            DateV2Value<DateTimeV2ValueType> dt2;
            std::string dt_str2 = "2023-05-10 10:59:59.999999";
            EXPECT_TRUE(dt2.from_date_format_str(format.data(), format.size(), dt_str2.data(),
                                                 dt_str2.size()));

            EXPECT_EQ(datetime_diff<TimeUnit::HOUR>(dt1, dt2), 0);
        }

        // Test 2.2: Hour rounding - exactly 1 hour
        {
            DateV2Value<DateTimeV2ValueType> dt1;
            std::string dt_str1 = "2023-05-10 10:00:00.000000";
            std::string format = "%Y-%m-%d %H:%i:%s.%f";
            EXPECT_TRUE(dt1.from_date_format_str(format.data(), format.size(), dt_str1.data(),
                                                 dt_str1.size()));

            DateV2Value<DateTimeV2ValueType> dt2;
            std::string dt_str2 = "2023-05-10 11:00:00.000000";
            EXPECT_TRUE(dt2.from_date_format_str(format.data(), format.size(), dt_str2.data(),
                                                 dt_str2.size()));

            EXPECT_EQ(datetime_diff<TimeUnit::HOUR>(dt1, dt2), 1);
        }

        // Test 2.3: Minute rounding - less than 1 minute should truncate to 0
        {
            DateV2Value<DateTimeV2ValueType> dt1;
            std::string dt_str1 = "2023-05-10 10:15:00.000000";
            std::string format = "%Y-%m-%d %H:%i:%s.%f";
            EXPECT_TRUE(dt1.from_date_format_str(format.data(), format.size(), dt_str1.data(),
                                                 dt_str1.size()));

            DateV2Value<DateTimeV2ValueType> dt2;
            std::string dt_str2 = "2023-05-10 10:15:59.999999";
            EXPECT_TRUE(dt2.from_date_format_str(format.data(), format.size(), dt_str2.data(),
                                                 dt_str2.size()));

            EXPECT_EQ(datetime_diff<TimeUnit::MINUTE>(dt1, dt2), 0);
        }

        // Test 2.4: Minute rounding - exactly 1 minute
        {
            DateV2Value<DateTimeV2ValueType> dt1;
            std::string dt_str1 = "2023-05-10 10:15:00.000000";
            std::string format = "%Y-%m-%d %H:%i:%s.%f";
            EXPECT_TRUE(dt1.from_date_format_str(format.data(), format.size(), dt_str1.data(),
                                                 dt_str1.size()));

            DateV2Value<DateTimeV2ValueType> dt2;
            std::string dt_str2 = "2023-05-10 10:16:00.000000";
            EXPECT_TRUE(dt2.from_date_format_str(format.data(), format.size(), dt_str2.data(),
                                                 dt_str2.size()));

            EXPECT_EQ(datetime_diff<TimeUnit::MINUTE>(dt1, dt2), 1);
        }

        // Test 2.5: Second rounding - less than 1 second should truncate to 0
        {
            DateV2Value<DateTimeV2ValueType> dt1;
            std::string dt_str1 = "2023-05-10 10:15:30.000000";
            std::string format = "%Y-%m-%d %H:%i:%s.%f";
            EXPECT_TRUE(dt1.from_date_format_str(format.data(), format.size(), dt_str1.data(),
                                                 dt_str1.size()));

            DateV2Value<DateTimeV2ValueType> dt2;
            std::string dt_str2 = "2023-05-10 10:15:30.999999";
            EXPECT_TRUE(dt2.from_date_format_str(format.data(), format.size(), dt_str2.data(),
                                                 dt_str2.size()));

            EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(dt1, dt2), 0);
        }

        // Test 2.6: Second rounding - exactly 1 second
        {
            DateV2Value<DateTimeV2ValueType> dt1;
            std::string dt_str1 = "2023-05-10 10:15:30.000000";
            std::string format = "%Y-%m-%d %H:%i:%s.%f";
            EXPECT_TRUE(dt1.from_date_format_str(format.data(), format.size(), dt_str1.data(),
                                                 dt_str1.size()));

            DateV2Value<DateTimeV2ValueType> dt2;
            std::string dt_str2 = "2023-05-10 10:15:31.000000";
            EXPECT_TRUE(dt2.from_date_format_str(format.data(), format.size(), dt_str2.data(),
                                                 dt_str2.size()));

            EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(dt1, dt2), 1);
        }

        // Test 2.7: Mixed unit truncating case - complex example with multiple units
        {
            DateV2Value<DateTimeV2ValueType> dt1;
            std::string dt_str1 = "2023-05-10 10:00:00.000000";
            std::string format = "%Y-%m-%d %H:%i:%s.%f";
            EXPECT_TRUE(dt1.from_date_format_str(format.data(), format.size(), dt_str1.data(),
                                                 dt_str1.size()));

            DateV2Value<DateTimeV2ValueType> dt2;
            std::string dt_str2 = "2023-05-10 11:29:45.750000";
            EXPECT_TRUE(dt2.from_date_format_str(format.data(), format.size(), dt_str2.data(),
                                                 dt_str2.size()));

            EXPECT_EQ(datetime_diff<TimeUnit::HOUR>(dt1, dt2),
                      1); // 1h 29m 45.75s = 1.496h, truncates to 1
            EXPECT_EQ(datetime_diff<TimeUnit::MINUTE>(dt1, dt2),
                      89); // 1h 29m 45.75s = 89.7625m, truncates to 89
            EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(dt1, dt2),
                      5385); // 1h 29m 45.75s = 5385.75s, truncates to 5385
        }

        // Test 2.8: Negative differences with truncating - less than 1 unit
        {
            DateV2Value<DateTimeV2ValueType> dt1;
            std::string dt_str1 = "2023-05-10 10:15:00.000000";
            std::string format = "%Y-%m-%d %H:%i:%s.%f";
            EXPECT_TRUE(dt1.from_date_format_str(format.data(), format.size(), dt_str1.data(),
                                                 dt_str1.size()));

            DateV2Value<DateTimeV2ValueType> dt2;
            std::string dt_str2 = "2023-05-10 10:14:30.250000";
            EXPECT_TRUE(dt2.from_date_format_str(format.data(), format.size(), dt_str2.data(),
                                                 dt_str2.size()));

            EXPECT_EQ(datetime_diff<TimeUnit::MINUTE>(dt1, dt2), 0);   // -0.5m truncates to 0
            EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(dt1, dt2), -29); // -29.75s truncates to -29
        }

        // Test 2.9: Negative differences with truncating - exact unit
        {
            DateV2Value<DateTimeV2ValueType> dt1;
            std::string dt_str1 = "2023-05-10 10:15:00.000000";
            std::string format = "%Y-%m-%d %H:%i:%s.%f";
            EXPECT_TRUE(dt1.from_date_format_str(format.data(), format.size(), dt_str1.data(),
                                                 dt_str1.size()));

            DateV2Value<DateTimeV2ValueType> dt2;
            std::string dt_str2 = "2023-05-10 10:14:00.000000";
            EXPECT_TRUE(dt2.from_date_format_str(format.data(), format.size(), dt_str2.data(),
                                                 dt_str2.size()));

            EXPECT_EQ(datetime_diff<TimeUnit::MINUTE>(dt1, dt2), -1);  // Exactly -1 minute
            EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(dt1, dt2), -60); // Exactly -60 seconds
        }

        // Test 2.10: Negative differences with truncating - complex example
        {
            DateV2Value<DateTimeV2ValueType> dt1;
            std::string dt_str1 = "2023-05-10 11:30:30.750000";
            std::string format = "%Y-%m-%d %H:%i:%s.%f";
            EXPECT_TRUE(dt1.from_date_format_str(format.data(), format.size(), dt_str1.data(),
                                                 dt_str1.size()));

            DateV2Value<DateTimeV2ValueType> dt2;
            std::string dt_str2 = "2023-05-10 10:00:00.000000";
            EXPECT_TRUE(dt2.from_date_format_str(format.data(), format.size(), dt_str2.data(),
                                                 dt_str2.size()));

            EXPECT_EQ(datetime_diff<TimeUnit::HOUR>(dt1, dt2),
                      -1); // -1h 30m 30.75s = -1.5085h, truncates to -1
            EXPECT_EQ(datetime_diff<TimeUnit::MINUTE>(dt1, dt2),
                      -90); // -1h 30m 30.75s = -90.5125m, truncates to -90
            EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(dt1, dt2),
                      -5430); // -1h 30m 30.75s = -5430.75s, truncates to -5430
        }
    }
}

} // namespace doris::vectorized

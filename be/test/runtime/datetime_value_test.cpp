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

#include "runtime/datetime_value.h"

#include <gtest/gtest.h>

#include <string>

#include "common/logging.h"
#include "util/logging.h"
#include "util/timezone_utils.h"

namespace doris {

class DateTimeValueTest : public testing::Test {
public:
    DateTimeValueTest() {}

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
};

// Assert size
TEST_F(DateTimeValueTest, struct_size) {
    ASSERT_EQ(16, sizeof(DateTimeValue));
}

TEST_F(DateTimeValueTest, equal) {
    DateTimeValue value1;
    value1.from_date_int64(19880201);
    DateTimeValue value2;

    value2.from_date_int64(19880201);
    ASSERT_TRUE(value1 == value2);
    ASSERT_TRUE(value1 <= value2);
    ASSERT_TRUE(value1 >= value2);
    ASSERT_FALSE(value1 < value2);
    ASSERT_FALSE(value1 > value2);

    value2.from_date_int64(19880201000000);
    ASSERT_TRUE(value1 == value2);
    ASSERT_TRUE(value1 <= value2);
    ASSERT_TRUE(value1 >= value2);
    ASSERT_FALSE(value1 < value2);
    ASSERT_FALSE(value1 > value2);

    value2.from_date_int64(19880202);
    ASSERT_TRUE(value1 != value2);
    ASSERT_TRUE(value1 <= value2);
    ASSERT_TRUE(value1 < value2);
    ASSERT_FALSE(value1 >= value2);
    ASSERT_FALSE(value1 > value2);

    value2.from_date_int64(19880131);
    ASSERT_TRUE(value1 != value2);
    ASSERT_TRUE(value1 > value2);
    ASSERT_TRUE(value1 >= value2);
    ASSERT_FALSE(value1 < value2);
    ASSERT_FALSE(value1 <= value2);

    std::string test_str = "1988-02-01 00:00:00.12345";
    value2.from_date_str(test_str.c_str(), test_str.size());
    ASSERT_TRUE(value1 != value2);
    ASSERT_TRUE(value1 < value2);
    ASSERT_TRUE(value1 <= value2);
    ASSERT_FALSE(value1 > value2);
    ASSERT_FALSE(value1 >= value2);
}

// Test check range
TEST_F(DateTimeValueTest, day_of_year) {
    DateTimeValue value;
    value.from_date_int64(20070101);
    ASSERT_EQ(1, value.day_of_year());
    value.from_date_int64(20070203);
    ASSERT_EQ(34, value.day_of_year());
}

// Test check range
TEST_F(DateTimeValueTest, random_convert) {
    char buf[64];

    for (int i = 0; i < 64; ++i) {
        buf[i] = '0' + i % 10;
    }

    DateTimeValue value_check;

    DateTimeValue* value = (DateTimeValue*)buf;
    value->from_date_daynr(366);
    value_check.from_date_daynr(366);
    ASSERT_STREQ("0001-01-01", value->debug_string().c_str());
    ASSERT_FALSE(memcmp(value, &value_check, sizeof(value_check)));

    value = (DateTimeValue*)&buf[5];
    value->from_date_str("19880201", 8);
    value_check.from_date_str("19880201", 8);
    ASSERT_STREQ("1988-02-01", value->debug_string().c_str());
    ASSERT_FALSE(memcmp(value, &value_check, sizeof(value_check)));

    value = (DateTimeValue*)&buf[10];
    value->from_date_format_str("%Y%m%d", 6, "19880201", 8);
    value_check.from_date_format_str("%Y%m%d", 6, "19880201", 8);
    ASSERT_STREQ("1988-02-01", value->debug_string().c_str());
    ASSERT_FALSE(memcmp(value, &value_check, sizeof(value_check)));

    value = (DateTimeValue*)&buf[15];
    value->from_date_int64(19880201);
    value_check.from_date_int64(19880201);
    ASSERT_STREQ("1988-02-01", value->debug_string().c_str());
    ASSERT_FALSE(memcmp(value, &value_check, sizeof(value_check)));

    value = (DateTimeValue*)&buf[20];
    value->from_olap_datetime(19880201235959);
    value_check.from_date_int64(19880201235959);
    ASSERT_STREQ("1988-02-01 23:59:59", value->debug_string().c_str());
    ASSERT_FALSE(memcmp(value, &value_check, sizeof(value_check)));

    value = (DateTimeValue*)&buf[30];
    uint64_t date = 0;
    date = 1988;
    date <<= 4;
    date |= 2;
    date <<= 5;
    date |= 1;
    value->from_olap_date(date);
    value_check.from_date_int64(19880201);
    ASSERT_STREQ("1988-02-01", value->debug_string().c_str());
    ASSERT_FALSE(memcmp(value, &value_check, sizeof(value_check)));
}

// Test check range
TEST_F(DateTimeValueTest, construct_int64) {
    char buf[64];

    DateTimeValue value;
    value.from_date_int64(19880201);

    DateTimeValue value1(19880201);
    value1.to_string(buf);
    ASSERT_STREQ("1988-02-01", buf);
    ASSERT_TRUE(value == value1);

    value.from_date_int64(19880201123456);
    DateTimeValue value2(19880201123456);
    value2.to_string(buf);
    ASSERT_STREQ("1988-02-01 12:34:56", buf);
    ASSERT_TRUE(value == value2);
}

// Test check range
TEST_F(DateTimeValueTest, acc) {
    DateTimeValue value;
    char buf[64];

    value.from_date_int64(19880201);
    ++value;
    value.to_string(buf);
    ASSERT_STREQ("1988-02-02", buf);

    value.from_date_int64(19880131235959);
    value.to_string(buf);
    ASSERT_STREQ("1988-01-31 23:59:59", buf);
    ++value;
    value.to_string(buf);
    ASSERT_STREQ("1988-02-01 00:00:00", buf);
}

// Test check range
TEST_F(DateTimeValueTest, local_time) {
    DateTimeValue value = DateTimeValue::local_time();
    LOG(INFO) << value.debug_string();
}

// Test check range
TEST_F(DateTimeValueTest, check_range) {
    DateTimeValue value;
    ASSERT_TRUE(value.from_date_int64(19880201123456));

    value._year = 10000;
    ASSERT_TRUE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._year = 1988;

    value._month = 13;
    ASSERT_TRUE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._month = 2;

    value._day = 32;
    ASSERT_TRUE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._day = 1;

    value._hour = TIME_MAX_HOUR;
    ASSERT_TRUE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._type = TIME_TIME;
    ASSERT_FALSE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._hour = TIME_MAX_HOUR + 1;
    ASSERT_TRUE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._type = TIME_DATETIME;
    value._hour = 12;

    value._minute = 60;
    ASSERT_TRUE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._minute = 34;

    value._second = 60;
    ASSERT_TRUE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._second = 56;

    value._microsecond = 1000000;
    ASSERT_TRUE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._month = 0;
}

TEST_F(DateTimeValueTest, check_date) {
    DateTimeValue value;
    ASSERT_TRUE(value.from_date_int64(19880201));

    value._month = 0;
    ASSERT_FALSE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._month = 2;

    value._day = 0;
    ASSERT_FALSE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._year = 1987;
    value._day = 29;
    ASSERT_TRUE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
    value._year = 2000;
    ASSERT_FALSE(DateTimeValue::check_range(value.year(), value.month(), value.day(),
            value.hour(), value.minute(), value.second(), value.microsecond(), value.type()));
}

// Calculate format
TEST_F(DateTimeValueTest, week) {
    std::string date_str;
    DateTimeValue value;

    // 0000-01-01
    date_str = "0000-01-01";
    value.from_date_str(date_str.c_str(), date_str.size());
    ASSERT_EQ(1, value.week(mysql_week_mode(0)));
    ASSERT_EQ(0, value.week(mysql_week_mode(1)));
    ASSERT_EQ(1, value.week(mysql_week_mode(2)));
    ASSERT_EQ(52, value.week(mysql_week_mode(3)));
    ASSERT_EQ(1, value.week(mysql_week_mode(4)));
    ASSERT_EQ(0, value.week(mysql_week_mode(5)));
    ASSERT_EQ(1, value.week(mysql_week_mode(6)));
    ASSERT_EQ(52, value.week(mysql_week_mode(7)));

    // 2013-12-31
    date_str = "2013-12-31";
    value.from_date_str(date_str.c_str(), date_str.size());
    ASSERT_EQ(52, value.week(mysql_week_mode(0)));
    ASSERT_EQ(53, value.week(mysql_week_mode(1)));
    ASSERT_EQ(52, value.week(mysql_week_mode(2)));
    ASSERT_EQ(1, value.week(mysql_week_mode(3)));
    ASSERT_EQ(53, value.week(mysql_week_mode(4)));
    ASSERT_EQ(52, value.week(mysql_week_mode(5)));
    ASSERT_EQ(1, value.week(mysql_week_mode(6)));
    ASSERT_EQ(52, value.week(mysql_week_mode(7)));

    // 1988-02-01
    date_str = "1988-02-01";
    value.from_date_str(date_str.c_str(), date_str.size());
    ASSERT_EQ(5, value.week(mysql_week_mode(0)));
    ASSERT_EQ(5, value.week(mysql_week_mode(1)));
    ASSERT_EQ(5, value.week(mysql_week_mode(2)));
    ASSERT_EQ(5, value.week(mysql_week_mode(3)));
    ASSERT_EQ(5, value.week(mysql_week_mode(4)));
    ASSERT_EQ(5, value.week(mysql_week_mode(5)));
    ASSERT_EQ(5, value.week(mysql_week_mode(6)));
    ASSERT_EQ(5, value.week(mysql_week_mode(7)));
}

// Calculate format
TEST_F(DateTimeValueTest, from_unixtime) {
    char str[MAX_DTVALUE_STR_LEN];
    DateTimeValue value;

    value.from_unixtime(570672000, TimezoneUtils::default_time_zone);
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 08:00:00", str);

    value.from_unixtime(253402271999, TimezoneUtils::default_time_zone);
    value.to_string(str);
    ASSERT_STREQ("9999-12-31 23:59:59", str);

    value.from_unixtime(0, TimezoneUtils::default_time_zone);
    value.to_string(str);
    ASSERT_STREQ("1970-01-01 08:00:00", str);

    ASSERT_FALSE(value.from_unixtime(1586098092, "+20:00"));
    ASSERT_FALSE(value.from_unixtime(1586098092, "foo"));
}

// Calculate format
TEST_F(DateTimeValueTest, unix_timestamp) {
    DateTimeValue value;
    int64_t timestamp;
    value.from_date_int64(19691231);
    value.unix_timestamp(&timestamp, TimezoneUtils::default_time_zone);
    ASSERT_EQ(-115200, timestamp);
    value.from_date_int64(19700101);
    value.unix_timestamp(&timestamp, TimezoneUtils::default_time_zone);
    ASSERT_EQ(0 - 28800, timestamp);
    value.from_date_int64(19700102);
    value.unix_timestamp(&timestamp, TimezoneUtils::default_time_zone);
    ASSERT_EQ(86400 - 28800, timestamp);
    value.from_date_int64(19880201000000);
    value.unix_timestamp(&timestamp, TimezoneUtils::default_time_zone);
    ASSERT_EQ(570672000 - 28800, timestamp);
    value.from_date_int64(20380119);
    value.unix_timestamp(&timestamp, TimezoneUtils::default_time_zone);
    ASSERT_EQ(2147472000 - 28800, timestamp);
    value.from_date_int64(20380120);
    value.unix_timestamp(&timestamp, TimezoneUtils::default_time_zone);
    ASSERT_EQ(2147529600, timestamp);

    value.from_date_int64(10000101);
    value.unix_timestamp(&timestamp, TimezoneUtils::default_time_zone);
    ASSERT_EQ(-30610252800, timestamp);
}

// Calculate format
TEST_F(DateTimeValueTest, add_interval) {
    // Used to check
    char str[MAX_DTVALUE_STR_LEN];

    TimeInterval interval;

    DateTimeValue value;

    value.from_date_int64(19880201);
    interval.microsecond = 1;
    ASSERT_TRUE(value.date_add_interval(interval, MICROSECOND));
    interval.microsecond = 0;
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 00:00:00.000001", str);

    value.from_date_int64(19880201);
    interval.second = 1;
    ASSERT_TRUE(value.date_add_interval(interval, SECOND));
    interval.second = 0;
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 00:00:01", str);

    value.from_date_int64(19880201);
    interval.minute = 1;
    ASSERT_TRUE(value.date_add_interval(interval, MINUTE));
    interval.minute = 0;
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 00:01:00", str);

    value.from_date_int64(19880201);
    interval.hour = 1;
    ASSERT_TRUE(value.date_add_interval(interval, HOUR));
    interval.hour = 0;
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 01:00:00", str);

    value.from_date_int64(19880201);
    interval.microsecond = 1;
    interval.second = 1;
    ASSERT_TRUE(value.date_add_interval(interval, SECOND_MICROSECOND));
    interval.microsecond = 0;
    interval.second = 0;
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 00:00:01.000001", str);

    value.from_date_int64(19880201);
    interval.day = 100;
    ASSERT_TRUE(value.date_add_interval(interval, DAY));
    interval.day = 0;
    value.to_string(str);
    ASSERT_STREQ("1988-05-11", str);

    value.from_date_int64(19880131);
    interval.month = 97;
    ASSERT_TRUE(value.date_add_interval(interval, MONTH));
    interval.month = 0;
    value.to_string(str);
    ASSERT_STREQ("1996-02-29", str);

    value.from_date_int64(19880229);
    interval.year = 1;
    ASSERT_TRUE(value.date_add_interval(interval, YEAR));
    interval.year = 0;
    value.to_string(str);
    ASSERT_STREQ("1989-02-28", str);
}

// Calculate format
TEST_F(DateTimeValueTest, from_date_format_str) {
    // Used to check
    char str[MAX_DTVALUE_STR_LEN];
    std::string format_str;
    std::string value_str;
    DateTimeValue value;

    // %Y-%m-%d
    format_str = "%Y-%m-%d";
    value_str = "1988-02-01";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    format_str = "%Y-%M-%d";
    value_str = "1988-feb-01";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    format_str = "%Y-%b-%d";
    value_str = "1988-feb-01";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    format_str = "%Y%b%d";
    value_str = "1988f01";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    format_str = "%y%m%d";
    value_str = "880201";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    format_str = "%y%c%d";
    value_str = "880201";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    format_str = "%y%c-%e";
    value_str = "882-1";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    // %j
    format_str = "%Y%j %H";
    value_str = "198832 03";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 03:00:00", str);

    // %x
    format_str = "%X %V %w";
    value_str = "2015 1 1";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("2015-01-05", str);

    // %x
    format_str = "%x %v %w";
    value_str = "2015 1 1";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("2014-12-29", str);

    // %x
    format_str = "%x %v %W";
    value_str = "2015 1 Monday";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("2014-12-29", str);

    // %x
    format_str = "%X %V %a";
    value_str = "2015 1 Mon";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("2015-01-05", str);

    // %T
    format_str = "%X %V %a %r";
    value_str = "2015 1 Mon 2:34:56 AM";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("2015-01-05 02:34:56", str);

    // %T
    format_str = "%X %V %a %T";
    value_str = "2015 1 Mon 12:34:56";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("2015-01-05 12:34:56", str);

    // %T
    format_str = "%W %U %Y";
    value_str = "Tuesday 00 2002";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("2002-01-01", str);

    // hour
    format_str = "%Y-%m-%d %H %i %s";
    value_str = "88-2-1 03 4 5";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 03:04:05", str);

    // escape %
    format_str = "%Y-%m-%d %H%%3A%i%%3A%s";
    value_str = "2020-02-26 00%3A00%3A00";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("2020-02-26 00:00:00", str);

    // escape %
    format_str = "%Y-%m-%d%%%% %H%%3A%i%%3A%s";
    value_str = "2020-02-26%% 00%3A00%3A00";
    ASSERT_TRUE(value.from_date_format_str(format_str.c_str(), format_str.size(), value_str.c_str(),
                                           value_str.size()));
    value.to_string(str);
    ASSERT_STREQ("2020-02-26 00:00:00", str);
}

// Calculate format
TEST_F(DateTimeValueTest, from_date_format_str_invalid) {
    // Used to check
    std::string format_str;
    std::string value_str;
    DateTimeValue value;

    format_str = "%y%c%e";
    value_str = "8821";
    ASSERT_FALSE(value.from_date_format_str(format_str.c_str(), format_str.size(),
                                            value_str.c_str(), value_str.size()));
    format_str = "%y-%c-%e";
    value_str = "8821";
    ASSERT_FALSE(value.from_date_format_str(format_str.c_str(), format_str.size(),
                                            value_str.c_str(), value_str.size()));
    format_str = "%y%c-%e";
    value_str = "882-30";
    ASSERT_FALSE(value.from_date_format_str(format_str.c_str(), format_str.size(),
                                            value_str.c_str(), value_str.size()));
    // %x
    format_str = "%X %v %w";
    value_str = "2015 1 1";
    ASSERT_FALSE(value.from_date_format_str(format_str.c_str(), format_str.size(),
                                            value_str.c_str(), value_str.size()));

    format_str = "%x %V %w";
    value_str = "2015 1 1";
    ASSERT_FALSE(value.from_date_format_str(format_str.c_str(), format_str.size(),
                                            value_str.c_str(), value_str.size()));

    format_str = "%Y-%m-%d %H%3A%i%3A%s";
    value_str = "2020-02-26 00%3A00%3A00";
    ASSERT_FALSE(value.from_date_format_str(format_str.c_str(), format_str.size(),
                                            value_str.c_str(), value_str.size()));
}
// Calculate format
TEST_F(DateTimeValueTest, format_str) {
    // Used to check
    char str[MAX_DTVALUE_STR_LEN];

    std::string format_str;
    DateTimeValue value;

    // %a
    format_str = "%a";
    value.from_date_int64(20150215);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("Sun", str);

    // %b
    format_str = "%b";
    value.from_date_int64(20150215);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("Feb", str);

    // %c
    format_str = "%c";
    value.from_date_int64(20150215);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("2", str);
    value.from_date_int64(20151215);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("12", str);

    // %d
    format_str = "%d";
    value.from_date_int64(20150215);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("15", str);
    value.from_date_int64(20150205);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("05", str);

    // %D
    format_str = "%D";
    value.from_date_int64(20150201);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("1st", str);
    value.from_date_int64(20150202);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("2nd", str);
    value.from_date_int64(20150203);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("3rd", str);
    value.from_date_int64(20150204);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("4th", str);

    // %e
    format_str = "%e";
    value.from_date_int64(20150201);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("1", str);
    value.from_date_int64(20150211);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("11", str);

    // %f
    format_str = "%f";
    value.from_date_str("20150201000000.1", 16);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("100000", str);
    value.from_date_str("20150211000000.123456", 21);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("123456", str);

    // %h %I
    format_str = "%h";
    value.from_date_int64(20150201010000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("01", str);
    value.from_date_int64(20150201230000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("11", str);
    format_str = "%I";
    value.from_date_int64(20150201010000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("01", str);
    value.from_date_int64(20150201230000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("11", str);

    // %H
    format_str = "%H";
    value.from_date_int64(20150201010000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("01", str);
    value.from_date_int64(20150201230000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("23", str);

    // %i
    format_str = "%i";
    value.from_date_int64(20150201010000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("00", str);
    value.from_date_int64(20150201235900L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("59", str);

    // %j
    format_str = "%j";
    value.from_date_int64(20150201L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("032", str);
    value.from_date_int64(20151231L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("365", str);

    // %k
    format_str = "%k";
    value.from_date_int64(20150201000000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("0", str);
    value.from_date_int64(20150201230000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("23", str);

    // %l
    format_str = "%l";
    value.from_date_int64(20150201010000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("1", str);
    value.from_date_int64(20150201230000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("11", str);

    // %m
    format_str = "%m";
    value.from_date_int64(20150201);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("02", str);

    // %M
    format_str = "%M";
    value.from_date_int64(20150201);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("February", str);

    // %p
    format_str = "%p";
    value.from_date_int64(20150201000000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("AM", str);
    value.from_date_int64(20150201120000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("PM", str);

    // %r
    format_str = "%r";
    value.from_date_int64(20150201023456L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("02:34:56 AM", str);
    value.from_date_int64(20150201003456L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("12:34:56 AM", str);
    value.from_date_int64(20150201123456L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("12:34:56 PM", str);

    // %s & %S
    format_str = "%s";
    value.from_date_int64(20150201023456L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("56", str);
    value.from_date_int64(20150201023406L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("06", str);
    format_str = "%S";
    value.from_date_int64(20150201023456L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("56", str);
    value.from_date_int64(20150201023406L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("06", str);

    // %T
    format_str = "%T";
    value.from_date_int64(20150201023456L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("02:34:56", str);
    value.from_date_int64(20150201003456L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("00:34:56", str);
    value.from_date_int64(20150201123456L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("12:34:56", str);

    // %u
    format_str = "%u";
    value.from_date_int64(20131231L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("53", str);
    value.from_date_int64(20150101);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("01", str);

    // %U
    format_str = "%U";
    value.from_date_int64(20131231L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("52", str);
    value.from_date_int64(20150101);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("00", str);

    // %v
    format_str = "%v";
    value.from_date_int64(20131231L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("01", str);
    value.from_date_int64(20150101);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("01", str);

    // %V
    format_str = "%V";
    value.from_date_int64(20131231L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("52", str);
    value.from_date_int64(20150101);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("52", str);

    // %w
    format_str = "%w";
    value.from_date_int64(20131231L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("2", str);
    value.from_date_int64(20150101);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("4", str);

    // %W
    format_str = "%W";
    value.from_date_int64(20131231L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("Tuesday", str);
    value.from_date_int64(20150101);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("Thursday", str);

    // %x
    format_str = "%x";
    value.from_date_str("0000-01-01", 10);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("4294967295", str);
    value.from_date_int64(20131231L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("2014", str);
    value.from_date_int64(20150101);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("2015", str);

    // %X
    format_str = "%X";
    value.from_date_str("0000-01-01", 10);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("0000", str);
    value.from_date_int64(20131231L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("2013", str);
    value.from_date_int64(20150101);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("2014", str);

    // %y
    format_str = "%y";
    value.from_date_int64(20150201000000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("15", str);
    value.from_date_str("0005.0201120000", 17);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("05", str);

    // %Y
    format_str = "%Y";
    value.from_date_int64(20150201000000L);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("2015", str);
    value.from_date_str("0015.0201120000", 17);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("0015", str);

    // %Y%m%a
    format_str = "this is %Y-%m-%d";
    value.from_date_int64(19880201);
    ASSERT_TRUE(value.to_format_string(format_str.c_str(), format_str.size(), str));
    ASSERT_STREQ("this is 1988-02-01", str);
}

// Calculate weekday
TEST_F(DateTimeValueTest, weekday) {
    DateTimeValue value;
    // NORMAL CASE
    ASSERT_TRUE(value.from_date_int64(101));
    ASSERT_TRUE(value.from_date_daynr(6));
    // NORMAL CASE
    ASSERT_TRUE(value.from_date_int64(20150215));
    ASSERT_TRUE(value.from_date_daynr(6));
}

// Calculate from daynr
TEST_F(DateTimeValueTest, calc_form_daynr) {
    // Used to check
    char str[MAX_DTVALUE_STR_LEN];

    DateTimeValue value;
    // NORMAL CASE
    ASSERT_TRUE(value.from_date_daynr(1));
    value.to_string(str);
    ASSERT_STREQ("0000-01-01", str);
    ASSERT_TRUE(value.from_date_daynr(59));
    value.to_string(str);
    ASSERT_STREQ("0000-02-28", str);
    ASSERT_TRUE(value.from_date_daynr(60));
    value.to_string(str);
    ASSERT_STREQ("0000-03-01", str);
    ASSERT_TRUE(value.from_date_daynr(365));
    value.to_string(str);
    ASSERT_STREQ("0000-12-31", str);
    ASSERT_TRUE(value.from_date_daynr(366));
    value.to_string(str);
    ASSERT_STREQ("0001-01-01", str);
    ASSERT_TRUE(value.from_date_daynr(1520));
    value.to_string(str);
    ASSERT_STREQ("0004-02-29", str);
    ASSERT_TRUE(value.from_date_daynr(1519));
    value.to_string(str);
    ASSERT_STREQ("0004-02-28", str);
    ASSERT_TRUE(value.from_date_daynr(1521));
    value.to_string(str);
    ASSERT_STREQ("0004-03-01", str);
    ASSERT_TRUE(value.from_date_daynr(726133));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);
    ASSERT_TRUE(value.from_date_daynr(3652424));
    value.to_string(str);
    ASSERT_STREQ("9999-12-31", str);

    // BAD CASE
    ASSERT_FALSE(value.from_date_daynr(0));
    ASSERT_FALSE(value.from_date_daynr(3652425));
    ASSERT_FALSE(value.from_date_daynr(36524251));
}

// Calculate daynr
TEST_F(DateTimeValueTest, calc_daynr) {
    ASSERT_EQ(0, DateTimeValue::calc_daynr(0, 0, 1));
    ASSERT_EQ(1, DateTimeValue::calc_daynr(0, 1, 1));
    ASSERT_EQ(365, DateTimeValue::calc_daynr(0, 12, 31));
    ASSERT_EQ(366, DateTimeValue::calc_daynr(1, 1, 1));
    ASSERT_EQ(726133, DateTimeValue::calc_daynr(1988, 2, 1));
    DateTimeValue value;
    value.from_date_int64(880201);
    ASSERT_EQ(726133, value.daynr());
    ASSERT_EQ(726161, DateTimeValue::calc_daynr(1988, 2, 29));
}

// Construct from int value
TEST_F(DateTimeValueTest, from_time_str) {
    // Used to check
    char str[MAX_DTVALUE_STR_LEN];
    std::string test_str;

    DateTimeValue value;
    // INTERNAL MODE WITH OUT
    test_str = " 880201";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    test_str = "88020112";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("8802-01-12", str);

    test_str = "8802011223";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 12:23:00", str);

    test_str = "880201122334";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 12:23:34", str);

    test_str = "19880201";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    test_str = "1988.020112";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 12:00:00", str);

    test_str = "1988.0201123";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 12:03:00", str);

    test_str = "1988.020112345";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 12:34:05", str);

    test_str = "19880201000000.1234567";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 00:00:00.123456", str);

    test_str = "19880201T000000.1234567";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 00:00:00.123456", str);

    // normal-2

    // Delim MODE
    test_str = "88-02-01";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    test_str = "1988-02-01";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);

    test_str = "1988-02-01 12";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 12:00:00", str);

    test_str = "1988-02-01 12:34";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 12:34:00", str);

    test_str = "1988-02-01 12:34:56";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 12:34:56", str);

    test_str = "1988-02-01 12:34:56.123";
    ASSERT_TRUE(value.from_date_str(test_str.c_str(), test_str.size()));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 12:34:56.123000", str);
}

TEST_F(DateTimeValueTest, from_time_str_invalid) {
    std::string test_str;

    DateTimeValue value;
    test_str = "  ";
    ASSERT_FALSE(value.from_date_str(test_str.c_str(), test_str.size()));
    test_str = " a";
    ASSERT_FALSE(value.from_date_str(test_str.c_str(), test_str.size()));
    // normal-1
    test_str = "1988-02-31 00:00:00";
    ASSERT_FALSE(value.from_date_str(test_str.c_str(), test_str.size()));

    test_str = "1988-02 01 12:34:56";
    ASSERT_FALSE(value.from_date_str(test_str.c_str(), test_str.size()));
    // excede 9999999
    test_str = "1988999-02-31 00:00:00";
    ASSERT_FALSE(value.from_date_str(test_str.c_str(), test_str.size()));

    test_str = "1988-02-31 100";
    ASSERT_FALSE(value.from_date_str(test_str.c_str(), test_str.size()));

    test_str = "1988.02-31 10";
    ASSERT_FALSE(value.from_date_str(test_str.c_str(), test_str.size()));
}

// Construct from int value
TEST_F(DateTimeValueTest, from_time_int) {
    // Used to check
    char str[MAX_DTVALUE_STR_LEN];

    DateTimeValue value;
    // 59 -> 00:00:59
    ASSERT_TRUE(value.from_time_int64(59));
    value.to_string(str);
    ASSERT_STREQ("00:00:59", str);
    // 5959 -> 00:59:59
    ASSERT_TRUE(value.from_time_int64(5959));
    value.to_string(str);
    ASSERT_STREQ("00:59:59", str);
    // 8385959 -> 838:59:59
    ASSERT_TRUE(value.from_time_int64(8385959));
    value.to_string(str);
    ASSERT_STREQ("838:59:59", str);
    // 880201000000
    ASSERT_TRUE(value.from_time_int64(880201000000));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 00:00:00", str);
}

// Construct from int value
TEST_F(DateTimeValueTest, from_time_int_invalid) {
    DateTimeValue value;
    // 69
    ASSERT_FALSE(value.from_time_int64(69));
    // 6959
    ASSERT_FALSE(value.from_time_int64(6959));
    // 9006969
    ASSERT_FALSE(value.from_time_int64(8396969));
    // 1000132000000
    ASSERT_FALSE(value.from_time_int64(1000132000000));
}

// Construct from int value
TEST_F(DateTimeValueTest, from_int_value) {
    // Used to check
    char str[MAX_DTVALUE_STR_LEN];

    DateTimeValue value;
    // 101 -> 2000-01-01
    ASSERT_TRUE(value.from_date_int64(101));
    value.to_string(str);
    ASSERT_STREQ("2000-01-01", str);
    // 150201 -> 2015-02-01
    ASSERT_TRUE(value.from_date_int64(150201));
    value.to_string(str);
    ASSERT_STREQ("2015-02-01", str);
    // 691231 -> 2069-12-31
    ASSERT_TRUE(value.from_date_int64(691231));
    value.to_string(str);
    ASSERT_STREQ("2069-12-31", str);
    // 700101 -> 1970-01-01
    ASSERT_TRUE(value.from_date_int64(700101));
    value.to_string(str);
    ASSERT_STREQ("1970-01-01", str);
    // 880201 -> 1988-02-01
    ASSERT_TRUE(value.from_date_int64(880201));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01", str);
    // 991231 -> 1999-12-31
    ASSERT_TRUE(value.from_date_int64(991231));
    value.to_string(str);
    ASSERT_STREQ("1999-12-31", str);
    // 10000101 -> 1000-01-01
    ASSERT_TRUE(value.from_date_int64(10000101));
    value.to_string(str);
    ASSERT_STREQ("1000-01-01", str);
    // 12340531 -> 1234-05-31
    ASSERT_TRUE(value.from_date_int64(12340531));
    value.to_string(str);
    ASSERT_STREQ("1234-05-31", str);
    // 99991231 -> 9999-12-31
    ASSERT_TRUE(value.from_date_int64(99991231));
    value.to_string(str);
    ASSERT_STREQ("9999-12-31", str);

    // BELOW IS DATETIME VALUE CHECK

    // 101000000 -> 2000-01-01 00:00:00
    ASSERT_TRUE(value.from_date_int64(101000000));
    value.to_string(str);
    ASSERT_STREQ("2000-01-01 00:00:00", str);
    // 150201123456 -> 2015-02-01 12:34:56
    ASSERT_TRUE(value.from_date_int64(150201123456));
    value.to_string(str);
    ASSERT_STREQ("2015-02-01 12:34:56", str);
    // 691231235959 -> 2069-12-31 23:59:59
    ASSERT_TRUE(value.from_date_int64(691231235959));
    value.to_string(str);
    ASSERT_STREQ("2069-12-31 23:59:59", str);

    // 700101000000 -> 1970-01-01 00:00:00
    ASSERT_TRUE(value.from_date_int64(700101000000));
    value.to_string(str);
    ASSERT_STREQ("1970-01-01 00:00:00", str);
    // 880201123456 -> 1988-02-01 12:34:56
    ASSERT_TRUE(value.from_date_int64(880201123456));
    value.to_string(str);
    ASSERT_STREQ("1988-02-01 12:34:56", str);
    // 991231235959 -> 1999-12-31 23:59:59
    ASSERT_TRUE(value.from_date_int64(991231235959));
    value.to_string(str);
    ASSERT_STREQ("1999-12-31 23:59:59", str);

    // four digits year
    // 1231231235959 -> 0123-12-31 23:59:59
    ASSERT_TRUE(value.from_date_int64(1231231235959));
    value.to_string(str);
    ASSERT_STREQ("0123-12-31 23:59:59", str);

    // 99991231235959 -> 9999-12-31 23:59:59
    ASSERT_TRUE(value.from_date_int64(99991231235959));
    value.to_string(str);
    ASSERT_STREQ("9999-12-31 23:59:59", str);
}

// Construct from int value invalid
TEST_F(DateTimeValueTest, from_int_value_invalid) {
    DateTimeValue value;
    char str[MAX_DTVALUE_STR_LEN];
    // minus value
    ASSERT_FALSE(value.from_date_int64(-1231));
    // [0, 101)
    ASSERT_FALSE(value.from_date_int64(100));
    // 691323
    ASSERT_FALSE(value.from_date_int64(691323));
    // three digits year 8980101
    ASSERT_FALSE(value.from_date_int64(8980101));
    // 100-12-31
    ASSERT_FALSE(value.from_date_int64(1000101L));
    // 100-12-31
    ASSERT_FALSE(value.from_date_int64(1232));
    // 99 00:00:00
    ASSERT_TRUE(value.from_date_int64(99000000));
    value.to_string(str);
    ASSERT_STREQ("9900-00-00", str);

    // 9999-99-99 99:99:99 + 1
    ASSERT_FALSE(value.from_date_int64(99999999999999L + 1));
}

// Convert to datetime string
TEST_F(DateTimeValueTest, to_string) {
    char str[MAX_DTVALUE_STR_LEN];

    // 0000-00-00 00:00:00
    {
        DateTimeValue value;
        // datetime
        value.to_string(str);
        ASSERT_STREQ("0000-00-00 00:00:00", str);

        // date
        value._type = TIME_DATE;
        value.to_string(str);
        ASSERT_STREQ("0000-00-00", str);

        // time
        value._type = TIME_TIME;
        value.to_string(str);
        ASSERT_STREQ("00:00:00", str);
    }

    // 8765-12-23 12:34:56.987654
    {
        DateTimeValue value;
        value._year = 8765;
        value._month = 12;
        value._day = 23;
        value._hour = 12;
        value._minute = 34;
        value._second = 56;
        value._microsecond = 987654;
        // datetime
        value.to_string(str);
        ASSERT_STREQ("8765-12-23 12:34:56.987654", str);

        // time
        value._type = TIME_TIME;
        value.to_string(str);
        ASSERT_STREQ("12:34:56.987654", str);

        // date
        value._type = TIME_DATE;
        value.to_string(str);
        ASSERT_STREQ("8765-12-23", str);
    }

    // 0001-02-03 04:05:06.000007
    {
        DateTimeValue value;
        value._year = 1;
        value._month = 2;
        value._day = 3;
        value._hour = 4;
        value._minute = 5;
        value._second = 6;
        value._microsecond = 7;
        value.to_string(str);
        ASSERT_STREQ("0001-02-03 04:05:06.000007", str);

        // date
        value._type = TIME_DATE;
        value.to_string(str);
        ASSERT_STREQ("0001-02-03", str);

        // time
        value._type = TIME_TIME;
        value.to_string(str);
        ASSERT_STREQ("04:05:06.000007", str);
    }

    // time minus -100:05:06
    {
        DateTimeValue value;
        value._hour = 100;
        value._minute = 5;
        value._second = 6;
        value._microsecond = 7;
        value._type = TIME_TIME;
        value._neg = 1;
        value.to_string(str);
        ASSERT_STREQ("-100:05:06.000007", str);
    }
}

TEST_F(DateTimeValueTest, to_int64) {
    // 0000-00-00 00:00:00
    {
        DateTimeValue value;
        // datetime
        ASSERT_EQ(0L, value.to_int64());

        // date
        value._type = TIME_DATE;
        ASSERT_EQ(0L, value.to_int64());

        // time
        value._type = TIME_TIME;
        ASSERT_EQ(0L, value.to_int64());
    }

    // 8765-12-23 12:34:56.987654
    {
        DateTimeValue value;
        value._year = 8765;
        value._month = 12;
        value._day = 23;
        value._hour = 12;
        value._minute = 34;
        value._second = 56;
        value._microsecond = 987654;
        // datetime
        ASSERT_EQ(87651223123456L, value.to_int64());

        // time
        value._type = TIME_TIME;
        ASSERT_EQ(123456L, value.to_int64());

        // date
        value._type = TIME_DATE;
        ASSERT_EQ(87651223L, value.to_int64());
    }

    // 0001-02-03 04:05:06.000007
    {
        DateTimeValue value;
        value._year = 1;
        value._month = 2;
        value._day = 3;
        value._hour = 4;
        value._minute = 5;
        value._second = 6;
        value._microsecond = 7;
        ASSERT_EQ(10203040506L, value.to_int64());

        // date
        value._type = TIME_DATE;
        ASSERT_EQ(10203L, value.to_int64());

        // time
        value._type = TIME_TIME;
        ASSERT_EQ(40506L, value.to_int64());
    }

    // time minus -100:05:06
    {
        DateTimeValue value;
        value._hour = 100;
        value._minute = 5;
        value._second = 6;
        value._microsecond = 7;
        value._type = TIME_TIME;
        value._neg = 1;
        ASSERT_EQ(-1000506L, value.to_int64());
    }
}

TEST_F(DateTimeValueTest, operator_minus) {
    {
        DateTimeValue v1;
        ASSERT_TRUE(v1.from_date_int64(19880201));
        DateTimeValue v2;
        ASSERT_TRUE(v2.from_date_int64(19870201));
        int value = v1 - v2;
        ASSERT_EQ(365, value);
        value = v2 - v1;
        ASSERT_EQ(-365, value);
        value = v1 - v1;
        ASSERT_EQ(0, value);
    }

    {
        DateTimeValue v1;
        ASSERT_TRUE(v1.from_date_int64(19880201));
        DateTimeValue v2;
        ASSERT_TRUE(v2.from_date_int64(19870201123456));
        int value = v1 - v2;
        ASSERT_EQ(365, value);
        value = v2 - v1;
        ASSERT_EQ(-365, value);
        value = v1 - v1;
        ASSERT_EQ(0, value);
    }
}

TEST_F(DateTimeValueTest, min_max) {
    char buf[64];
    {
        DateTimeValue v1 = DateTimeValue::datetime_min_value();
        v1.to_string(buf);
        ASSERT_STREQ("0000-01-01 00:00:00", buf);
    }

    {
        DateTimeValue v1 = DateTimeValue::datetime_max_value();
        v1.to_string(buf);
        ASSERT_STREQ("9999-12-31 23:59:59", buf);
    }
}

TEST_F(DateTimeValueTest, packed_time) {
    char buf[64];
    {
        DateTimeValue v1;
        v1.from_date_int64(20010203123456L);
        v1.to_string(buf);
        ASSERT_STREQ("2001-02-03 12:34:56", buf);

        int64_t packed_time = v1.to_int64_date_packed();
        ASSERT_EQ(1830649476851695616L, packed_time);
        packed_time = v1.to_int64_datetime_packed();
        ASSERT_EQ(1830650338932162560L, packed_time);
    }

    {
        doris_udf::DateTimeVal tv;
        tv.packed_time = 1830650338932162560L;
        tv.type = TIME_DATETIME;
        DateTimeValue v1 = DateTimeValue::from_datetime_val(tv);
        v1.to_string(buf);
        ASSERT_STREQ("2001-02-03 12:34:56", buf);

        doris_udf::DateTimeVal tv2;
        v1.to_datetime_val(&tv2);

        ASSERT_TRUE(tv == tv2);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    // doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

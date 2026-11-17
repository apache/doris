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

#include "util/timezone_utils.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <boost/utility/binary.hpp>
#include <iostream>

#include "cctz/time_zone.h"
#include "gtest/gtest.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {

TEST(TimezoneUtilsTest, ConvertLocalToInstant) {
    struct TestCase {
        const char* timezone;
        cctz::civil_second local;
        cctz::civil_second utc;
    };
    const TestCase cases[] = {
            {"UTC", cctz::civil_second(2024, 3, 10, 2, 30, 0),
             cctz::civil_second(2024, 3, 10, 2, 30, 0)},
            {"Etc/GMT-8", cctz::civil_second(2024, 3, 10, 2, 30, 0),
             cctz::civil_second(2024, 3, 9, 18, 30, 0)},
            {"America/New_York", cctz::civil_second(2024, 3, 10, 1, 59, 59),
             cctz::civil_second(2024, 3, 10, 6, 59, 59)},
            {"America/New_York", cctz::civil_second(2024, 3, 10, 2, 0, 0),
             cctz::civil_second(2024, 3, 10, 7, 0, 0)},
            {"America/New_York", cctz::civil_second(2024, 3, 10, 2, 30, 0),
             cctz::civil_second(2024, 3, 10, 7, 30, 0)},
            {"America/New_York", cctz::civil_second(2024, 3, 10, 2, 59, 59),
             cctz::civil_second(2024, 3, 10, 7, 59, 59)},
            {"America/New_York", cctz::civil_second(2024, 3, 10, 3, 0, 0),
             cctz::civil_second(2024, 3, 10, 7, 0, 0)},
            {"America/New_York", cctz::civil_second(2024, 11, 3, 1, 30, 0),
             cctz::civil_second(2024, 11, 3, 5, 30, 0)},
            {"Australia/Lord_Howe", cctz::civil_second(2024, 10, 6, 2, 15, 0),
             cctz::civil_second(2024, 10, 5, 15, 45, 0)},
            {"Pacific/Apia", cctz::civil_second(2011, 12, 30, 12, 0, 0),
             cctz::civil_second(2011, 12, 30, 22, 0, 0)},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.timezone);
        SCOPED_TRACE(test_case.local);
        cctz::time_zone timezone;
        ASSERT_TRUE(cctz::load_time_zone(test_case.timezone, &timezone));
        const auto instant = TimezoneUtils::convert_local_to_instant(test_case.local, timezone);
        EXPECT_EQ(test_case.utc, cctz::convert(instant, cctz::utc_time_zone()));
    }
}

TEST(TimezoneUtilsTest, ParseOffset) {
    const auto tp = cctz::civil_second(2011, 1, 1, 0, 0,
                                       0); // offset has no DST, every time point is acceptable
    cctz::time_zone result;
    const auto lookup_offset = [&](const cctz::time_zone& tz) {
        return tz.lookup(cctz::convert(tp, tz)).offset;
    };

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("+14:00", result));
    EXPECT_EQ(lookup_offset(result), 14 * 3600);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("+00:00", result));
    EXPECT_EQ(lookup_offset(result), 0);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("+00:30", result));
    EXPECT_EQ(lookup_offset(result), 1800);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("+10:30", result));
    EXPECT_EQ(lookup_offset(result), 10 * 3600 + 1800);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("+01:00", result));
    EXPECT_EQ(lookup_offset(result), 1 * 3600);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("-12:00", result));
    EXPECT_EQ(lookup_offset(result), -12 * 3600);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("-09:00", result));
    EXPECT_EQ(lookup_offset(result), -9 * 3600);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("-01:00", result));
    EXPECT_EQ(lookup_offset(result), -1 * 3600);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("-00:00", result));
    EXPECT_EQ(lookup_offset(result), 0);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("-00:30", result));
    EXPECT_EQ(lookup_offset(result), -1800);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("-10:30", result));
    EXPECT_EQ(lookup_offset(result), -10 * 3600 - 1800);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("+9:30", result));
    EXPECT_EQ(lookup_offset(result), 9 * 3600 + 1800);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("UTC+8", result));
    EXPECT_EQ(lookup_offset(result), 8 * 3600);

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("GMT-06:30", result));
    EXPECT_EQ(lookup_offset(result), -(6 * 3600 + 1800));

    EXPECT_TRUE(TimezoneUtils::parse_tz_offset_string("UTC", result));
    EXPECT_EQ(lookup_offset(result), 0);

    // out of range or illegal format
    EXPECT_FALSE(TimezoneUtils::parse_tz_offset_string("+15:00", result));
    EXPECT_FALSE(TimezoneUtils::parse_tz_offset_string("-13:00", result));
    EXPECT_FALSE(TimezoneUtils::parse_tz_offset_string("+800", result));
    EXPECT_FALSE(TimezoneUtils::parse_tz_offset_string("0800", result));
    EXPECT_FALSE(TimezoneUtils::parse_tz_offset_string("UTC+8:75", result));
}

TEST(TimezoneUtilsTest, LoadOffsets) {
    TimezoneUtils::clear_timezone_caches();
    TimezoneUtils::load_offsets_to_cache();
    EXPECT_EQ(TimezoneUtils::cache_size(), (13 + 15) * 3 - 4);

    TimezoneUtils::load_timezones_to_cache();
    EXPECT_GE(TimezoneUtils::cache_size(), 100);
}

TEST(TimezoneUtilsTest, FindTimezone) {
    TimezoneUtils::load_timezones_to_cache();

    std::string tzname;
    cctz::time_zone result;
    const auto tp = cctz::civil_second(2011, 1, 1, 0, 0, 0);
    const auto lookup_offset = [&](const cctz::time_zone& tz) {
        return tz.lookup(cctz::convert(tp, tz)).offset;
    };

    tzname = "Asia/Shanghai";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(tzname, result));
    EXPECT_EQ(lookup_offset(result), 8 * 3600);

    tzname = "America/Los_Angeles";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(tzname, result));
    EXPECT_EQ(lookup_offset(result), -8 * 3600);

    tzname = "+00:30";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(tzname, result));
    EXPECT_EQ(lookup_offset(result), 1800);

    tzname = "-00:00";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(tzname, result));
    EXPECT_EQ(lookup_offset(result), 0);

    tzname = "+14:00";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(tzname, result));
    EXPECT_EQ(lookup_offset(result), 14 * 3600);

    tzname = "-12:00";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(tzname, result));
    EXPECT_EQ(lookup_offset(result), -12 * 3600);

    tzname = "+8:00";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(tzname, result));
    EXPECT_EQ(lookup_offset(result), 8 * 3600);

    tzname = "UTC+8";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(tzname, result));
    EXPECT_EQ(lookup_offset(result), 8 * 3600);

    tzname = "GMT-6";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(tzname, result));
    EXPECT_EQ(lookup_offset(result), -6 * 3600);

    // out of range or illegal format
    tzname = "+15:00";
    EXPECT_FALSE(TimezoneUtils::find_cctz_time_zone(tzname, result));

    tzname = "-13:00";
    EXPECT_FALSE(TimezoneUtils::find_cctz_time_zone(tzname, result));

    tzname = "+800";
    EXPECT_FALSE(TimezoneUtils::find_cctz_time_zone(tzname, result));
}

TEST(TimezoneUtilsTest, OffsetBoundariesWithAndWithoutCache) {
    for (bool preload : {false, true}) {
        TimezoneUtils::clear_timezone_caches();
        if (preload) {
            TimezoneUtils::load_timezones_to_cache();
        }
        for (const auto* prefix : {"", "UTC", "GMT", "utc", "gmt"}) {
            for (const auto* offset : {"+14:01", "+14:30", "+14:45", "-12:01", "-12:30", "-12:45",
                                       "+15:00", "-13:00", "+08:60"}) {
                const std::string input = std::string(prefix) + offset;
                cctz::time_zone result = cctz::fixed_time_zone(cctz::seconds(3600));
                EXPECT_FALSE(TimezoneUtils::find_cctz_time_zone(input, result)) << input;
                EXPECT_EQ(
                        result.lookup(cctz::convert(cctz::civil_second(2024, 1, 1), result)).offset,
                        3600)
                        << input;
            }
            for (const auto* offset : {"+14:00", "-12:00", "+08:01", "+08:15", "-00:30"}) {
                cctz::time_zone result;
                EXPECT_TRUE(
                        TimezoneUtils::find_cctz_time_zone(std::string(prefix) + offset, result));
            }
        }
    }
    TimezoneUtils::load_timezones_to_cache();
}

TEST(TimezoneUtilsTest, TryGetFixedOffsetSeconds) {
    TimezoneUtils::load_timezones_to_cache();

    cctz::time_zone result;
    int32_t offset_seconds = 0;

    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("UTC", result));
    EXPECT_TRUE(TimezoneUtils::try_get_fixed_offset_seconds(result, &offset_seconds));
    EXPECT_EQ(0, offset_seconds);

    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Etc/UTC", result));
    EXPECT_TRUE(TimezoneUtils::try_get_fixed_offset_seconds(result, &offset_seconds));
    EXPECT_EQ(0, offset_seconds);

    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("+08:00", result));
    EXPECT_TRUE(TimezoneUtils::try_get_fixed_offset_seconds(result, &offset_seconds));
    EXPECT_EQ(8 * 3600, offset_seconds);

    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Etc/GMT-8", result));
    EXPECT_TRUE(TimezoneUtils::try_get_fixed_offset_seconds(result, &offset_seconds));
    EXPECT_EQ(8 * 3600, offset_seconds);

    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("-06:00", result));
    EXPECT_TRUE(TimezoneUtils::try_get_fixed_offset_seconds(result, &offset_seconds));
    EXPECT_EQ(-6 * 3600, offset_seconds);

    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("+05:45", result));
    EXPECT_TRUE(TimezoneUtils::try_get_fixed_offset_seconds(result, &offset_seconds));
    EXPECT_EQ(5 * 3600 + 45 * 60, offset_seconds);

    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Asia/Shanghai", result));
    EXPECT_FALSE(TimezoneUtils::try_get_fixed_offset_seconds(result, &offset_seconds));
}

} // namespace doris

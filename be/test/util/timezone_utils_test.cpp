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

TEST(TimezoneUtilsTest, ParseOffset) {
    const auto tp = cctz::civil_second(2011, 1, 1, 0, 0,
                                       0); // offset has no DST, every time point is acceptable
    cctz::time_zone result;

    TimezoneUtils::parse_tz_offset_string("+14:00", result);
    auto cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, 14 * 3600);

    TimezoneUtils::parse_tz_offset_string("+00:00", result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, 0 * 3600);

    TimezoneUtils::parse_tz_offset_string("+00:30", result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, 1800);

    TimezoneUtils::parse_tz_offset_string("+10:30", result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, 10 * 3600 + 1800);

    TimezoneUtils::parse_tz_offset_string("+01:00", result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, 1 * 3600);

    TimezoneUtils::parse_tz_offset_string("-12:00", result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, -12 * 3600);

    TimezoneUtils::parse_tz_offset_string("-09:00", result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, -9 * 3600);

    TimezoneUtils::parse_tz_offset_string("-01:00", result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, -1 * 3600);

    TimezoneUtils::parse_tz_offset_string("-00:00", result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, 0 * 3600);

    TimezoneUtils::parse_tz_offset_string("-00:30", result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, -1800);

    TimezoneUtils::parse_tz_offset_string("-10:30", result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, -10 * 3600 - 1800);

    // out of range or illegal format
    EXPECT_FALSE(TimezoneUtils::parse_tz_offset_string("+15:00", result));
    EXPECT_FALSE(TimezoneUtils::parse_tz_offset_string("-13:00", result));
    EXPECT_FALSE(TimezoneUtils::parse_tz_offset_string("+9:30", result));
}

TEST(TimezoneUtilsTest, LoadOffsets) {
    TimezoneUtils::clear_timezone_caches();
    TimezoneUtils::load_offsets_to_cache();
    EXPECT_EQ(TimezoneUtils::cache_size(), (13 + 15) * 2);

    TimezoneUtils::load_timezones_to_cache();
    EXPECT_GE(TimezoneUtils::cache_size(), 100);
}

TEST(TimezoneUtilsTest, FindTimezone) {
    TimezoneUtils::load_timezones_to_cache();

    std::string tzname;
    cctz::time_zone result;
    const auto tp = cctz::civil_second(2011, 1, 1, 0, 0, 0);

    tzname = "Asia/Shanghai";
    TimezoneUtils::find_cctz_time_zone(tzname, result);
    auto cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, 8 * 3600);

    tzname = "America/Los_Angeles";
    TimezoneUtils::find_cctz_time_zone(tzname, result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, -8 * 3600);

    tzname = "+00:30";
    TimezoneUtils::find_cctz_time_zone(tzname, result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, 1800);

    tzname = "-00:00";
    TimezoneUtils::find_cctz_time_zone(tzname, result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, 0);

    tzname = "+14:00";
    TimezoneUtils::find_cctz_time_zone(tzname, result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, 14 * 3600);

    tzname = "-12:00";
    TimezoneUtils::find_cctz_time_zone(tzname, result);
    cl = result.lookup(cctz::convert(tp, result));
    EXPECT_EQ(cl.offset, -12 * 3600);

    // out of range or illegal format
    tzname = "+15:00";
    EXPECT_FALSE(TimezoneUtils::find_cctz_time_zone(tzname, result));

    tzname = "-13:00";
    EXPECT_FALSE(TimezoneUtils::find_cctz_time_zone(tzname, result));

    tzname = "+9:30";
    EXPECT_FALSE(TimezoneUtils::find_cctz_time_zone(tzname, result));
}

} // namespace doris

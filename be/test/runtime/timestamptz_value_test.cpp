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

#include "core/value/timestamptz_value.h"

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <utility>

#include "common/exception.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_to_timestamptz_impl.hpp"
#include "testutil/datetime_ut_util.h"
#include "util/timezone_utils.h"

namespace doris {

TEST(TimeStampTzValueTest, make_time) {
    TimestampTzValue tz {};
    EXPECT_EQ(tz.to_date_int_val(), MIN_DATETIME_V2);
}

TEST(TimeStampTzValueTest, ToStringPreservesHistoricalOffsetSeconds) {
    TimezoneUtils::load_offsets_to_cache();
    const auto utc = cctz::utc_time_zone();
    cctz::time_zone shanghai;
    cctz::time_zone new_york;
    cctz::time_zone kathmandu;
    ASSERT_TRUE(cctz::load_time_zone("Asia/Shanghai", &shanghai));
    ASSERT_TRUE(cctz::load_time_zone("America/New_York", &new_york));
    ASSERT_TRUE(cctz::load_time_zone("Asia/Kathmandu", &kathmandu));
    struct TestCase {
        cctz::time_zone zone;
        int year;
        const char* civil;
        const char* offset;
    };
    const TestCase cases[] = {
            {shanghai, 1890, "1890-01-01 08:05:43", "+08:05:43"},
            {new_york, 1880, "1879-12-31 19:03:58", "-04:56:02"},
            // Pre-standard offsets vary across tzdata versions. Fixed zones keep coverage
            // of offsets beyond 14 hours independent of the host's historical records.
            {.zone = cctz::fixed_time_zone(std::chrono::seconds(-57368)),
             .year = 1800,
             .civil = "1799-12-31 08:03:52",
             .offset = "-15:56:08"},
            {.zone = cctz::fixed_time_zone(std::chrono::seconds(-51660)),
             .year = 1800,
             .civil = "1799-12-31 09:39:00",
             .offset = "-14:21"},
            {shanghai, 2024, "2024-01-01 08:00:00", "+08:00"},
            {new_york, 2024, "2023-12-31 19:00:00", "-05:00"},
            {kathmandu, 2024, "2024-01-01 05:45:00", "+05:45"},
            {utc, 2024, "2024-01-01 00:00:00", "+00:00"},
    };
    for (const auto& test_case : cases) {
        const auto& zone = test_case.zone;
        for (const auto scale : {0, 3, 6}) {
            SCOPED_TRACE(testing::Message() << zone.name() << ", scale=" << scale);
            const auto micros = scale == 6 ? 123456 : scale == 3 ? 123000 : 0;
            const auto value = make_timestamptz(test_case.year, 1, 1, 0, 0, 0, micros);
            const std::string fraction = scale == 6 ? ".123456" : scale == 3 ? ".123" : "";
            const auto formatted = value.to_string(zone, scale);
            EXPECT_EQ(formatted, std::string(test_case.civil) + fraction + test_case.offset);

            // The client-visible offset must describe the same instant, including historical
            // sub-minute offsets; parsing in UTC must not depend on the display session zone.
            for (const bool strict : {false, true}) {
                TimestampTzValue parsed;
                CastParameters params;
                params.is_strict = strict;
                ASSERT_TRUE(parsed.from_string(StringRef(formatted), &utc, params, scale))
                        << params.status.to_string();
                EXPECT_EQ(parsed, value) << formatted;
            }
        }
    }
}

TEST(TimeStampTzValueTest, ToStringRejectsUnrepresentableLocalYear) {
    TimezoneUtils::load_offsets_to_cache();
    const auto utc = cctz::utc_time_zone();
    const auto east = cctz::fixed_time_zone(std::chrono::hours(8));
    const auto west = cctz::fixed_time_zone(std::chrono::hours(-8));
    for (const auto scale : {0, 3, 6}) {
        const auto micros = scale == 6 ? 999999 : scale == 3 ? 999000 : 0;
        const auto minimum = make_timestamptz(0, 1, 1, 0, 0, 0, 0);
        const auto maximum = make_timestamptz(9999, 12, 31, 23, 59, 59, micros);
        // A valid UTC instant must not turn into an offset-only protocol value.
        for (const auto& entry : {std::make_pair(minimum, west), std::make_pair(maximum, east)}) {
            try {
                static_cast<void>(entry.first.to_string(entry.second, scale));
                FAIL() << "Expected an unrepresentable local year error";
            } catch (const Exception& e) {
                EXPECT_EQ(e.code(), ErrorCode::INVALID_ARGUMENT);
                EXPECT_NE(std::string(e.what()).find("TIMESTAMPTZ local year is outside [0, 9999]"),
                          std::string::npos);
            }
        }
        for (const auto& entry : {std::make_pair(minimum, utc), std::make_pair(maximum, utc),
                                  std::make_pair(minimum, east), std::make_pair(maximum, west)}) {
            const auto wire = entry.first.to_string(entry.second, scale);
            for (const bool strict : {false, true}) {
                TimestampTzValue parsed;
                CastParameters params;
                params.is_strict = strict;
                ASSERT_TRUE(parsed.from_string(StringRef(wire), &utc, params, scale))
                        << wire << ": " << params.status.to_string();
                EXPECT_EQ(parsed, entry.first);
            }
        }
    }
}

TEST(TimeStampTzValueTest, from_string) {
    cctz::time_zone time_zone = cctz::fixed_time_zone(std::chrono::hours(8));
    TimezoneUtils::load_offsets_to_cache();
    {
        TimestampTzValue tz {};
        StringRef str {"2024-01-01 12:00:00"};
        CastParameters params;
        params.is_strict = true;
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0));
        EXPECT_EQ(tz, make_timestamptz(2024, 1, 1, 4, 0, 0, 0)) << tz._utc_dt.to_string();
    }

    {
        TimestampTzValue tz {};
        StringRef str {"2024-01-01 12:00:00.123456"};
        CastParameters params;
        params.is_strict = true;
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 6));
        EXPECT_EQ(tz, make_timestamptz(2024, 1, 1, 4, 0, 0, 123456)) << tz._utc_dt.to_string();
    }

    {
        TimestampTzValue tz {};
        StringRef str {"2020-01-01 00:00:00 +03:00"};
        CastParameters params;
        params.is_strict = true;
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0)) << params.status.to_string();
        EXPECT_EQ(tz, make_timestamptz(2019, 12, 31, 21, 0, 0, 0)) << tz._utc_dt.to_string();
    }

    {
        TimestampTzValue tz {};
        StringRef str {"2020-01-01 00:00:00 -03:00"};
        CastParameters params;
        params.is_strict = true;
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0)) << params.status.to_string();
        EXPECT_EQ(tz, make_timestamptz(2020, 1, 1, 3, 0, 0, 0)) << tz._utc_dt.to_string();
    }

    {
        TimestampTzValue tz {};
        StringRef str {"2020-01-01 00:00:00 +08:00"};
        CastParameters params;
        params.is_strict = true;
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0)) << params.status.to_string();
        EXPECT_EQ(tz, make_timestamptz(2019, 12, 31, 16, 0, 0, 0)) << tz._utc_dt.to_string();
    }

    {
        TimestampTzValue tz {};
        StringRef str {"2020-01-01 00:00:00 -08:00"};
        CastParameters params;
        params.is_strict = true;
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0)) << params.status.to_string();
        EXPECT_EQ(tz, make_timestamptz(2020, 1, 1, 8, 0, 0, 0)) << tz._utc_dt.to_string();
    }

    {
        TimestampTzValue tz {};
        StringRef str {"2020-01-01 00:00:00 +14:00"};
        CastParameters params;
        params.is_strict = true;
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0)) << params.status.to_string();
        EXPECT_EQ(tz, make_timestamptz(2019, 12, 31, 10, 0, 0, 0)) << tz._utc_dt.to_string();
    }

    {
        TimestampTzValue tz {};
        StringRef str {"2020-01-01 00:00:00 -12:00"};
        CastParameters params;
        params.is_strict = true;
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0)) << params.status.to_string();
        EXPECT_EQ(tz, make_timestamptz(2020, 1, 1, 12, 0, 0, 0)) << tz._utc_dt.to_string();
    }
}

TEST(TimeStampTzValueTest, HistoricalOffsetsInStrictAndFallbackParsers) {
    const auto utc = cctz::utc_time_zone();
    const auto expected = make_timestamptz(1890, 1, 1, 0, 0, 0, 123456);
    for (const std::string input :
         {"1890-01-01 08:05:43.123456+08:05:43", "1889-12-31 19:03:58.123456-04:56:02",
          "1890-01-01 00:00:30.123456+00:00:30", "1889-12-31 23:59:30.123456-00:00:30",
          "1890-01-01 08:05:00.123456+08:05", "1889-12-31 08:03:52.123456-15:56:08",
          "1889-12-31 09:39:00.123456-14:21", "1890-01-01 15:00:00.123456+15:00",
          "1889-12-31 11:59:59.123456-12:00:01"}) {
        SCOPED_TRACE(input);
        for (const bool fallback : {false, true}) {
            TimestampTzValue parsed;
            CastParameters params;
            params.is_strict = !fallback;
            const bool success =
                    fallback
                            ? CastToTimestampTz::from_string_non_strict_mode_impl(
                                      StringRef(input), parsed, params, &utc, 6)
                            : CastToTimestampTz::from_string_strict_mode<DatelikeParseMode::STRICT>(
                                      StringRef(input), parsed, params, &utc, 6);
            EXPECT_TRUE(success) << params.status.to_string();
            EXPECT_EQ(parsed, expected);
        }
    }
    for (const std::string offset : {"+08:60:00", "+08:05:60", "+08:05:", "+08:05:4", "+08:05:430",
                                     "+24:00:00", "-24:00:00", "+99:00:00"}) {
        SCOPED_TRACE(offset);
        const auto input = "1890-01-01 00:00:00" + offset;
        for (const bool strict : {false, true}) {
            TimestampTzValue parsed;
            CastParameters params;
            params.is_strict = strict;
            EXPECT_FALSE(parsed.from_string(StringRef(input), &utc, params, 6));
        }
    }
}

TEST(TimeStampTzValueTest, from_datetime) {
    cctz::time_zone time_zone = cctz::fixed_time_zone(std::chrono::hours(8));
    TimezoneUtils::load_offsets_to_cache();

    {
        TimestampTzValue tz {};
        DateV2Value<DateTimeV2ValueType> dtv = make_datetime(2024, 1, 1, 12, 0, 0, 123456);
        EXPECT_TRUE(tz.from_datetime(dtv, time_zone, 6, 6));
        EXPECT_EQ(tz, make_timestamptz(2024, 1, 1, 4, 0, 0, 123456)) << tz._utc_dt.to_string();
    }

    {
        TimestampTzValue tz {};
        DateV2Value<DateTimeV2ValueType> dtv = make_datetime(1970, 1, 1, 0, 0, 0, 0);
        EXPECT_TRUE(tz.from_datetime(dtv, time_zone, 6, 6));
        EXPECT_EQ(tz, make_timestamptz(1969, 12, 31, 16, 0, 0, 0)) << tz._utc_dt.to_string();
    }

    {
        TimestampTzValue tz {};
        DateV2Value<DateTimeV2ValueType> dtv = make_datetime(2038, 1, 19, 3, 14, 7, 0);
        EXPECT_TRUE(tz.from_datetime(dtv, time_zone, 6, 6));
        EXPECT_EQ(tz, make_timestamptz(2038, 1, 18, 19, 14, 7, 0)) << tz._utc_dt.to_string();
    }
}

TEST(TimeStampTzValueTest, to_datetime) {
    cctz::time_zone time_zone = cctz::fixed_time_zone(std::chrono::hours(8));
    TimezoneUtils::load_offsets_to_cache();

    CastParameters params;
    params.is_strict = true;

    {
        TimestampTzValue tz {};
        tz.from_string(StringRef {"2024-01-01 12:00:00"}, &time_zone, params, 0);
        DateV2Value<DateTimeV2ValueType> res;
        EXPECT_TRUE(tz.to_datetime(res, time_zone, 6, 6));
        EXPECT_EQ(res, make_datetime(2024, 1, 1, 12, 0, 0, 0)) << res.to_string();
    }

    {
        TimestampTzValue tz {};
        tz.from_string(StringRef {"2020-01-01 00:00:00 +03:00"}, &time_zone, params, 0);
        DateV2Value<DateTimeV2ValueType> res;
        EXPECT_TRUE(tz.to_datetime(res, time_zone, 6, 6));
        EXPECT_EQ(res, make_datetime(2020, 1, 1, 5, 0, 0, 0)) << res.to_string();
    }

    {
        TimestampTzValue tz {};
        tz.from_string(StringRef {"2020-01-01 00:00:00 -03:00"}, &time_zone, params, 0);
        DateV2Value<DateTimeV2ValueType> res;
        EXPECT_TRUE(tz.to_datetime(res, time_zone, 6, 6));
        EXPECT_EQ(res, make_datetime(2020, 1, 1, 11, 0, 0, 0)) << res.to_string();
    }

    {
        TimestampTzValue tz {};
        tz.from_string(StringRef {"2020-01-01 00:00:00 +08:00"}, &time_zone, params, 0);
        DateV2Value<DateTimeV2ValueType> res;
        EXPECT_TRUE(tz.to_datetime(res, time_zone, 6, 6));
        EXPECT_EQ(res, make_datetime(2020, 1, 1, 0, 0, 0, 0)) << res.to_string();
    }

    {
        TimestampTzValue tz {};
        tz.from_string(StringRef {"2020-01-01 00:00:00 -08:00"}, &time_zone, params, 0);
        DateV2Value<DateTimeV2ValueType> res;
        EXPECT_TRUE(tz.to_datetime(res, time_zone, 6, 6));
        EXPECT_EQ(res, make_datetime(2020, 1, 1, 16, 0, 0, 0)) << res.to_string();
    }
}

} // namespace doris

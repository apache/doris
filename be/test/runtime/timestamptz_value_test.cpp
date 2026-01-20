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

#include "vec/runtime/timestamptz_value.h"

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <string>

#include "testutil/datetime_ut_util.h"
#include "util/timezone_utils.h"
#include "vec/functions/cast/cast_base.h"

namespace doris::vectorized {

TEST(TimeStampTzValueTest, make_time) {
    TimestampTzValue tz {};
    EXPECT_EQ(tz.to_date_int_val(), MIN_DATETIME_V2);
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

} // namespace doris::vectorized
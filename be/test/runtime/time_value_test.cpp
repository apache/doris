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

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>
#include <vec/runtime/time_value.h>

namespace doris {

TEST(TimeValueTest, make_time) {
    int64_t hour = 1;
    int64_t minute = 2;
    int64_t second = 3;
    TimeValue::TimeType time = TimeValue::make_time(hour, minute, second);
    EXPECT_EQ(time, 3723000000);
}

TEST(TimeValueTest, round_time) {
    //  01:02:03.500000 -> 01:02:04.000000
    EXPECT_EQ(TimeValue::round_time(TimeValue::make_time(1, 2, 3, 500000), 0),
              TimeValue::make_time(1, 2, 4));

    //  01:02:03.499999 -> 01:01:03.000000
    EXPECT_EQ(TimeValue::round_time(TimeValue::make_time(1, 2, 3, 499999), 0),
              TimeValue::make_time(1, 2, 3));

    //  -01:02:03.500000 -> -01:01:04.000000
    EXPECT_EQ(TimeValue::round_time(-TimeValue::make_time(1, 2, 3, 500000), 0),
              -TimeValue::make_time(1, 2, 4));

    //  -01:02:03.499999 -> -01:01:03.000000
    EXPECT_EQ(TimeValue::round_time(-TimeValue::make_time(1, 2, 3, 499999), 0),
              -TimeValue::make_time(1, 2, 3));
}

TEST(TimeValueTest, check_over_max_time) {
    {
        double time = 4020399LL * 1000 * 1000;
        int64_t result = TimeValue::check_over_max_time(time);
        EXPECT_EQ(result, 3020399LL * 1000 * 1000);
    }

    {
        double time = -4020399LL * 1000 * 1000;
        int64_t result = TimeValue::check_over_max_time(time);
        EXPECT_EQ(result, -3020399LL * 1000 * 1000);
    }
    {
        double time = 1L * 1000 * 1000;
        int64_t result = TimeValue::check_over_max_time(time);
        EXPECT_EQ(result, 1000000);
    }
}

TEST(TimeValueTest, to_string) {
    TimeValue::TimeType time = 3723000000;
    int scale = 0;
    std::string result = TimeValue::to_string(time, scale);
    EXPECT_EQ(result, "01:02:03");
}

TEST(TimeValueTest, hour) {
    {
        TimeValue::TimeType time = 3723000000;
        int result = TimeValue::hour(time);
        EXPECT_EQ(result, 1);
    }

    {
        TimeValue::TimeType time = -3723000000;
        int result = TimeValue::hour(time);
        EXPECT_EQ(result, 1);
    }
}

TEST(TimeValueTest, minute) {
    {
        TimeValue::TimeType time = 3723000000;
        int result = TimeValue::minute(time);
        EXPECT_EQ(result, 2);
    }
    {
        TimeValue::TimeType time = -3723000000;
        int result = TimeValue::minute(time);
        EXPECT_EQ(result, 2);
    }
}

TEST(TimeValueTest, second) {
    {
        TimeValue::TimeType time = 3723000000;
        int result = TimeValue::second(time);
        EXPECT_EQ(result, 3);
    }
    {
        TimeValue::TimeType time = -3723000000;
        int result = TimeValue::second(time);
        EXPECT_EQ(result, 3);
    }
}

TEST(TimeValueTest, from_second) {
    int64_t sec = 3723;
    TimeValue::TimeType result = TimeValue::from_second(sec);
    EXPECT_EQ(result, 3723000000);
}

TEST(TimeValueTest, try_parse_time_from_number) {
    TimeValue::TimeType x = 0;

    {
        int64_t from_other = 10203;
        bool result = TimeValue::try_parse_time(from_other, x);
        EXPECT_TRUE(result);
        EXPECT_EQ(x, 3723000000);
    }
    {
        int64_t from_other = -10203;
        bool result = TimeValue::try_parse_time(from_other, x);
        EXPECT_TRUE(result);
        EXPECT_EQ(x, -3723000000);
    }
    {
        int64_t from_other = -106630;
        bool result = TimeValue::try_parse_time(from_other, x);
        EXPECT_FALSE(result);
    }

    {
        int128_t from_other = -10203;
        bool result = TimeValue::try_parse_time(from_other, x);
        EXPECT_TRUE(result);
        EXPECT_EQ(x, -3723000000);
    }
    {
        int128_t from_other = -106630;
        bool result = TimeValue::try_parse_time(from_other, x);
        EXPECT_FALSE(result);
    }
    {
        int64_t from_other = 12003123;
        bool result = TimeValue::try_parse_time(from_other, x);
        EXPECT_FALSE(result);
    }
    {
        int64_t from_other = -12003123;
        bool result = TimeValue::try_parse_time(from_other, x);
        EXPECT_FALSE(result);
    }
    {
        int128_t from_other = 12003123;
        bool result = TimeValue::try_parse_time(from_other, x);
        EXPECT_FALSE(result);
    }
    {
        int128_t from_other = -12003123;
        bool result = TimeValue::try_parse_time(from_other, x);
        EXPECT_FALSE(result);
    }
}

TEST(TimeValueTest, try_parse_time_from_string) {
    TimeValue::TimeType x = 0;
    cctz::time_zone local_time_zone {nullptr};

    {
        char s[] = "01:02:03";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_TRUE(result);
        EXPECT_EQ(x, 3723000000);
    }
    {
        char s[] = "10203";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_TRUE(result);
        EXPECT_EQ(x, 3723000000);
    }
    {
        char s[] = "-10203";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_TRUE(result);
        EXPECT_EQ(x, -3723000000);
    }
    {
        char s[] = "1abab0203";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_FALSE(result);
    }
    {
        char s[] = "01:66:03";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_FALSE(result);
    }
    {
        char s[] = "-1:02:03";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_TRUE(result);
        EXPECT_EQ(x, -3723000000);
    }
    {
        char s[] = "01:-20:03";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_FALSE(result);
    }
    {
        char s[] = "ab:02:03";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_FALSE(result);
    }
    {
        char s[] = "01:ab:03";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_FALSE(result);
    }
    {
        char s[] = "01:02:ab";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_FALSE(result);
    }

    {
        char s[] = "01:02";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_TRUE(result);
        EXPECT_EQ(x, 3720000000);
    }

    {
        char s[] = "ab:02";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_FALSE(result);
    }

    {
        char s[] = "01:ab";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_FALSE(result);
    }

    {
        char s[] = "2013-01-01 01:02:03";
        size_t len = std::strlen(s);
        bool result = TimeValue::try_parse_time(s, len, x, local_time_zone);
        EXPECT_TRUE(result);
        EXPECT_EQ(x, 3723000000);
    }
}
} // namespace doris

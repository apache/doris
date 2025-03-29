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
} // namespace doris
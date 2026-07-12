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

#include "format_v2/timestamp_statistics.h"

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

namespace doris::format {

TEST(TimestampStatisticsTest, DetectsBackwardTimezoneTransitionsInUtcRange) {
    cctz::time_zone new_york;
    ASSERT_TRUE(cctz::load_time_zone("America/New_York", &new_york));

    // The 2021 fall transition at 06:00 UTC makes local civil time move backward by one hour.
    EXPECT_FALSE(utc_timestamp_range_is_monotonic(1636263000, 1636266600, new_york));
    EXPECT_FALSE(utc_timestamp_range_is_monotonic(1636263000, 1636264800, new_york));
    EXPECT_TRUE(utc_timestamp_range_is_monotonic(1636264800, 1636266600, new_york));

    // A range beginning before the spring-forward transition must continue scanning and find the
    // later rollback in the same year.
    EXPECT_FALSE(utc_timestamp_range_is_monotonic(1609477200, 1641013200, new_york));

    // The 2021 spring transition at 07:00 UTC skips civil values but preserves ordering.
    EXPECT_TRUE(utc_timestamp_range_is_monotonic(1615703400, 1615707000, new_york));
}

TEST(TimestampStatisticsTest, FloorsNegativeEpochFractions) {
    EXPECT_EQ(floor_epoch_seconds(1001, 1000), 1);
    EXPECT_EQ(floor_epoch_seconds(-1, 1000), -1);
    EXPECT_EQ(floor_epoch_seconds(-1001, 1000), -2);
}

} // namespace doris::format

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

#include "cctz/time_zone.h"

#include <gtest/gtest.h>

#include <string>

#include "util/timezone_utils.h"

namespace doris {
class TimeZoneTest : public ::testing::Test {
protected:
    TimeZoneTest() {}
    virtual ~TimeZoneTest() {}
};

void check_time_zone_valid(const std::string& timezone) {
    cctz::time_zone ctz;
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(timezone, ctz));
}

TEST_F(TimeZoneTest, TestTimeZone) {
    // UTC has mapped to Africa/Abidjan
    check_time_zone_valid("Africa/Abidjan");
    // PRC CST has mapped to Asia/Shanghai
    check_time_zone_valid("Asia/Shanghai");
    check_time_zone_valid("+08:00");
}
} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
